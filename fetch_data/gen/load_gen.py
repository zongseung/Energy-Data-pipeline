"""
남동발전(KOEN) 비태양광 long CSV -> generation 코어 직접 적재 ("wind식 upsert" 모듈).

transform_gen 이 만든 gen_data/{category}_long.csv ([timestamp, plant_name, generation])를
plants/generation 코어 테이블에 멱등 UPSERT 한다. 구테이블·dual-write 트리거를 두지 않고
코어에 직접 쓴다 (비태양광은 FDW/구테이블 소비자가 없음).

- plants     : (plant_name, unit_no='1') 기준 upsert. operator='namdong'(=KOEN 한국남동발전),
               fuel_type=카테고리 매핑, 용량/좌표는 capacities/locations 에서 보강.
               이미 있으면 건드리지 않음(ON CONFLICT DO NOTHING).
- generation : plant_name -> plant_id 매핑 후 (timestamp, plant_id) upsert.
               gen_kwh = CSV generation 값 그대로 (원본 헤더는 MWh지만 실제 kWh — 변환 안 함).
               source='api'.

※ 선행 조건: plants 의 KOEN 비태양광 operator 가 'namdong' 으로 통일돼 있어야 한다
  (scripts/p_unify_koen_operator.sql). 'koen' 인 채로 두면 plant_id 매핑이 비어 적재가 0이 된다.

사용 예:
    # gen_data/*_long.csv 전체를 코어에 적재
    uv run python -m fetch_data.gen.load_gen

    # 일부 카테고리만
    uv run python -m fetch_data.gen.load_gen --types thermal,fuel_cell
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
from sqlalchemy import create_engine, text

from fetch_data.common.db_utils import resolve_db_url
from fetch_data.common.logger import get_logger
from fetch_data.gen.capacities import resolve_capacity
from fetch_data.gen.locations import resolve_location
from fetch_data.gen.transform_gen import DEFAULT_OUT_DIR

logger = get_logger(__name__)

# KOEN = 한국남동발전. operator 라벨을 namdong 으로 통일 (solar/wind 와 동일).
OPERATOR = "namdong"
SOURCE = "api"

# 카테고리 -> generation.fuel_type
FUEL_MAP: Dict[str, str] = {
    "ocean_hydro": "hydro",
    "fuel_cell": "fuel_cell",
    "thermal": "thermal",
}

UPSERT_PLANT = text("""
    INSERT INTO plants (plant_name, unit_no, fuel_type, operator, region,
                        capacity_mw, capacity_confidence, lat, lon, address, site_name)
    VALUES (:name, '1', :fuel, :op, 'mainland',
            :cap_mw, :conf, :lat, :lon, :addr, :site)
    ON CONFLICT (plant_name, unit_no) DO NOTHING
""")

UPSERT_GEN = text("""
    INSERT INTO generation (timestamp, plant_id, gen_kwh, source)
    VALUES (:ts, :pid, :kwh, :src)
    ON CONFLICT (timestamp, plant_id)
    DO UPDATE SET gen_kwh = EXCLUDED.gen_kwh
""")  # 충돌 시 gen_kwh 만 갱신. source 는 보존(기존 backfill 이력 유지, 신규 월만 'api' insert).


def _ensure_plants(conn, fuel: str, names) -> Dict[str, int]:
    """카테고리 발전소들을 plants 에 upsert 하고 {plant_name: plant_id} 반환."""
    for name in names:
        cap = resolve_capacity(name)
        loc = resolve_location(name)
        conn.execute(UPSERT_PLANT, {
            "name": name,
            "fuel": fuel,
            "op": OPERATOR,
            "cap_mw": cap["capacity_mw"] if cap else None,
            "conf": cap["confidence"] if cap else "불확실",
            "lat": loc["lat"] if loc else None,
            "lon": loc["lon"] if loc else None,
            "addr": loc["address"] if loc else None,
            "site": loc["site"] if loc else None,
        })
    return dict(conn.execute(text(
        "SELECT plant_name, plant_id FROM plants WHERE operator = :op AND fuel_type = :f"
    ), {"op": OPERATOR, "f": fuel}).fetchall())


def load_category(engine, category: str, out_dir: Path) -> int:
    """한 카테고리의 long CSV 를 코어에 적재. 적재(처리)한 generation 행수 반환."""
    if category not in FUEL_MAP:
        raise ValueError(f"알 수 없는 카테고리: {category} (가능: {list(FUEL_MAP)})")
    fuel = FUEL_MAP[category]
    csv_path = Path(out_dir) / f"{category}_long.csv"
    if not csv_path.exists():
        logger.warning(f"[{category}] {csv_path} 없음 — 건너뜀")
        return 0

    df = pd.read_csv(csv_path)
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df["generation"] = pd.to_numeric(df["generation"], errors="coerce")
    df["plant_name"] = df["plant_name"].astype(str).str.strip()
    df = df.dropna(subset=["timestamp"])

    with engine.begin() as conn:
        id_map = _ensure_plants(conn, fuel, df["plant_name"].unique())
        df["plant_id"] = df["plant_name"].map(id_map)

        unmapped = sorted(df.loc[df["plant_id"].isna(), "plant_name"].unique())
        if unmapped:
            logger.warning(
                f"[{category}] plant_id 미매핑 {len(unmapped)}종 (operator 통일 누락 의심): "
                f"{unmapped[:5]}{' ...' if len(unmapped) > 5 else ''}"
            )
        df = df[df["plant_id"].notna()].copy()
        df["plant_id"] = df["plant_id"].astype(int)

        records = [
            {"ts": t, "pid": int(pid),
             "kwh": None if pd.isna(g) else float(g), "src": SOURCE}
            for t, pid, g in zip(df["timestamp"], df["plant_id"], df["generation"])
        ]
        before = conn.execute(text("SELECT count(*) FROM generation")).scalar()
        batch = 5000
        for i in range(0, len(records), batch):
            conn.execute(UPSERT_GEN, records[i:i + batch])
        after = conn.execute(text("SELECT count(*) FROM generation")).scalar()

    logger.info(
        f"[{category}->{fuel}] CSV {len(df):,}행 upsert | "
        f"generation 신규 {after - before:,} (총 {after:,})"
    )
    return len(df)


def load_all(
    categories: Optional[List[str]] = None,
    out_dir: Path = DEFAULT_OUT_DIR,
    db_url: Optional[str] = None,
) -> int:
    """카테고리별 long CSV 를 generation 코어에 적재. 처리 총 행수 반환."""
    categories = categories or list(FUEL_MAP.keys())
    engine = create_engine(resolve_db_url(db_url))
    total = 0
    for cat in categories:
        total += load_category(engine, cat, Path(out_dir))
    logger.info(f"비태양광 코어 적재 완료 — {total:,}행 처리 ({categories})")
    return total


# =========================================================
# CLI
# =========================================================
def _parse_types(s: Optional[str]) -> List[str]:
    if not s:
        return list(FUEL_MAP.keys())
    keys = [t.strip() for t in s.split(",") if t.strip()]
    bad = [k for k in keys if k not in FUEL_MAP]
    if bad:
        raise SystemExit(f"알 수 없는 카테고리: {bad} (가능: {list(FUEL_MAP)})")
    return keys


def main() -> None:
    parser = argparse.ArgumentParser(
        description="KOEN 비태양광 long CSV -> generation 코어 적재"
    )
    parser.add_argument("--types", default=None,
                        help=f"카테고리 콤마구분 (기본 전체). 가능: {','.join(FUEL_MAP)}")
    parser.add_argument("--out-dir", default=str(DEFAULT_OUT_DIR),
                        help="long CSV 디렉터리 (transform_gen 출력)")
    parser.add_argument("--db-url", default=None, help="DB 접속 URL override")
    args = parser.parse_args()

    load_all(
        categories=_parse_types(args.types),
        out_dir=Path(args.out_dir),
        db_url=args.db_url,
    )


if __name__ == "__main__":
    main()
