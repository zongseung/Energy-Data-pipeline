"""
남동발전(KOEN) 비태양광 long CSV -> generation 코어 직접 적재 ("wind식 upsert" 모듈).

transform_gen 이 만든 gen_data/{category}_long.csv ([timestamp, plant_name, generation])를
plants/generation 코어 테이블에 멱등 UPSERT 한다. 구테이블·dual-write 트리거를 두지 않고
코어에 직접 쓴다 (비태양광은 FDW/구테이블 소비자가 없음).

- plants     : **만들지 않는다.** 없는 설비가 오면 RuntimeError 로 멈춘다 — 원천이 표기를
               바꾸면 조용히 새 행이 생겨 시계열이 쪼개진다. 새 설비는 사람이 등록한다.
- generation : plant_name -> plant_id 매핑 후 (timestamp, plant_id) upsert.
               gen_kwh = CSV generation 값 그대로 (원본 헤더는 MWh지만 실제 kWh — 변환 안 함).
               source='api'.

※ 선행 조건: plants 의 KOEN 비태양광 operator 가 'namdong' 으로 통일돼 있어야 한다
  (scripts/p_unify_koen_operator.sql). 'koen' 인 채로 두면 plant_id 매핑이 비어 적재가 0이 된다.

사용 예:
    # gen_data/*_long.csv 전체를 코어에 적재
    uv run python -m pipeline.fetch_data.gen.load_gen

    # 일부 카테고리만
    uv run python -m pipeline.fetch_data.gen.load_gen --types thermal,fuel_cell
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
from sqlalchemy import create_engine, text

from pipeline.fetch_data.common.db_utils import resolve_db_url
from pipeline.fetch_data.common.logger import get_logger
from pipeline.fetch_data.gen.transform_gen import DEFAULT_OUT_DIR

logger = get_logger(__name__)

# KOEN = 한국남동발전. operator 라벨을 namdong 으로 통일 (solar/wind 와 동일).
OPERATOR = "namdong"
SOURCE = "api"

# 카테고리 -> generation.fuel_type
FUEL_MAP: Dict[str, str] = {
    "wind": "wind",
    "ocean_hydro": "hydro",
    "fuel_cell": "fuel_cell",
    "thermal": "thermal",
}

UPSERT_GEN = text("""
    INSERT INTO generation (timestamp, plant_id, gen_kwh, source)
    VALUES (:ts, :pid, :kwh, :src)
    ON CONFLICT (timestamp, plant_id)
    DO UPDATE SET gen_kwh = EXCLUDED.gen_kwh
""")  # 충돌 시 gen_kwh 만 갱신. source 는 보존(기존 backfill 이력 유지, 신규 월만 'api' insert).


def _plant_ids(conn, fuel: str, names) -> Dict[str, int]:
    """{plant_name: plant_id} 반환. 없는 설비가 있으면 만들지 않고 멈춘다."""
    id_map = dict(conn.execute(text(
        "SELECT plant_name, plant_id FROM plants WHERE operator = :op AND fuel_type = :f"
    ), {"op": OPERATOR, "f": fuel}).fetchall())
    unknown = sorted(set(names) - id_map.keys())
    if unknown:
        raise RuntimeError(
            f"[{fuel}] plants 에 없는 설비 {len(unknown)}종 — 자동 생성하지 않는다. "
            f"확인 후 등록하고 다시 돌려라: {unknown[:10]}"
        )
    return id_map


def load_category(engine, category: str, out_dir: Path) -> int:
    """한 카테고리의 long CSV 를 코어에 적재. 적재(처리)한 generation 행수 반환."""
    if category not in FUEL_MAP:
        raise ValueError(f"알 수 없는 카테고리: {category} (가능: {list(FUEL_MAP)})")
    fuel = FUEL_MAP[category]
    src_path = Path(out_dir) / f"{category}_long.parquet"
    if not src_path.exists():
        logger.warning(f"[{category}] {src_path} 없음 — 건너뜀")
        return 0

    df = pd.read_parquet(src_path)
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df["generation"] = pd.to_numeric(df["generation"], errors="coerce")
    df["plant_name"] = df["plant_name"].astype(str).str.strip()
    df = df.dropna(subset=["timestamp"])

    with engine.begin() as conn:
        id_map = _plant_ids(conn, fuel, df["plant_name"].unique())
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
