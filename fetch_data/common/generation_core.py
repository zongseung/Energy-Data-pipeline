"""
수집기 공용: DataFrame을 신규 코어(plants + generation)에 직접 UPSERT.

dual-write 트리거(scripts/migrations/p4_dual_write_triggers.sql)가 구테이블 INSERT 시
하던 매핑을 Python으로 옮긴 것 — 수집기가 구테이블 대신 코어에 직접 쓰도록 전환(P6 준비).
  · plants 보장: _ensure_plant 동등 (ON CONFLICT (plant_name, unit_no) DO NOTHING, 최소행)
  · generation UPSERT: ON CONFLICT (timestamp, plant_id) DO UPDATE, source='api'

입력 df 필수 컬럼: timestamp, plant_name, generation
선택 컬럼: unit_no(기본 '1'), plant_code(기본 None)
"""

from __future__ import annotations

from typing import Optional

import pandas as pd
from sqlalchemy import create_engine, text

from fetch_data.common.db_utils import resolve_db_url
from fetch_data.common.logger import get_logger

logger = get_logger(__name__)

_UPSERT_PLANT = text("""
    INSERT INTO plants (plant_code, plant_name, unit_no, fuel_type, operator, capacity_confidence)
    VALUES (:code, :name, :unit, :fuel, :op, '불확실')
    ON CONFLICT (plant_name, unit_no) DO NOTHING
""")

_UPSERT_GEN = text("""
    INSERT INTO generation (timestamp, plant_id, gen_kwh, source)
    VALUES (:ts, :pid, :kwh, 'api')
    ON CONFLICT (timestamp, plant_id)
    DO UPDATE SET gen_kwh = EXCLUDED.gen_kwh, source = 'api'
""")


def upsert_generation(
    df: pd.DataFrame,
    *,
    operator: str,
    fuel_type: str,
    db_url: Optional[str] = None,
    engine=None,
    batch: int = 5000,
) -> int:
    """수집기 df를 plants/generation 코어에 UPSERT. 처리(적재) 행수 반환.

    df: [timestamp, plant_name, generation] (+ 선택 unit_no, plant_code)
    operator/fuel_type: 코어 귀속 (예: nambu/solar, namdong/solar, namdong/wind, seobu/wind, hangyoung/wind)
    """
    if df is None or len(df) == 0:
        logger.info("[core] 적재할 데이터 없음")
        return 0

    df = df.copy()
    df["plant_name"] = df["plant_name"].astype(str).str.strip()
    if "unit_no" in df.columns:
        df["unit_no"] = df["unit_no"].astype(str).str.strip()
    else:
        df["unit_no"] = "1"
    if "plant_code" not in df.columns:
        df["plant_code"] = None
    df = df.dropna(subset=["timestamp", "plant_name"])
    if df.empty:
        return 0

    eng = engine if engine is not None else create_engine(resolve_db_url(db_url))

    with eng.begin() as conn:
        # 1) plants 보장 (없으면 최소행 생성, 있으면 그대로)
        keys = df[["plant_name", "unit_no", "plant_code"]].drop_duplicates()
        for r in keys.itertuples(index=False):
            conn.execute(_UPSERT_PLANT, {
                "code": (None if pd.isna(r.plant_code) else r.plant_code),
                "name": r.plant_name,
                "unit": r.unit_no,
                "fuel": fuel_type,
                "op": operator,
            })

        # 2) (plant_name, unit_no) -> plant_id
        rows = conn.execute(text(
            "SELECT plant_name, unit_no, plant_id FROM plants WHERE operator = :op AND fuel_type = :f"
        ), {"op": operator, "f": fuel_type}).fetchall()
        id_map = {(x.plant_name, x.unit_no): x.plant_id for x in rows}
        df["plant_id"] = [id_map.get((n, u)) for n, u in zip(df["plant_name"], df["unit_no"])]

        unmapped = sorted(df.loc[df["plant_id"].isna(), "plant_name"].unique())
        if unmapped:
            logger.warning(
                f"[core] {operator}/{fuel_type} plant_id 미매핑 {len(unmapped)}종: {unmapped[:5]}"
            )
        df = df[df["plant_id"].notna()].copy()
        df["plant_id"] = df["plant_id"].astype(int)

        # 3) generation UPSERT
        records = [
            {"ts": t, "pid": int(p), "kwh": (None if pd.isna(g) else float(g))}
            for t, p, g in zip(df["timestamp"], df["plant_id"], df["generation"])
        ]
        for i in range(0, len(records), batch):
            conn.execute(_UPSERT_GEN, records[i:i + batch])

    logger.info(f"[core] {operator}/{fuel_type} generation UPSERT {len(records):,}행")
    return len(records)
