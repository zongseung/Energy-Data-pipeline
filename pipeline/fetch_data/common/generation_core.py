"""
수집기 공용: DataFrame을 신규 코어(plants + generation)에 직접 UPSERT.

dual-write 트리거(scripts/migrations/p4_dual_write_triggers.sql)가 구테이블 INSERT 시
하던 매핑을 Python으로 옮긴 것 — 수집기가 구테이블 대신 코어에 직접 쓰도록 전환(P6 준비).
  · plants 는 **만들지 않는다** — 없는 설비가 오면 RuntimeError 로 멈춘다.
    원천이 표기를 바꾸면 조용히 새 행이 생겨 시계열이 두 동으로 쪼개진다
    (서부 '화순풍력'/'화순 풍력 발전소' 가 실제로 그렇게 갈라졌다).
    새 설비는 사람이 plants 에 등록한 뒤 다시 돌린다.
  · generation UPSERT: ON CONFLICT (timestamp, plant_id) DO UPDATE, source='api'

입력 df 필수 컬럼: timestamp, plant_name, generation
선택 컬럼: unit_no(기본 '1')
"""

from __future__ import annotations

from datetime import date
from typing import Optional

import pandas as pd
from sqlalchemy import create_engine, text

from pipeline.fetch_data.common.db_utils import resolve_db_url
from pipeline.fetch_data.common.logger import get_logger

logger = get_logger(__name__)

_UPSERT_GEN = text("""
    INSERT INTO generation (timestamp, plant_id, gen_kwh, source)
    VALUES (:ts, :pid, :kwh, 'api')
    ON CONFLICT (timestamp, plant_id)
    DO UPDATE SET gen_kwh = EXCLUDED.gen_kwh, source = 'api'
""")


_LATEST_TS = text("""
    SELECT max(g.timestamp)
    FROM generation g
    JOIN plants p ON p.plant_id = g.plant_id
    WHERE p.operator = :op AND p.fuel_type = :fuel
""")


def latest_generation_date(
    *,
    operator: str,
    fuel_type: str,
    db_url: Optional[str] = None,
    engine=None,
) -> Optional[date]:
    """이 operator/fuel 의 마지막 적재일. 수집 재개 커서로 쓴다.

    원본 CSV 파일명을 커서로 쓰면 컨테이너가 auto_remove 될 때 같이 사라져
    매번 0에서 재수집한다. DB 는 유일하게 살아남는 상태다.
    """
    eng = engine or create_engine(resolve_db_url(db_url))
    with eng.connect() as conn:
        ts = conn.execute(_LATEST_TS, {"op": operator, "fuel": fuel_type}).scalar()
    if ts is None:
        return None
    return ts.date() if hasattr(ts, "date") else date.fromisoformat(str(ts)[:10])


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

    df: [timestamp, plant_name, generation] (+ 선택 unit_no)
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
    df = df.dropna(subset=["timestamp", "plant_name"])
    if df.empty:
        return 0

    eng = engine if engine is not None else create_engine(resolve_db_url(db_url))

    with eng.begin() as conn:
        # 1) (plant_name, unit_no) -> plant_id. 없으면 만들지 않고 멈춘다.
        rows = conn.execute(text(
            "SELECT plant_name, unit_no, plant_id FROM plants WHERE operator = :op AND fuel_type = :f"
        ), {"op": operator, "f": fuel_type}).fetchall()
        id_map = {(x.plant_name, x.unit_no): x.plant_id for x in rows}
        df["plant_id"] = [id_map.get((n, u)) for n, u in zip(df["plant_name"], df["unit_no"])]

        unknown = sorted(
            {(n, u) for n, u, p in zip(df["plant_name"], df["unit_no"], df["plant_id"]) if pd.isna(p)}
        )
        if unknown:
            raise RuntimeError(
                f"[core] {operator}/{fuel_type}: plants 에 없는 설비 {len(unknown)}종 — "
                f"자동 생성하지 않는다. 확인 후 plants 에 등록하고 다시 돌려라: {unknown[:10]}"
            )
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
