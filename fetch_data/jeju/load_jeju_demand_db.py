"""
제주 수급(5분) CSV → demand-postgres `jeju_supply_demand` 테이블 적재.

목적: energy_hub가 이 테이블을 **FDW로 소비**(육지 demand_5min과 동일 패턴)하게 하여
CSV→수동 ETL 없이 실시간으로 유지한다. (jeju_realtime_collect가 5분마다 CSV를 갱신 → 이 로더가 DB로 동기화)

- 전체 백필:   run()                 # 모든 월 CSV
- 10분 동기화: run(months_back=1)    # 당월만(델타, 멱등 UPSERT)
"""
import os
from pathlib import Path

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text

from fetch_data.common.logger import get_logger

logger = get_logger(__name__)

SUKUB_DIR = Path("/mnt/iscsi-renewable/jeju_data/sukub")
# 호스트: localhost:5433 / 컨테이너: DEMAND_DB_URL 주입
DEFAULT_DEMAND_DB_URL = "postgresql+psycopg2://demand:demand@localhost:5433/demand"

_COLS = ["ts", "supply_mw", "demand_mw", "renewable_total_mw", "solar_mw", "wind_mw"]

DDL = """
CREATE TABLE IF NOT EXISTS jeju_supply_demand (
    ts                 timestamp PRIMARY KEY,
    supply_mw          double precision,
    demand_mw          double precision,
    renewable_total_mw double precision,
    solar_mw           double precision,
    wind_mw            double precision
);
COMMENT ON TABLE jeju_supply_demand IS '제주 계통수급 5분 (KPX). energy_hub가 FDW로 소비';
"""

UPSERT = text("""
    INSERT INTO jeju_supply_demand (ts, supply_mw, demand_mw, renewable_total_mw, solar_mw, wind_mw)
    VALUES (:ts, :supply_mw, :demand_mw, :renewable_total_mw, :solar_mw, :wind_mw)
    ON CONFLICT (ts) DO UPDATE SET
        supply_mw = EXCLUDED.supply_mw,
        demand_mw = EXCLUDED.demand_mw,
        renewable_total_mw = EXCLUDED.renewable_total_mw,
        solar_mw = EXCLUDED.solar_mw,
        wind_mw = EXCLUDED.wind_mw
""")


def _read_sukub(months_back=None) -> pd.DataFrame:
    files = sorted(SUKUB_DIR.glob("jeju_sukub_*.csv"))
    if not files:
        raise FileNotFoundError(f"sukub CSV 없음: {SUKUB_DIR}")
    if months_back:
        files = files[-months_back:]
    df = pd.concat([pd.read_csv(f, encoding="utf-8-sig") for f in files], ignore_index=True)
    df["ts"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df = df.dropna(subset=["ts"]).drop_duplicates(subset=["ts"], keep="last")
    return df[_COLS].replace({np.nan: None})


def run(db_url: str | None = None, months_back: int | None = None) -> int:
    """제주 수급 CSV를 demand DB로 upsert. 적재 행수 반환."""
    url = db_url or os.getenv("DEMAND_DB_URL", DEFAULT_DEMAND_DB_URL)
    engine = create_engine(url)
    df = _read_sukub(months_back)
    records = df.to_dict("records")
    with engine.begin() as conn:
        conn.execute(text(DDL))
        for i in range(0, len(records), 5000):
            conn.execute(UPSERT, records[i:i + 5000])
    logger.info(f"jeju_supply_demand upsert 완료: {len(records)}행 (months_back={months_back})")
    return len(records)


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(description="제주 수급 CSV → demand DB 적재")
    ap.add_argument("--months-back", type=int, default=None, help="최근 N개월만(없으면 전체)")
    ap.add_argument("--db-url", default=None)
    args = ap.parse_args()
    print("적재 행수:", run(db_url=args.db_url, months_back=args.months_back))
