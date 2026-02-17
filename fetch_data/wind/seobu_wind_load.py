"""
서부발전 풍력 데이터 CSV 로드 모듈

seobu_wind.csv를 읽어 wind_seobu 테이블에 적재합니다.
CSV 컬럼: 발전기명, capacity (MW), generation, datetime
"""

import os
from pathlib import Path
from typing import Optional

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

from fetch_data.common.db_utils import resolve_db_url

PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(PROJECT_ROOT / ".env")


def load_seobu_wind_csv(csv_path: Optional[str] = None) -> pd.DataFrame:
    """
    seobu_wind.csv를 읽어 DB 적재용 DataFrame을 반환합니다.

    Returns:
        DataFrame[timestamp, plant_name, capacity_mw, generation]
    """
    if csv_path is None:
        csv_path = PROJECT_ROOT / "seobu_wind.csv"
    else:
        csv_path = Path(csv_path)

    if not csv_path.exists():
        raise FileNotFoundError(f"CSV 파일을 찾을 수 없습니다: {csv_path}")

    df = pd.read_csv(csv_path, index_col=0)

    df["timestamp"] = pd.to_datetime(df["datetime"], errors="coerce")
    df["plant_name"] = df["발전기명"].astype(str).str.strip()
    df["capacity_mw"] = pd.to_numeric(df["capacity (MW)"], errors="coerce")
    df["generation"] = pd.to_numeric(df["generation"], errors="coerce")

    result = df[["timestamp", "plant_name", "capacity_mw", "generation"]].dropna(subset=["timestamp"])
    print(f"[CSV] 서부발전 풍력 CSV 로드: {len(result)}행")
    return result.reset_index(drop=True)


def upsert_wind_seobu(df: pd.DataFrame, db_url: Optional[str] = None) -> int:
    """
    wind_seobu 테이블에 upsert합니다.
    ON CONFLICT (timestamp, plant_name) DO UPDATE
    """
    if df.empty:
        print("[DB] 적재할 데이터가 없습니다.")
        return 0

    resolved_url = resolve_db_url(db_url)
    if not resolved_url:
        raise RuntimeError("DB_URL이 설정되지 않았습니다.")

    engine = create_engine(resolved_url)

    upsert_sql = text("""
        INSERT INTO wind_seobu (timestamp, plant_name, capacity_mw, generation)
        VALUES (:timestamp, :plant_name, :capacity_mw, :generation)
        ON CONFLICT (timestamp, plant_name)
        DO UPDATE SET
            capacity_mw = EXCLUDED.capacity_mw,
            generation = EXCLUDED.generation
    """)

    records = df[["timestamp", "plant_name", "capacity_mw", "generation"]].to_dict("records")
    batch_size = 5000
    total = 0

    with engine.begin() as conn:
        for i in range(0, len(records), batch_size):
            batch = records[i : i + batch_size]
            conn.execute(upsert_sql, batch)
            total += len(batch)
            print(f"[DB] wind_seobu upsert: {total}/{len(records)}")

    print(f"[DB] wind_seobu 적재 완료: {total}행")
    return total


def load_seobu_to_db(csv_path: Optional[str] = None, db_url: Optional[str] = None) -> int:
    """CSV -> DB 적재 실행"""
    df = load_seobu_wind_csv(csv_path)
    return upsert_wind_seobu(df, db_url)


if __name__ == "__main__":
    load_seobu_to_db()
