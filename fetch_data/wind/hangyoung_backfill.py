"""
한경풍력 데이터 CSV 로드 모듈

Hangyoung_wind_power.csv를 읽어 wind_hangyoung 테이블에 적재합니다.
CSV 컬럼: timestamp, generation (plant_name 없음, 한 timestamp에 여러 터빈)
"""

import os
from pathlib import Path
from typing import Optional

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

from fetch_data.common.db_utils import resolve_db_url
from fetch_data.common.generation_core import upsert_generation
from fetch_data.common.logger import get_logger

logger = get_logger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(PROJECT_ROOT / ".env")


def load_hangyoung_wind_csv(csv_path: Optional[str] = None) -> pd.DataFrame:
    """
    Hangyoung_wind_power.csv를 읽어 DB 적재용 DataFrame을 반환합니다.

    Returns:
        DataFrame[timestamp, plant_name, generation]
    """
    if csv_path is None:
        csv_path = PROJECT_ROOT / "inputs" / "wind" / "Hangyoung_wind_power.csv"
    else:
        csv_path = Path(csv_path)

    if not csv_path.exists():
        raise FileNotFoundError(f"CSV 파일을 찾을 수 없습니다: {csv_path}")

    df = pd.read_csv(csv_path, index_col=0)

    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df["generation"] = pd.to_numeric(df["generation"], errors="coerce")
    df["plant_name"] = "Hangyoung"

    result = df[["timestamp", "plant_name", "generation"]].dropna(subset=["timestamp"])
    logger.info(f"한경풍력 CSV 로드: {len(result)}행")
    return result.reset_index(drop=True)


def load_hangyoung_to_db(csv_path: Optional[str] = None, db_url: Optional[str] = None) -> int:
    """한경풍력을 신규 코어(plants/generation)에 UPSERT (구 wind_hangyoung write 대체).

    원본 CSV는 한 timestamp에 터빈/단지별(한경1·2단계 등) 여러 행이라
    (timestamp, plant_name)별로 SUM(= 단지 총 출력)해 적재한다.
    """
    df = load_hangyoung_wind_csv(csv_path)

    if df.empty:
        logger.info("적재할 데이터가 없습니다.")
        return 0

    agg = df.groupby(["timestamp", "plant_name"], as_index=False)["generation"].sum()
    return upsert_generation(
        agg, operator="hangyoung", fuel_type="wind", db_url=db_url,
    )


if __name__ == "__main__":
    load_hangyoung_to_db()
