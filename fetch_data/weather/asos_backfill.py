"""
ASOS 기상 데이터 백필

data/asos_all_merged.csv(누적 CSV) 전체를 읽어 weather_asos 테이블에 적재한다.
(timestamp, station_name) UPSERT라 몇 번을 다시 돌려도 행 수가 늘어나지 않는다(멱등).

사용법:
    uv run python -m fetch_data.weather.asos_backfill
    uv run python -m fetch_data.weather.asos_backfill --csv data/asos_all_merged.csv
"""

import argparse
from pathlib import Path

import pandas as pd

from fetch_data.common.logger import get_logger
from fetch_data.weather.database import init_db, load_asos_df

logger = get_logger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CSV = PROJECT_ROOT / "data" / "asos_all_merged.csv"


def run_backfill(csv_path: Path = DEFAULT_CSV) -> int:
    """CSV 전체를 읽어 weather_asos에 적재한다.

    Returns:
        upsert된 행 수
    """
    csv_path = Path(csv_path)
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV를 찾을 수 없습니다: {csv_path}")

    init_db()

    logger.info(f"[backfill] CSV 로드 시작: {csv_path}")
    df = pd.read_csv(csv_path, encoding="utf-8-sig")
    logger.info(f"[backfill] CSV 로드 완료: {len(df)}행")

    total = load_asos_df(df)
    logger.info(f"[backfill] 완료: {total}행 upsert ({csv_path.name})")
    return total


def main() -> None:
    parser = argparse.ArgumentParser(
        description="ASOS 누적 CSV 백필 (data/asos_all_merged.csv -> weather_asos)"
    )
    parser.add_argument("--csv", default=str(DEFAULT_CSV), help="CSV 경로")
    args = parser.parse_args()
    run_backfill(Path(args.csv))


if __name__ == "__main__":
    main()
