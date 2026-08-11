"""
ASOS 기상 데이터 백필 — 로컬 CSV 미러로부터의 DB 복구용

data/asos_all_merged.csv(누적 CSV) 전체를 읽어 weather_asos 테이블에 적재한다.
(timestamp, station_name) UPSERT라 몇 번을 다시 돌려도 행 수가 늘어나지 않는다(멱등).

**현재 데이터 상태 기준으로는 사실상 no-op이다** — 이 CSV의 temperature/humidity는
이미 매일 같은 경로로 DB에 적재된 값과 동일하고, solar_radiation은 이 CSV에
2019-01·43개 지점분만 있는데(당시 일사량을 기록하지 않던 시절의 산물) 그마저도
DB에 이미 반영돼 있다. 2019-01-01~2026-08-10 전 구간·95개 지점의 일사량은
`asos_solar_backfill.py`가 기상청 API로 다시 채워 DB에는 있지만, 이 CSV 파일
자체는 갱신하지 않는다 — CSV와 DB의 solar_radiation 커버리지가 어긋나 있다.

그래도 스크립트는 남겨둔다: weather_asos 테이블이 손상되거나 통째로 날아갔을 때
temperature/humidity 전 구간을 기상청 API 재호출(수년치, rate limit 있음) 없이
로컬 CSV에서 즉시 복구할 수 있는 유일한 경로이기 때문이다. **이 경로로 복구한
뒤에는 반드시 `asos_solar_backfill.py`를 다시 돌려 일사량 이력을 채워야 한다**
(이 CSV만으로는 2019-01 이후 일사량이 채워지지 않는다).

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
