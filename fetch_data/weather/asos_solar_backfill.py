"""
ASOS 과거 일사량(+기온/습도) 백필

`data/asos_all_merged.csv`에는 일사량이 거의 없다(2019-01, 43개 지점 한 달치뿐).
과거 CSV 자체에 일사량이 없으니 CSV 재적재로는 채울 수 없고, 기상청 API를
과거 구간에 대해 다시 호출해야 한다. 이 스크립트는 기존 일일 수집기
(`asos_collect.select_data_async`/`normalize_weather_data`)를 그대로 재사용해
지정 구간을 조회하고 `weather_asos`에 UPSERT한다.

- (timestamp, station_name) UPSERT + COALESCE라 재실행해도 안전하다(멱등).
  같은 값을 다시 적재해도 행 수가 늘지 않고, 새로 온 값이 NULL이어도 기존 값을
  지우지 않는다.
- 기간이 길면 청크(기본 30일 — 지점당 numOfRows=999 한도 안에서 여유 있게)로
  나눠 청크 사이에 대기(--sleep-sec)한다. 지점 간 동시 요청 수는 기존
  asos_collect.MAX_CONCURRENT(세마포어)가 그대로 제한한다.

사용법:
    uv run python -m fetch_data.weather.asos_solar_backfill --start 20190101 --end 20190131
    uv run python -m fetch_data.weather.asos_solar_backfill --start 20190101 --end 20191231 \
        --chunk-days 30 --sleep-sec 2
"""

import argparse
import asyncio
from datetime import date, datetime, timedelta
from typing import Iterator, Tuple

from fetch_data.common.logger import get_logger
from fetch_data.weather.asos_collect import (
    normalize_weather_data,
    select_data_async,
    station_ids,
)
from fetch_data.weather.database import init_db, load_asos_df

logger = get_logger(__name__)

DEFAULT_CHUNK_DAYS = 30


def _date_chunks(start: date, end: date, chunk_days: int) -> Iterator[Tuple[date, date]]:
    """[start, end] 폐구간을 chunk_days 단위 연속 구간으로 쪼갠다."""
    if chunk_days < 1:
        raise ValueError("chunk_days는 1 이상이어야 합니다.")
    cur = start
    while cur <= end:
        chunk_end = min(cur + timedelta(days=chunk_days - 1), end)
        yield cur, chunk_end
        cur = chunk_end + timedelta(days=1)


async def backfill_range(
    start: date,
    end: date,
    chunk_days: int = DEFAULT_CHUNK_DAYS,
    sleep_sec: float = 1.0,
) -> int:
    """[start, end] 구간을 청크 단위로 수집 → 정규화 → weather_asos UPSERT.

    Returns:
        청크별 upsert 행수의 합(같은 (timestamp, station_name)이 여러 청크에
        걸쳐 다시 들어와도 매번 카운트되므로, 실제 DB 행 증가량과는 다를 수 있다).
    """
    init_db()
    chunks = list(_date_chunks(start, end, chunk_days))
    total = 0
    for i, (chunk_start, chunk_end) in enumerate(chunks, start=1):
        s, e = chunk_start.strftime("%Y%m%d"), chunk_end.strftime("%Y%m%d")
        logger.info(f"[solar-backfill] 청크 {i}/{len(chunks)}: {s}~{e} ({len(station_ids)}개 지점)")

        df = await select_data_async(station_ids, s, e)
        if df.empty:
            logger.warning(f"[solar-backfill] {s}~{e}: 수집 결과 없음")
        else:
            df = normalize_weather_data(df)
            n = load_asos_df(df)
            total += n
            logger.info(f"[solar-backfill] {s}~{e}: {n}행 upsert")

        if i < len(chunks):
            await asyncio.sleep(sleep_sec)

    logger.info(f"[solar-backfill] 완료: 총 {total}행 upsert ({start}~{end})")
    return total


def main() -> None:
    parser = argparse.ArgumentParser(description="ASOS 과거 일사량(+기온/습도) 백필")
    parser.add_argument("--start", required=True, help="시작일 YYYYMMDD")
    parser.add_argument("--end", required=True, help="종료일 YYYYMMDD")
    parser.add_argument("--chunk-days", type=int, default=DEFAULT_CHUNK_DAYS, help="청크 크기(일), 기본 30")
    parser.add_argument("--sleep-sec", type=float, default=1.0, help="청크 사이 대기(초), 기본 1")
    args = parser.parse_args()

    start = datetime.strptime(args.start, "%Y%m%d").date()
    end = datetime.strptime(args.end, "%Y%m%d").date()
    if end < start:
        raise ValueError("end가 start보다 빠릅니다.")

    asyncio.run(backfill_range(start, end, args.chunk_days, args.sleep_sec))


if __name__ == "__main__":
    main()
