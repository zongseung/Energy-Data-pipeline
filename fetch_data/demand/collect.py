"""Download and persist nationwide five-minute KPX demand data."""

import asyncio
import random
import re
from datetime import date, datetime, timedelta
from io import StringIO

import aiohttp
import pandas as pd
from sqlalchemy.engine import Engine
from workalendar.asia import SouthKorea

from fetch_data.demand.database import get_last_5min_timestamp, upsert_demand_5min

BASE_URL = "https://openapi.kpx.or.kr"
SUKUB_URL = f"{BASE_URL}/sukub.do"
DOWNLOAD_URL = f"{BASE_URL}/downloadSukubCSV.do"
BACKOFF_SCHEDULE = [1, 5, 10, 20, 30]
BASE_THROTTLE_SECONDS = 0.4
REQUEST_TIMEOUT = 30
COLUMN_MAPPING = {
    "기준일시": "timestamp",
    "현재수요(MW)": "current_demand",
    "공급능력(MW)": "current_supply",
    "최대예측수요(MW)": "supply_capacity",
    "공급예비력(MW)": "supply_reserve",
    "공급예비율(%)": "reserve_rate",
    "운영예비력(MW)": "operation_reserve",
}

_calendar = SouthKorea()
_holiday_cache: dict[str, bool] = {}


def _as_date(value: date | str) -> date:
    if isinstance(value, date):
        return value
    return datetime.strptime(value, "%Y%m%d").date()


def _is_html_error(raw: bytes, content_type: str) -> bool:
    head = raw.lstrip()[:300]
    return (
        "text/html" in (content_type or "").lower()
        or head.lower().startswith(b"<!doctype html")
        or b"egovframe" in head.lower()
    )


async def request_with_retry(
    session: aiohttp.ClientSession,
    method: str,
    url: str,
    *,
    data: dict | None = None,
    expect_csv: bool = False,
    max_attempts: int = 5,
) -> bytes:
    """Request KPX data with retry handling for transient and HTML failures."""
    last_error: Exception | None = None
    for attempt in range(max_attempts):
        try:
            async with session.request(method, url, data=data) as response:
                raw = await response.read()
                if response.status == 200 and (
                    not expect_csv or not _is_html_error(raw, response.headers.get("Content-Type", ""))
                ):
                    return raw
        except (aiohttp.ClientError, asyncio.TimeoutError, ConnectionResetError) as error:
            last_error = error
        await asyncio.sleep(BACKOFF_SCHEDULE[min(attempt, len(BACKOFF_SCHEDULE) - 1)] + random.uniform(0, 0.6))
    raise RuntimeError(f"Request failed after {max_attempts} attempts: {url}") from last_error


def _normalize_csv_timestamps(content: str) -> str:
    return re.sub(
        r"\b\d{14}\b",
        lambda match: f"{match[0][:4]}-{match[0][4:6]}-{match[0][6:8]} {match[0][8:10]}:{match[0][10:12]}:{match[0][12:14]}",
        content,
    )


async def download_segment(
    session: aiohttp.ClientSession, start_date: date, end_date: date
) -> pd.DataFrame:
    """Download one KPX CSV date segment."""
    start = start_date.isoformat()
    end = end_date.isoformat()
    await request_with_retry(session, "GET", SUKUB_URL)
    await request_with_retry(
        session,
        "POST",
        SUKUB_URL,
        data={"startDate": start, "endDate": end, "searchUseYn": "Y", "message": ""},
    )
    raw = await request_with_retry(
        session, "POST", DOWNLOAD_URL, data={"startDate": start, "endDate": end}, expect_csv=True
    )
    return pd.read_csv(StringIO(_normalize_csv_timestamps(raw.decode("euc-kr", errors="ignore"))))


async def download_range(
    start_date: date | str,
    end_date: date | str,
    throttle_seconds: float = BASE_THROTTLE_SECONDS,
    max_retries: int = 3,
) -> pd.DataFrame:
    """Download daily KPX CSV segments and deduplicate their timestamp column."""
    start = _as_date(start_date)
    end = _as_date(end_date)
    if end < start:
        return pd.DataFrame()

    frames: list[pd.DataFrame] = []
    timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        current = start
        while current <= end:
            frame = pd.DataFrame()
            for _ in range(max_retries):
                try:
                    frame = await download_segment(session, current, current)
                    if not frame.empty:
                        break
                except Exception:
                    pass
                await asyncio.sleep(throttle_seconds)
            if not frame.empty:
                frames.append(frame)
            current += timedelta(days=1)

    if not frames:
        return pd.DataFrame()
    result = pd.concat(frames, ignore_index=True)
    return result.drop_duplicates(subset=[result.columns[0]], keep="first")


def is_holiday(timestamp: datetime) -> bool:
    key = timestamp.date().isoformat()
    if key not in _holiday_cache:
        _holiday_cache[key] = _calendar.is_holiday(timestamp.date())
    return _holiday_cache[key]


def get_day_type(timestamp: datetime) -> int:
    if is_holiday(timestamp):
        return 2
    return 1 if timestamp.weekday() >= 5 else 0


def prepare_records(dataframe: pd.DataFrame) -> list[dict]:
    """Map KPX CSV columns and add calendar metadata for database upsert."""
    dataframe = dataframe.rename(columns=COLUMN_MAPPING)
    dataframe = dataframe[[column for column in COLUMN_MAPPING.values() if column in dataframe]].copy()
    if "timestamp" in dataframe:
        dataframe["timestamp"] = pd.to_datetime(dataframe["timestamp"])
        dataframe["is_holiday"] = dataframe["timestamp"].apply(is_holiday)
        dataframe["day_type"] = dataframe["timestamp"].apply(get_day_type)
    return dataframe.where(pd.notnull(dataframe), None).to_dict(orient="records")


def get_collection_start(
    last_ts: datetime | None, now: datetime, recent_hours: int = 1
) -> date:
    """Use the missing day for backfill or the recent rolling-window day otherwise."""
    if last_ts is not None and last_ts.date() < now.date():
        return last_ts.date()
    return (now - timedelta(hours=recent_hours)).date()


async def collect_range(engine: Engine, start_date: date, end_date: date) -> int:
    """Download a non-empty date range and upsert its prepared records."""
    dataframe = await download_range(start_date, end_date)
    if dataframe.empty and start_date <= end_date:
        raise RuntimeError("수집된 전력수요 데이터가 없습니다")
    return upsert_demand_5min(engine, prepare_records(dataframe))


async def collect_latest(engine: Engine, now: datetime | None = None) -> int:
    """Collect from the last persisted day through the current day."""
    current_time = now or datetime.now()
    return await collect_range(
        engine,
        get_collection_start(get_last_5min_timestamp(engine), current_time),
        current_time.date(),
    )
