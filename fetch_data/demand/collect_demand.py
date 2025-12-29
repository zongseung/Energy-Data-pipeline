"""
전력 수급 데이터 수집 모듈

KPX API에서 5분 단위 전력수급 데이터를 수집하고 DB에 저장합니다.
- 백필 지원: DB의 마지막 timestamp 이후 데이터만 수집
- 1시간 단위 수집: Prefect에서 매 시간 호출 가능
"""

import asyncio
import os
import random
import re
from datetime import datetime, timedelta
from typing import Optional, List, Tuple

import aiohttp
import pandas as pd
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from workalendar.asia import SouthKorea

from fetch_data.common.database import (
    Demand5Min,
    get_async_session,
    init_db,
)


# ========================================
# Constants
# ========================================
BASE_URL = "https://openapi.kpx.or.kr"
SUKUB_URL = f"{BASE_URL}/sukub.do"
DOWNLOAD_URL = f"{BASE_URL}/downloadSukubCSV.do"

BACKOFF_SCHEDULE = [1, 5, 10, 20, 30]
BASE_THROTTLE_SECONDS = 0.4
REQUEST_TIMEOUT = 30

# Column mapping: Korean -> English
COLUMN_MAPPING = {
    "기준일시": "timestamp",
    "현재수요(MW)": "current_demand",
    "현재공급(MW)": "current_supply",
    "공급가능용량(MW)": "supply_capacity",
    "공급예비력(MW)": "supply_reserve",
    "공급예비율(%)": "reserve_rate",
    "운영예비력(MW)": "operation_reserve",
}


# ========================================
# Date/Time Utilities
# ========================================
def format_date_dash(date_yyyymmdd: str) -> str:
    """YYYYMMDD -> YYYY-MM-DD"""
    return f"{date_yyyymmdd[:4]}-{date_yyyymmdd[4:6]}-{date_yyyymmdd[6:8]}"


def parse_date(date_str: str) -> datetime:
    """Parse YYYYMMDD string to datetime."""
    return datetime.strptime(date_str, "%Y%m%d")


def format_date(dt: datetime) -> str:
    """Format datetime to YYYYMMDD string."""
    return dt.strftime("%Y%m%d")


def split_range_by_days(
    start: datetime,
    end: datetime,
    max_days: int = 90
) -> List[Tuple[datetime, datetime]]:
    """
    날짜 범위를 max_days 단위로 분할합니다.
    API 제한(최대 3개월) 대응.
    """
    ranges = []
    current = start
    while current <= end:
        segment_end = min(current + timedelta(days=max_days - 1), end)
        ranges.append((current, segment_end))
        current = segment_end + timedelta(days=1)
    return ranges


def convert_datetime_format(val: str) -> str:
    """
    14자리 날짜 문자열을 표준 형식으로 변환.
    20250402000000 -> 2025-04-02 00:00:00
    """
    return f"{val[:4]}-{val[4:6]}-{val[6:8]} {val[8:10]}:{val[10:12]}:{val[12:14]}"


def transform_datetime_in_csv(content: str) -> str:
    """CSV 전체에서 14자리 날짜를 변환."""
    pattern = r"\b\d{14}\b"
    return re.sub(pattern, lambda m: convert_datetime_format(m.group()), content)


# ========================================
# Holiday Check
# ========================================
_holiday_cache: dict[str, bool] = {}
_calendar = SouthKorea()


def is_holiday(date: datetime) -> bool:
    """Check if the given date is a holiday in Korea."""
    date_key = date.strftime("%Y-%m-%d")
    if date_key not in _holiday_cache:
        _holiday_cache[date_key] = _calendar.is_holiday(date.date())
    return _holiday_cache[date_key]


# ========================================
# Network Utilities
# ========================================
def _is_html_error(raw: bytes, content_type: str) -> bool:
    """Check if response is an HTML error page."""
    ct = (content_type or "").lower()
    head = raw.lstrip()[:300]
    if "text/html" in ct:
        return True
    if head.startswith(b"<!doctype html") or head.startswith(b"<!DOCTYPE html"):
        return True
    if b"eGovFrame" in head or b"egovframe" in head:
        return True
    return False


async def request_with_retry(
    session: aiohttp.ClientSession,
    method: str,
    url: str,
    *,
    data: Optional[dict] = None,
    expect_csv: bool = False,
    max_attempts: int = 5,
) -> bytes:
    """
    HTTP 요청을 재시도 로직과 함께 실행합니다.
    Adaptive backoff + jitter 적용.
    """
    last_error: Optional[Exception] = None

    for attempt in range(1, max_attempts + 1):
        backoff = BACKOFF_SCHEDULE[min(attempt - 1, len(BACKOFF_SCHEDULE) - 1)]
        jitter = random.uniform(0.0, 0.6)
        sleep_time = backoff + jitter

        try:
            async with session.request(method, url, data=data) as resp:
                raw = await resp.read()

                if resp.status != 200:
                    print(f"[WARN] HTTP {resp.status} (attempt {attempt}/{max_attempts})")
                    await asyncio.sleep(sleep_time)
                    continue

                if expect_csv and _is_html_error(raw, resp.headers.get("Content-Type", "")):
                    print(f"[WARN] HTML error page (attempt {attempt}/{max_attempts})")
                    await asyncio.sleep(sleep_time)
                    continue

                return raw

        except (aiohttp.ClientError, asyncio.TimeoutError, ConnectionResetError) as e:
            last_error = e
            print(f"[WARN] Network error: {e} (attempt {attempt}/{max_attempts})")
            await asyncio.sleep(sleep_time)

    raise RuntimeError(f"Request failed after {max_attempts} attempts: {url}") from last_error


# ========================================
# Data Download
# ========================================
async def download_segment(
    session: aiohttp.ClientSession,
    start_yyyymmdd: str,
    end_yyyymmdd: str,
) -> pd.DataFrame:
    """
    단일 구간의 데이터를 다운로드하고 DataFrame으로 반환합니다.
    """
    start_dash = format_date_dash(start_yyyymmdd)
    end_dash = format_date_dash(end_yyyymmdd)

    # 1) 세션 발급 (페이지 진입)
    await request_with_retry(session, "GET", SUKUB_URL)

    # 2) 조회 요청
    search_payload = {
        "startDate": start_dash,
        "endDate": end_dash,
        "searchUseYn": "Y",
        "message": "",
    }
    await request_with_retry(session, "POST", SUKUB_URL, data=search_payload)

    # 3) CSV 다운로드
    download_payload = {
        "startDate": start_dash,
        "endDate": end_dash,
    }
    raw = await request_with_retry(
        session, "POST", DOWNLOAD_URL,
        data=download_payload,
        expect_csv=True
    )

    # 4) 디코딩 및 DataFrame 변환
    content = raw.decode("euc-kr", errors="ignore")
    content = transform_datetime_in_csv(content)

    from io import StringIO
    df = pd.read_csv(StringIO(content))

    return df


async def download_range(
    start_date: str,
    end_date: str,
    throttle_seconds: float = BASE_THROTTLE_SECONDS,
) -> pd.DataFrame:
    """
    전체 날짜 범위의 데이터를 다운로드합니다.
    3개월 단위로 자동 분할.
    """
    start = parse_date(start_date)
    end = parse_date(end_date)
    ranges = split_range_by_days(start, end, max_days=90)

    print(f"[INFO] {len(ranges)}개 구간으로 분할하여 다운로드")

    timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
    connector = aiohttp.TCPConnector(
        limit=10,
        force_close=False,
        enable_cleanup_closed=True,
        ttl_dns_cache=300
    )

    all_dfs = []

    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        for idx, (seg_start, seg_end) in enumerate(ranges, start=1):
            start_str = format_date(seg_start)
            end_str = format_date(seg_end)

            print(f"[{idx}/{len(ranges)}] 다운로드: {start_str} ~ {end_str}")

            try:
                df = await download_segment(session, start_str, end_str)
                if not df.empty:
                    all_dfs.append(df)
                print(f"[DONE] {start_str} ~ {end_str}: {len(df)}건")
            except Exception as e:
                print(f"[ERROR] {start_str} ~ {end_str}: {e}")
                # 5초 대기 후 1회 재시도
                await asyncio.sleep(5)
                try:
                    df = await download_segment(session, start_str, end_str)
                    if not df.empty:
                        all_dfs.append(df)
                except Exception as e2:
                    print(f"[FAIL] {start_str} ~ {end_str}: {e2}")

            await asyncio.sleep(throttle_seconds)

    if all_dfs:
        return pd.concat(all_dfs, ignore_index=True)
    return pd.DataFrame()


# ========================================
# DB Operations
# ========================================
def prepare_records(df: pd.DataFrame) -> List[dict]:
    """
    DataFrame을 DB 레코드 형태로 변환합니다.
    """
    # 컬럼명 변환
    df = df.rename(columns=COLUMN_MAPPING)

    # 필요한 컬럼만 선택
    required_cols = list(COLUMN_MAPPING.values())
    available_cols = [c for c in required_cols if c in df.columns]
    df = df[available_cols].copy()

    # timestamp 변환
    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        # 공휴일 여부 추가
        df["is_holiday"] = df["timestamp"].apply(is_holiday)

    # NaN을 None으로 변환
    df = df.where(pd.notnull(df), None)

    return df.to_dict(orient="records")


async def save_to_db(records: List[dict]) -> int:
    """
    레코드를 DB에 저장합니다. (Upsert)
    """
    if not records:
        return 0

    async with get_async_session() as session:
        # PostgreSQL INSERT ... ON CONFLICT 사용
        stmt = pg_insert(Demand5Min).values(records)
        stmt = stmt.on_conflict_do_update(
            index_elements=["timestamp"],
            set_={
                "current_demand": stmt.excluded.current_demand,
                "current_supply": stmt.excluded.current_supply,
                "supply_capacity": stmt.excluded.supply_capacity,
                "supply_reserve": stmt.excluded.supply_reserve,
                "reserve_rate": stmt.excluded.reserve_rate,
                "operation_reserve": stmt.excluded.operation_reserve,
                "is_holiday": stmt.excluded.is_holiday,
            }
        )
        await session.execute(stmt)
        await session.commit()

    return len(records)


async def get_last_timestamp() -> Optional[datetime]:
    """DB에서 가장 최근 timestamp를 조회합니다."""
    async with get_async_session() as session:
        result = await session.execute(
            select(Demand5Min.timestamp).order_by(Demand5Min.timestamp.desc()).limit(1)
        )
        row = result.scalar()
        return row


# ========================================
# CSV 기반 백필 (기존 데이터 활용)
# ========================================
CSV_COLUMN_MAPPING = {
    "기준일시": "timestamp",
    "현재수요(MW)": "current_demand",
    "공급능력(MW)": "current_supply",
    "공급예비력(MW)": "supply_reserve",
    "공급예비율(%)": "reserve_rate",
    "운영예비력(MW)": "operation_reserve",
    "공휴일": "is_holiday",
}


async def load_csv_to_db(
    csv_path: str = "/app/data/Demand_Data_all.csv",
    batch_size: int = 5000,
) -> int:
    """
    CSV 파일을 읽어서 DB에 저장합니다.
    """
    print(f"\n{'='*60}")
    print(f"CSV -> DB 로드: {csv_path}")
    print(f"{'='*60}\n")

    # DB 테이블 생성
    await init_db()

    # DB의 마지막 timestamp 조회
    last_ts = await get_last_timestamp()
    if last_ts:
        print(f"[INFO] DB 마지막 기록: {last_ts}")
    else:
        print("[INFO] DB가 비어있음. 전체 로드")

    # CSV 로드
    try:
        df = pd.read_csv(csv_path, encoding="euc-kr")
    except FileNotFoundError:
        print(f"[ERROR] CSV 파일 없음: {csv_path}")
        return 0

    print(f"[INFO] CSV 총 {len(df):,}건 로드")

    # 컬럼명 변환
    df = df.rename(columns=CSV_COLUMN_MAPPING)

    # timestamp 변환
    df["timestamp"] = pd.to_datetime(df["timestamp"])

    # 마지막 timestamp 이후 데이터만 필터링
    if last_ts:
        df = df[df["timestamp"] > last_ts]
        print(f"[INFO] 신규 데이터: {len(df):,}건")

    if df.empty:
        print("[INFO] 추가할 데이터 없음")
        return 0

    # 필요한 컬럼만 선택
    available_cols = [c for c in CSV_COLUMN_MAPPING.values() if c in df.columns]
    df = df[available_cols].copy()

    # is_holiday 처리
    if "is_holiday" in df.columns:
        df["is_holiday"] = df["is_holiday"].astype(bool)

    # NaN을 None으로 변환
    df = df.where(pd.notnull(df), None)

    # 배치 저장
    total_saved = 0
    records = df.to_dict(orient="records")

    for i in range(0, len(records), batch_size):
        batch = records[i:i + batch_size]
        saved = await save_to_db(batch)
        total_saved += saved
        print(f"[PROGRESS] {min(i + batch_size, len(records)):,}/{len(records):,} 저장됨")

    print(f"\n[DONE] 총 {total_saved:,}건 DB 저장 완료")
    return total_saved


async def collect_with_backfill(
    csv_path: str = "/app/data/Demand_Data_all.csv",
) -> int:
    """
    백필 모드: CSV 파일에서 DB에 없는 데이터를 로드합니다.
    """
    print("\n[INFO] 백필 모드 시작 (CSV 기반)")
    return await load_csv_to_db(csv_path)


async def collect_recent_hours(
    hours: int = 2,
    throttle_seconds: float = BASE_THROTTLE_SECONDS,
) -> int:
    """
    최근 N시간 데이터를 API에서 수집합니다.
    """
    end_dt = datetime.now()
    start_dt = end_dt - timedelta(hours=hours)

    start_date = start_dt.strftime("%Y%m%d")
    end_date = end_dt.strftime("%Y%m%d")

    print(f"\n[INFO] 최근 {hours}시간 데이터 수집")

    # DB 테이블 생성
    await init_db()

    # 데이터 다운로드
    df = await download_range(start_date, end_date, throttle_seconds)

    if df.empty:
        print("[WARN] 수집된 데이터 없음")
        return 0

    print(f"[INFO] 총 {len(df)}건 다운로드 완료")

    # DB 저장
    records = prepare_records(df)
    saved_count = await save_to_db(records)

    print(f"[INFO] {saved_count}건 DB 저장 완료")
    return saved_count


# ========================================
# Legacy Support (기존 파일 기반 다운로드)
# ========================================
async def download_to_file(
    start_date: str,
    end_date: str,
    out_prefix: str = "Demand_Data",
    throttle_seconds: float = BASE_THROTTLE_SECONDS,
    skip_if_exists: bool = True,
) -> None:
    """
    기존 방식: CSV 파일로 다운로드 (하위 호환성 유지)
    """
    start = parse_date(start_date)
    end = parse_date(end_date)
    ranges = split_range_by_days(start, end, max_days=90)

    print(f"[INFO] {len(ranges)}개 구간으로 분할하여 다운로드")

    timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
    connector = aiohttp.TCPConnector(limit=10)

    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        for idx, (seg_start, seg_end) in enumerate(ranges, start=1):
            start_str = format_date(seg_start)
            end_str = format_date(seg_end)
            filename = f"{out_prefix}_{start_str}_{end_str}.csv"

            if skip_if_exists and os.path.exists(filename) and os.path.getsize(filename) > 0:
                print(f"[SKIP] 이미 존재: {filename}")
                continue

            print(f"[{idx}/{len(ranges)}] 다운로드: {start_str} ~ {end_str}")

            try:
                df = await download_segment(session, start_str, end_str)
                df.to_csv(filename, index=False, encoding="euc-kr")
                print(f"[DONE] {filename}")
            except Exception as e:
                print(f"[ERROR] {start_str} ~ {end_str}: {e}")

            await asyncio.sleep(throttle_seconds)


# ========================================
# CLI Entry Point
# ========================================
if __name__ == "__main__":
    import sys

    print("전력수급 데이터 수집기")
    print("=" * 40)
    print("1. 날짜 범위 지정 수집 (DB 저장)")
    print("2. 백필 모드 (DB 마지막 이후 수집)")
    print("3. 최근 2시간 수집 (1시간 스케줄용)")
    print("4. CSV 파일로 다운로드 (기존 방식)")
    print("=" * 40)

    choice = input("선택 (1-4): ").strip()

    if choice == "1":
        dates = input("start,end = 'YYYYMMDD,YYYYMMDD': ").split(",")
        start_date = dates[0].strip()
        end_date = dates[1].strip()
        asyncio.run(collect_and_save(start_date, end_date))

    elif choice == "2":
        asyncio.run(collect_with_backfill())

    elif choice == "3":
        asyncio.run(collect_recent_hours(hours=2))

    elif choice == "4":
        dates = input("start,end = 'YYYYMMDD,YYYYMMDD': ").split(",")
        start_date = dates[0].strip()
        end_date = dates[1].strip()
        asyncio.run(download_to_file(start_date, end_date))

    else:
        print("잘못된 선택")
        sys.exit(1)
