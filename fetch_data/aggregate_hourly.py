"""
1시간 단위 기상+전력수요 데이터 통합 모듈

5분 단위 전력수급 데이터를 1시간 평균으로 집계하고,
기상 데이터와 결합하여 demand_weather_1h 테이블에 저장합니다.
"""

import asyncio
from datetime import datetime, timedelta
from typing import List, Optional

import pandas as pd
from sqlalchemy import select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from workalendar.asia import SouthKorea

from fetch_data.common.database import (
    Demand5Min,
    DemandWeather1H,
    get_async_session,
    init_db,
)


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
# DB Operations - Demand 5min
# ========================================
async def get_hourly_demand_from_db(
    start_dt: datetime,
    end_dt: datetime
) -> pd.DataFrame:
    """
    DB에서 5분 데이터를 조회하여 1시간 평균으로 집계합니다.
    """
    async with get_async_session() as session:
        query = text("""
            SELECT
                date_trunc('hour', timestamp) as hour,
                AVG(current_demand) as demand_avg,
                BOOL_OR(is_holiday) as is_holiday
            FROM demand_5min
            WHERE timestamp >= :start AND timestamp < :end
            GROUP BY date_trunc('hour', timestamp)
            ORDER BY hour
        """)

        result = await session.execute(
            query,
            {"start": start_dt, "end": end_dt}
        )
        rows = result.fetchall()

        if not rows:
            return pd.DataFrame()

        df = pd.DataFrame(rows, columns=["timestamp", "demand_avg", "is_holiday"])
        return df


# ========================================
# Weather Data Integration
# ========================================
def load_weather_from_csv(
    csv_path: str = "/app/data/asos_all_merged.csv"
) -> pd.DataFrame:
    """
    기상 데이터 CSV를 로드합니다.
    """
    try:
        df = pd.read_csv(csv_path, encoding="utf-8-sig")

        # 컬럼 표준화
        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"])

        return df
    except FileNotFoundError:
        print(f"[WARN] Weather data not found: {csv_path}")
        return pd.DataFrame()


def get_hourly_weather(
    weather_df: pd.DataFrame,
    start_dt: datetime,
    end_dt: datetime
) -> pd.DataFrame:
    """
    기상 데이터를 1시간 단위로 필터링합니다.
    이미 1시간 단위 데이터라고 가정.
    """
    if weather_df.empty:
        return pd.DataFrame()

    # 날짜 필터링
    mask = (weather_df["date"] >= start_dt) & (weather_df["date"] < end_dt)
    filtered = weather_df[mask].copy()

    # hour 컬럼 생성 (시간 단위로 truncate)
    filtered["timestamp"] = filtered["date"].dt.floor("H")

    return filtered


# ========================================
# Merge and Save
# ========================================
async def merge_and_save_hourly(
    demand_df: pd.DataFrame,
    weather_df: pd.DataFrame
) -> int:
    """
    전력수요와 기상 데이터를 병합하여 DB에 저장합니다.
    """
    if demand_df.empty:
        print("[WARN] No demand data to merge")
        return 0

    if weather_df.empty:
        print("[WARN] No weather data to merge")
        # 수요 데이터만 저장 (기상 데이터 없이)
        records = []
        for _, row in demand_df.iterrows():
            records.append({
                "timestamp": row["timestamp"],
                "station_name": "UNKNOWN",
                "temperature": None,
                "humidity": None,
                "demand_avg": row["demand_avg"],
                "is_holiday": row["is_holiday"],
            })

        return await save_to_db(records)

    # 기상 데이터의 각 관측소에 대해 수요 데이터와 조인
    records = []

    for _, weather_row in weather_df.iterrows():
        ts = weather_row["timestamp"]

        # 해당 시간의 수요 데이터 찾기
        demand_match = demand_df[demand_df["timestamp"] == ts]

        if not demand_match.empty:
            demand_avg = demand_match.iloc[0]["demand_avg"]
            is_hol = demand_match.iloc[0]["is_holiday"]
        else:
            demand_avg = None
            is_hol = is_holiday(ts)

        records.append({
            "timestamp": ts,
            "station_name": weather_row.get("station_name", "UNKNOWN"),
            "temperature": weather_row.get("temperature"),
            "humidity": weather_row.get("humidity"),
            "demand_avg": demand_avg,
            "is_holiday": is_hol,
        })

    if not records:
        return 0

    return await save_to_db(records)


async def save_to_db(records: List[dict]) -> int:
    """
    레코드를 demand_weather_1h 테이블에 저장합니다. (Upsert)
    """
    if not records:
        return 0

    async with get_async_session() as session:
        stmt = pg_insert(DemandWeather1H).values(records)
        stmt = stmt.on_conflict_do_update(
            index_elements=["timestamp", "station_name"],
            set_={
                "temperature": stmt.excluded.temperature,
                "humidity": stmt.excluded.humidity,
                "demand_avg": stmt.excluded.demand_avg,
                "is_holiday": stmt.excluded.is_holiday,
            }
        )
        await session.execute(stmt)
        await session.commit()

    return len(records)


# ========================================
# Main Functions
# ========================================
async def aggregate_and_save_hourly(
    start_dt: datetime,
    end_dt: datetime,
    weather_csv_path: str = "/app/data/asos_all_merged.csv"
) -> int:
    """
    지정된 기간의 1시간 단위 통합 데이터를 생성하고 DB에 저장합니다.
    """
    print(f"\n{'='*60}")
    print(f"1시간 단위 데이터 통합: {start_dt} ~ {end_dt}")
    print(f"{'='*60}\n")

    # DB 초기화
    await init_db()

    # 1. 5분 수요 데이터를 1시간 평균으로 집계
    print("[1/3] 전력수요 데이터 집계 중...")
    demand_df = await get_hourly_demand_from_db(start_dt, end_dt)
    print(f"      → {len(demand_df)}건 집계 완료")

    # 2. 기상 데이터 로드 및 필터링
    print("[2/3] 기상 데이터 로드 중...")
    weather_raw = load_weather_from_csv(weather_csv_path)
    weather_df = get_hourly_weather(weather_raw, start_dt, end_dt)
    print(f"      → {len(weather_df)}건 로드 완료")

    # 3. 병합 및 저장
    print("[3/3] 데이터 병합 및 저장 중...")
    saved_count = await merge_and_save_hourly(demand_df, weather_df)
    print(f"      → {saved_count}건 저장 완료")

    return saved_count


async def aggregate_recent_hours(
    hours: int = 24,
    weather_csv_path: str = "/app/data/asos_all_merged.csv"
) -> int:
    """
    최근 N시간의 데이터를 통합합니다.
    """
    end_dt = datetime.now().replace(minute=0, second=0, microsecond=0)
    start_dt = end_dt - timedelta(hours=hours)

    return await aggregate_and_save_hourly(start_dt, end_dt, weather_csv_path)


async def aggregate_with_backfill(
    default_start: str = "20240101",
    weather_csv_path: str = "/app/data/asos_all_merged.csv"
) -> int:
    """
    백필 모드: DB의 마지막 레코드 이후부터 현재까지 통합합니다.
    """
    print("\n[INFO] 1시간 데이터 백필 모드 시작")

    await init_db()

    # 마지막 timestamp 조회
    async with get_async_session() as session:
        result = await session.execute(
            select(DemandWeather1H.timestamp)
            .order_by(DemandWeather1H.timestamp.desc())
            .limit(1)
        )
        last_ts = result.scalar()

    if last_ts:
        start_dt = last_ts + timedelta(hours=1)
        print(f"[INFO] DB 마지막 기록: {last_ts}")
    else:
        start_dt = datetime.strptime(default_start, "%Y%m%d")
        print(f"[INFO] DB가 비어있음. 기본 시작일: {default_start}")

    end_dt = datetime.now().replace(minute=0, second=0, microsecond=0)

    if start_dt >= end_dt:
        print("[INFO] 이미 최신 상태입니다.")
        return 0

    return await aggregate_and_save_hourly(start_dt, end_dt, weather_csv_path)


# ========================================
# CLI Entry Point
# ========================================
if __name__ == "__main__":
    import sys

    print("1시간 단위 데이터 통합기")
    print("=" * 40)
    print("1. 날짜 범위 지정 통합")
    print("2. 백필 모드")
    print("3. 최근 24시간 통합")
    print("=" * 40)

    choice = input("선택 (1-3): ").strip()

    if choice == "1":
        start = input("시작일시 (YYYY-MM-DD HH): ")
        end = input("종료일시 (YYYY-MM-DD HH): ")
        start_dt = datetime.strptime(start, "%Y-%m-%d %H")
        end_dt = datetime.strptime(end, "%Y-%m-%d %H")
        asyncio.run(aggregate_and_save_hourly(start_dt, end_dt))

    elif choice == "2":
        asyncio.run(aggregate_with_backfill())

    elif choice == "3":
        asyncio.run(aggregate_recent_hours(hours=24))

    else:
        print("잘못된 선택")
        sys.exit(1)
