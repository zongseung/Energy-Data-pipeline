"""
Prefect ETL Pipeline

일일 기상 데이터 수집 파이프라인
"""

import sys
from pathlib import Path
from datetime import datetime, timedelta

from prefect import task, flow
import pandas as pd

# 상위 디렉토리를 sys.path에 추가
sys.path.insert(0, str(Path(__file__).parent.parent))

from fetch_data.weather.asos_collect import (
    normalize_weather_data,
    select_data_async,
    station_ids,
)
from fetch_data.weather.database import load_asos_df
from prefect_flows.merge_to_all import merge_to_all_csv
from prefect_flows.notify_tasks import notify_slack_success, notify_slack_failure


# ==============================
# 기상 데이터 수집 Tasks
# ==============================

@task(name="기상 데이터 수집", retries=3, retry_delay_seconds=300)
async def collect_weather_data(date_str: str) -> pd.DataFrame:
    """특정 날짜의 기상 데이터를 수집합니다."""
    print(f"\n{'='*60}")
    print(f"기상 데이터 수집: {date_str}")
    print(f"{'='*60}\n")

    df = await select_data_async(station_ids, date_str, date_str)

    if df.empty:
        raise ValueError(f"날짜 {date_str}에 대해 수집된 데이터가 없습니다.")

    needed_cols = ["tm", "hm", "ta", "stnNm"]
    missing_cols = [c for c in needed_cols if c not in df.columns]
    if missing_cols:
        raise ValueError(f"필요한 컬럼이 없습니다: {missing_cols}")

    # icsr(일사량)은 필수 컬럼에서 제외 — 없어도 기온/습도 수집은 그대로 성공해야 한다.
    # 있으면 함께 가져가고, normalize_weather_data가 NULL 처리를 맡는다.
    select_cols = needed_cols + (["icsr"] if "icsr" in df.columns else [])
    df = df[select_cols]
    print(f"수집된 데이터: {len(df)}건")

    return df


@task(name="기상 컬럼 정규화", retries=2)
def process_missing_values(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize raw ASOS columns without inventing missing measurements."""
    return normalize_weather_data(df)


@task(name="기상 데이터 저장", retries=2)
def save_weather_data(df: pd.DataFrame, date_str: str, output_dir: str = "/app/data") -> str:
    """처리된 기상 데이터를 CSV로 저장합니다."""
    output_path = f"{output_dir}/asos_{date_str}_{date_str}.csv"
    df.to_csv(output_path, index=False, encoding="utf-8-sig")

    print(f"\n저장 완료: {output_path}")
    print(f"데이터 shape: {df.shape}")

    return output_path


@task(name="기상 데이터 병합", retries=2)
def merge_weather_to_all(output_path: str) -> str:
    """새로 저장된 CSV를 통합 파일에 병합합니다."""
    merged_path = merge_to_all_csv(output_path)
    return merged_path


@task(name="기상 데이터 DB 적재", retries=2, retry_delay_seconds=60)
def load_weather_to_db(df: pd.DataFrame) -> int:
    """정규화된 기상 데이터를 weather_asos 테이블에 적재합니다.

    예외를 여기서 삼키지 않는다 — 삼키면 retries가 발동하지 않고 Prefect UI에서도
    Failed로 안 보여 조용히 데이터가 누락된다. CSV 저장 결과를 지키는 일은
    (플로우가 output_path를 반환해 이 태스크의 실패 상태가 플로우 상태에
    반영되지 않는 것과) 호출부의 `db_rows_future.result(raise_on_failure=False)`가
    맡는다.
    """
    return load_asos_df(df)


# ==============================
# Utility Functions
# ==============================

def normalize_date_format(date_str: str) -> str:
    """날짜 문자열을 YYYYMMDD 형식으로 변환합니다."""
    normalized = date_str.replace("-", "").replace("/", "")

    if len(normalized) != 8 or not normalized.isdigit():
        raise ValueError(f"날짜 형식이 올바르지 않습니다: {date_str}")

    return normalized


# ==============================
# Prefect Flows
# ==============================

@flow(name="daily-weather-collection-flow", log_prints=True)
def daily_weather_collection_flow(target_date: str | None = None):
    """
    일일 기상 데이터 수집 플로우

    매일 오전 9시에 전날 데이터를 수집합니다.
    """
    if target_date is None:
        yesterday = datetime.now() - timedelta(days=1)
        target_date = yesterday.strftime("%Y%m%d")
    else:
        target_date = normalize_date_format(target_date)

    print(f"\n{'='*60}")
    print("일일 기상 데이터 수집 플로우 시작")
    print(f"수집 대상 날짜: {target_date}")
    print(f"실행 시각: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*60}\n")

    try:
        # 1. 데이터 수집
        df_future = collect_weather_data.submit(target_date)

        # 2. 원본 컬럼 정규화
        df_processed_future = process_missing_values.submit(df_future)

        # 3. 데이터 저장
        output_path_future = save_weather_data.submit(df_processed_future, target_date)

        # 4. 통합 파일에 병합
        merged_path_future = merge_weather_to_all.submit(output_path_future)

        # 5. DB 적재 (CSV 저장/병합과 별개로 병렬 진행, 실패해도 플로우에 영향 없음)
        db_rows_future = load_weather_to_db.submit(df_processed_future)

        output_path = output_path_future.result()
        merged_path = merged_path_future.result()
        # DB 적재는 재시도까지 다 실패해도 CSV 결과를 되돌리지 않는다 — 예외를
        # 올리지 않고 결과만 받는다. 실패 시 Slack 문자열에 예외가 그대로 찍혀
        # 눈에 띈다(태스크 런 자체는 Prefect UI에 Failed로 관측 가능).
        db_rows = db_rows_future.result(raise_on_failure=False)

        print("\n플로우 완료!")

        # 6. Slack 알림
        notify_slack_success.submit(
            "Weather ETL",
            f"- 날짜: {target_date}\n- 파일: {output_path}\n- 통합: {merged_path}\n- DB 적재: {db_rows}행"
        )

        return output_path

    except Exception as e:
        error_msg = f"{type(e).__name__}: {e}"
        print(f"\n플로우 실행 중 에러 발생: {error_msg}")
        notify_slack_failure.submit("Weather ETL", error_msg)
        raise
