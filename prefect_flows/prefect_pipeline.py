"""
Prefect ETL Pipeline

일일 기상 데이터 수집 파이프라인
"""

import sys
import os
from pathlib import Path
from datetime import datetime, timedelta

from prefect import task, flow
import pandas as pd
import requests

# 상위 디렉토리를 sys.path에 추가
sys.path.insert(0, str(Path(__file__).parent.parent))

from fetch_data.weather.collect_asos import select_data_async, station_ids
from fetch_data.common.impute_missing import impute_missing_values
from prefect_flows.merge_to_all import merge_to_all_csv


# ==============================
# Slack 알림 유틸리티
# ==============================

def send_slack_message(text: str, webhook_url: str | None = None):
    """Slack Incoming Webhook으로 메시지 전송"""
    if webhook_url is None:
        webhook_url = os.getenv("SLACK_WEBHOOK_URL")

    if not webhook_url:
        print("SLACK_WEBHOOK_URL이 설정되어 있지 않습니다. Slack 전송 스킵.")
        return

    try:
        resp = requests.post(webhook_url, json={"text": text}, timeout=5)
        if resp.status_code != 200:
            print(f"Slack 전송 실패: {resp.status_code}, {resp.text}")
    except Exception as e:
        print(f"Slack 전송 중 예외 발생: {e}")


def send_slack_rich_message(
    title: str,
    status: str,
    details: dict,
    webhook_url: str | None = None
):
    """
    Slack Block Kit 형식의 리치 메시지 전송
    status: "success", "warning", "error"
    """
    if webhook_url is None:
        webhook_url = os.getenv("SLACK_WEBHOOK_URL")

    if not webhook_url:
        print("SLACK_WEBHOOK_URL이 설정되어 있지 않습니다.")
        return

    # 상태별 이모지
    emoji_map = {
        "success": ":white_check_mark:",
        "warning": ":warning:",
        "error": ":x:",
        "info": ":information_source:",
    }
    emoji = emoji_map.get(status, ":bell:")

    # 상세 정보 포맷팅
    detail_lines = [f"• *{k}*: {v}" for k, v in details.items()]
    detail_text = "\n".join(detail_lines)

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    payload = {
        "blocks": [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": f"{emoji} {title}",
                    "emoji": True
                }
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": detail_text
                }
            },
            {
                "type": "context",
                "elements": [
                    {
                        "type": "mrkdwn",
                        "text": f":clock1: {timestamp} KST"
                    }
                ]
            },
            {"type": "divider"}
        ]
    }

    try:
        resp = requests.post(webhook_url, json=payload, timeout=5)
        if resp.status_code != 200:
            print(f"Slack 전송 실패: {resp.status_code}")
    except Exception as e:
        print(f"Slack 전송 예외: {e}")


@task(name="Slack 성공 알림", retries=0)
def notify_slack_success(flow_name: str, details: str):
    msg = f"[{flow_name} 완료]\n{details}"
    send_slack_message(msg)


@task(name="Slack 실패 알림", retries=0)
def notify_slack_failure(flow_name: str, error_msg: str):
    msg = f"[{flow_name} 실패]\n- 에러: {error_msg}"
    send_slack_message(msg)


@task(name="Slack 리치 알림", retries=0)
def notify_slack_rich(title: str, status: str, details: dict):
    send_slack_rich_message(title, status, details)


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

    df = df[needed_cols]
    print(f"수집된 데이터: {len(df)}건")

    return df


@task(name="결측치 처리", retries=2)
def process_missing_values(df: pd.DataFrame) -> pd.DataFrame:
    """결측치를 처리합니다."""
    print("\n결측치 처리 시작...")

    result = impute_missing_values(
        df,
        columns=["ta", "hm"],
        date_col="tm",
        station_col="stnNm",
        debug=True,
    )

    if isinstance(result, tuple):
        df_imputed, _ = result
    else:
        df_imputed = result

    df_imputed = df_imputed.rename(columns={
        "tm": "date",
        "hm": "humidity",
        "ta": "temperature",
        "stnNm": "station_name",
    })

    return df_imputed


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

        # 2. 결측치 처리
        df_processed_future = process_missing_values.submit(df_future)

        # 3. 데이터 저장
        output_path_future = save_weather_data.submit(df_processed_future, target_date)

        # 4. 통합 파일에 병합
        merged_path_future = merge_weather_to_all.submit(output_path_future)

        output_path = output_path_future.result()
        merged_path = merged_path_future.result()

        print("\n플로우 완료!")

        # 5. Slack 알림
        notify_slack_success.submit(
            "Weather ETL",
            f"- 날짜: {target_date}\n- 파일: {output_path}\n- 통합: {merged_path}"
        )

        return output_path

    except Exception as e:
        error_msg = f"{type(e).__name__}: {e}"
        print(f"\n플로우 실행 중 에러 발생: {error_msg}")
        notify_slack_failure.submit("Weather ETL", error_msg)
        raise


@flow(name="full-etl-flow", log_prints=True)
def full_etl_flow(target_date: str | None = None):
    """
    전체 ETL 플로우

    기상 데이터 수집을 수행합니다.
    """
    print(f"\n{'='*60}")
    print("전체 ETL 플로우 시작")
    print(f"실행 시각: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*60}\n")

    results = {}

    try:
        # 기상 데이터 수집
        print("\n[1/1] 기상 데이터 수집...")
        weather_path = daily_weather_collection_flow(target_date)
        results["weather"] = weather_path

        print(f"\n전체 ETL 완료!")
        print(f"- 기상 데이터: {weather_path}")

        notify_slack_success.submit(
            "Full ETL",
            f"- 기상: {weather_path}"
        )

        return results

    except Exception as e:
        error_msg = f"{type(e).__name__}: {e}"
        print(f"\nETL 중 에러 발생: {error_msg}")
        notify_slack_failure.submit("Full ETL", error_msg)
        raise
