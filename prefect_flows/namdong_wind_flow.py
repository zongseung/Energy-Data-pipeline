"""
Prefect Flow: Namdong Wind Collection

남동발전 풍력 데이터를 매월 수집합니다.
스케줄: 매월 10일 오전 11시 (KST)
"""

from __future__ import annotations

from typing import Optional

from prefect import flow, task

from fetch_data.wind.namdong_wind_collect import run_namdong_wind_collection
from prefect_flows.notify_tasks import notify_slack_success, notify_slack_failure


@task(name="남동발전 풍력 수집 실행", retries=2, retry_delay_seconds=300)
def run_wind_collection(target_start: Optional[str], target_end: Optional[str]) -> int:
    return run_namdong_wind_collection(target_start, target_end)


@flow(name="Monthly Namdong Wind Collection Flow", log_prints=True)
def monthly_namdong_wind_flow(
    target_start: Optional[str] = None,
    target_end: Optional[str] = None,
) -> int:
    try:
        inserted = run_wind_collection(target_start, target_end)
        notify_slack_success.submit("Namdong Wind", f"- 적재 행수: {inserted}")
        return inserted
    except Exception as e:
        error_msg = f"{type(e).__name__}: {e}"
        notify_slack_failure.submit("Namdong Wind", error_msg)
        raise
