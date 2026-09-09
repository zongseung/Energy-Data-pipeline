"""
Prefect Flow: Nambu PV collection/backfill

남부발전 PV 데이터를 core plants/generation의 마지막 적재 시점 이후부터 어제까지 백필합니다.
"""

from __future__ import annotations

from prefect import flow, task

from pipeline.fetch_data.pv.nambu_collect import solar_automation_flow
from pipeline.prefect_flows.notify_tasks import (
    assert_rows_loaded,
    notify_slack_failure,
    notify_slack_success,
)


@task(name="남부발전 PV 수집 실행", retries=2, retry_delay_seconds=300)
def run_nambu_collection() -> int:
    return solar_automation_flow()


@flow(name="Daily Nambu PV Collection Flow", log_prints=True)
def daily_nambu_collection_flow() -> int:
    try:
        rows = run_nambu_collection()
        assert_rows_loaded("Nambu PV", rows)
        notify_slack_success.submit("Nambu PV", f"- 수집/백필 적재 행수: {rows}")
        return rows
    except Exception as e:
        error_msg = f"{type(e).__name__}: {e}"
        notify_slack_failure.submit("Nambu PV", error_msg)
        raise
