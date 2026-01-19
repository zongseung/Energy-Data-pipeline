"""
Prefect Flow: Nambu PV collection/backfill

남부발전 PV 데이터(nambu_generation)를 마지막 적재 시점 이후부터 어제까지 백필합니다.
"""

from __future__ import annotations

from prefect import flow, task

from fetch_data.pv.daily_pv_automation import solar_automation_flow
from notify.slack_notifier import send_slack_message


@task(name="남부발전 PV 수집 실행", retries=2, retry_delay_seconds=300)
def run_nambu_collection() -> None:
    solar_automation_flow()


@task(name="Slack 성공 알림", retries=0)
def notify_success(details: str) -> None:
    send_slack_message(f"[Nambu PV 완료]\n{details}")


@task(name="Slack 실패 알림", retries=0)
def notify_failure(error_msg: str) -> None:
    send_slack_message(f"[Nambu PV 실패]\n- 에러: {error_msg}")


@flow(name="Daily Nambu PV Collection Flow", log_prints=True)
def daily_nambu_collection_flow() -> None:
    try:
        run_nambu_collection()
        notify_success.submit("- 수집/백필 실행 완료")
    except Exception as e:
        error_msg = f"{type(e).__name__}: {e}"
        notify_failure.submit(error_msg)
        raise

