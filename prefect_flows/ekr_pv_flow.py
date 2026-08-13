"""
Prefect Flow: 한국농어촌공사 영암/율치 태양광 (odcloud 15005796) 연간 수집.

연간 갱신 데이터(차기 등록 2027-01-06) → 매년 1회 실행.
OAS에서 연도 uddi를 동적 enumerate해 전체를 멱등 수집 → generation/plants 코어 적재.
"""
from __future__ import annotations

from prefect import flow, task, get_run_logger

from fetch_data.pv.ekr_collect import run as ekr_run
from prefect_flows.notify_tasks import notify_slack_success, notify_slack_failure


@task(name="EKR 영암/율치 PV 수집", retries=2, retry_delay_seconds=300)
def collect_ekr_pv() -> int:
    n = ekr_run()  # 전체 연도(멱등 upsert) — 신규 연도는 OAS에서 자동 포함
    get_run_logger().info(f"[EKR PV] generation {n}행 적재")
    return n


@flow(name="Yearly EKR PV Collection Flow", log_prints=True)
def yearly_ekr_pv_flow() -> int:
    try:
        n = collect_ekr_pv()
        notify_slack_success.submit("EKR PV", f"- generation {n}행 적재")
        return n
    except Exception as e:
        notify_slack_failure.submit("EKR PV", f"{type(e).__name__}: {e}")
        raise
