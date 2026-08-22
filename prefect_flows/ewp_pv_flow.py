"""
Prefect Flow: 한국동서발전(EWP) 지점별 태양광 (data.go.kr 15099650) 연간 수집.

연간 갱신 파일데이터 → 매년 1회 실행. 전체 기간을 멱등 재적재한다
(발전기 5기 × 4년 ≈ 16만 행이라 통째로 다시 넣어도 2분이면 끝난다).
"""
from __future__ import annotations

from prefect import flow, task, get_run_logger

from fetch_data.pv.ewp_collect import run as ewp_run
from prefect_flows.notify_tasks import notify_slack_success, notify_slack_failure


@task(name="EWP 지점별 PV 수집", retries=2, retry_delay_seconds=300)
def collect_ewp_pv() -> int:
    n = ewp_run()
    get_run_logger().info(f"[EWP PV] generation {n}행 적재")
    return n


@flow(name="Yearly EWP PV Collection Flow", log_prints=True)
def yearly_ewp_pv_flow() -> int:
    try:
        n = collect_ewp_pv()
        notify_slack_success.submit("EWP PV", f"- generation {n}행 적재")
        return n
    except Exception as e:
        notify_slack_failure.submit("EWP PV", f"{type(e).__name__}: {e}")
        raise
