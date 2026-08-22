"""
Prefect Flow: KPX 실시간 발전원별 발전량 5분 수집.

KPX 페이지가 **당일치만** 싣고 지나간 날은 다시 못 받으므로 5분마다 돌린다.
매번 당일 전체를 다시 UPSERT 하므로 한두 번 실패해도 다음 실행이 메운다.
"""
from __future__ import annotations

from prefect import flow, task, get_run_logger

from fetch_data.demand.genmix_collect import run as genmix_run
from prefect_flows.notify_tasks import notify_slack_failure


@task(name="KPX 발전원별 5분 수집", retries=2, retry_delay_seconds=60)
def collect_gen_mix() -> int:
    n = genmix_run()
    get_run_logger().info(f"[genmix] {n}행 UPSERT")
    return n


@flow(name="Realtime Generation Mix Flow", log_prints=True)
def realtime_gen_mix_flow() -> int:
    try:
        return collect_gen_mix()
    except Exception as e:
        # 5분마다 도는 flow라 실패 알림이 시끄러워지지 않게 예외 종류만 보낸다
        notify_slack_failure.submit("발전원별 5분", f"{type(e).__name__}: {e}")
        raise
