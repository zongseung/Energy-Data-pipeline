"""시간별 국제유가 수집 flow — 매시 정각.

수집 로직은 `fetch_data/oil/oil_hourly.py` 에 있고 여기서는 스케줄·재시도·Slack
알림만 맡는다(다른 수집기와 같은 구조).
"""

from __future__ import annotations

import asyncio

from prefect import flow, task

from fetch_data.oil.oil_hourly import run_once
from prefect_flows.notify_tasks import notify_slack_failure, notify_slack_success


@task(name="시간별 유가 수집 실행", retries=2, retry_delay_seconds=120)
def run_oil_hourly_task() -> int:
    return asyncio.run(run_once())


@flow(name="Hourly Oil Price Flow", log_prints=True)
def hourly_oil_flow() -> int:
    try:
        rows = run_oil_hourly_task()
        if rows == 0:
            # 0행은 원천이 빈 응답을 준 것이다. 한두 번은 흔들림이지만 계속되면
            # 티커가 바뀐 것이므로 성공 알림과 구별되게 보낸다 — 제주 실시간 SMP 가
            # "적재 행수: 0" 을 성공으로 72회 보내는 동안 두 달을 놓친 전례가 있다.
            notify_slack_success(
                "Oil Hourly",
                "- ⚠ 수집 0행. 며칠 이상 이어지면 Hyperliquid 티커"
                "(xyz:BRENTOIL / xyz:CL)가 바뀌었는지 확인하라.",
            )
        else:
            notify_slack_success("Oil Hourly", f"- 시간별 유가 수집 행수: {rows}")
        return rows
    except Exception as e:
        notify_slack_failure("Oil Hourly", f"{type(e).__name__}: {e}")
        raise


if __name__ == "__main__":
    print("수집 행수:", hourly_oil_flow())
