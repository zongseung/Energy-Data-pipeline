"""
Prefect Flows: SMP(계통한계가격) 수집

- daily_smp_collection_flow   : 매일 09:00  전날 하루전시장 시간별 + 일별 가중평균(육지/제주)
- monthly_smp_aggregate_flow  : 매월 2일 07:00  월별/연도별 공식 가중평균
- daily_smp_realtime_jeju_flow: 매일 19:00  제주 실시간 15분(전일 확정)
- weekly_smp_legacy_sync_flow : 매주 월 07:00  공통 DB -> 개인 DB 백업 동기화

기존 PV/Wind/Weather flow와 동일하게 try/except + Slack 알림 패턴을 따른다.
개인 DB 백업 실패는 flow 실패로 보지 않고 경고만 보낸다(공통 DB 무영향).
"""

from __future__ import annotations

from prefect import flow, task

from fetch_data.smp.smp_aggregate import run_aggregate
from fetch_data.smp.smp_collect import run_smp_collection
from fetch_data.smp.smp_realtime import run_realtime_collection
from fetch_data.smp.legacy_sync import run_legacy_sync
from prefect_flows.notify_tasks import notify_slack_failure, notify_slack_success


# ========================================
# 1. 하루전시장 시간별 + 일별 가중평균
# ========================================

@task(name="SMP 시간별 수집 실행", retries=2, retry_delay_seconds=300)
def run_smp_collection_task() -> int:
    return run_smp_collection()


@flow(name="Daily SMP Collection Flow", log_prints=True)
def daily_smp_collection_flow() -> int:
    try:
        inserted = run_smp_collection_task()
        notify_slack_success.submit("SMP Hourly", f"- 시간별 적재 행수: {inserted}")
        return inserted
    except Exception as e:
        notify_slack_failure.submit("SMP Hourly", f"{type(e).__name__}: {e}")
        raise


# ========================================
# 2. 월/연 가중평균
# ========================================

@task(name="SMP 월/연 가중평균 수집 실행", retries=2, retry_delay_seconds=300)
def run_smp_aggregate_task() -> int:
    return run_aggregate("all")


@flow(name="Monthly SMP Aggregate Flow", log_prints=True)
def monthly_smp_aggregate_flow() -> int:
    try:
        inserted = run_smp_aggregate_task()
        notify_slack_success.submit("SMP Aggregate", f"- 월/연 가중평균 적재 행수: {inserted}")
        return inserted
    except Exception as e:
        notify_slack_failure.submit("SMP Aggregate", f"{type(e).__name__}: {e}")
        raise


# ========================================
# 3. 제주 실시간 15분
# ========================================

@task(name="제주 실시간 SMP 수집 실행", retries=2, retry_delay_seconds=300)
def run_smp_realtime_task() -> int:
    return run_realtime_collection()


@flow(name="Daily SMP Realtime Jeju Flow", log_prints=True)
def daily_smp_realtime_jeju_flow() -> int:
    try:
        inserted = run_smp_realtime_task()
        notify_slack_success.submit("SMP Realtime Jeju", f"- 실시간 적재 행수: {inserted}")
        return inserted
    except Exception as e:
        notify_slack_failure.submit("SMP Realtime Jeju", f"{type(e).__name__}: {e}")
        raise


# ========================================
# 4. 개인 DB 백업 동기화 (weekly)
# ========================================

@task(name="SMP 개인 DB 백업 동기화 실행", retries=1, retry_delay_seconds=300)
def run_smp_legacy_sync_task() -> int:
    return run_legacy_sync()


@flow(name="Weekly SMP Legacy Sync Flow", log_prints=True)
def weekly_smp_legacy_sync_flow() -> int:
    """공통 DB -> 개인 DB 백업. 실패해도 flow 실패로 보지 않고 경고만(공통 DB 무영향)."""
    try:
        synced = run_smp_legacy_sync_task()
        notify_slack_success.submit("SMP Legacy Sync", f"- 개인 DB 동기화 행수: {synced}")
        return synced
    except Exception as e:
        notify_slack_failure.submit("SMP Legacy Sync(경고)", f"{type(e).__name__}: {e}")
        return 0
