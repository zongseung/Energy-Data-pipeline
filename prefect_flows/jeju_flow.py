"""
제주 전력 데이터 수집 Prefect Flows

1. jeju_realtime_flow        : 5분마다 제주 계통수급 실시간 수집
2. jeju_sukub_monthly_flow   : 매월 1일 전월 수급 데이터 백필
3. jeju_gen_monthly_flow     : 매월 1일 연료원별 전력거래량 수집
4. jeju_demand_flow          : 분기마다 시간별 전력수요 수집
"""

from __future__ import annotations

from datetime import date
from typing import Optional

from prefect import flow, task, get_run_logger

from fetch_data.common.date_utils import month_end, add_months
from fetch_data.jeju.jeju_realtime_collect import run_once as _realtime_once
from fetch_data.jeju.jeju_sukub_collect import run as sukub_run
from fetch_data.jeju.jeju_gen_collect import run as gen_run
from fetch_data.jeju.jeju_demand_collect import run as demand_run


# ─── 실시간 수급 (5분마다) ───────────────────────────────────────────────────

@task(name="제주 계통수급 실시간 fetch", retries=2, retry_delay_seconds=60)
async def fetch_jeju_realtime():
    added = await _realtime_once()
    get_run_logger().info(f"[실시간] +{added}행 추가")
    return added


@flow(name="jeju-realtime-collection", log_prints=True)
async def jeju_realtime_flow():
    """5분마다 제주 계통수급 실시간 1회 수집 (Prefect가 스케줄 담당)."""
    await fetch_jeju_realtime()


# ─── 수급 월별 백필 (매월 1일) ─────────────────────────────────────────────

@task(name="제주 수급 월별 수집", retries=2, retry_delay_seconds=120)
def collect_jeju_sukub_monthly(target_month: Optional[str] = None):
    """전월 수급 데이터 수집. target_month=YYYY-MM 형식 또는 None(자동 전월)."""
    logger = get_run_logger()
    if target_month:
        y, m = map(int, target_month.split("-"))
        start = date(y, m, 1)
        end = month_end(start)
    else:
        today = date.today()
        start = add_months(today.replace(day=1), -1)
        end = month_end(start)

    logger.info(f"수집 기간: {start} ~ {end}")
    saved = sukub_run(start, end)
    logger.info(f"저장 완료: {len(saved)}개월")
    return [str(p) for p in saved]


@flow(name="jeju-sukub-monthly-collection", log_prints=True)
def jeju_sukub_monthly_flow(target_month: Optional[str] = None):
    """매월 1일 전월 수급 데이터 백필."""
    collect_jeju_sukub_monthly(target_month)


# ─── 연료원별 거래량 (매월 1일) ────────────────────────────────────────────

@task(name="제주 연료원별 거래량 수집", retries=2, retry_delay_seconds=120)
def collect_jeju_gen():
    """공공데이터포털 제주 연료원별 전력거래량 수집 (연간 파일)."""
    logger = get_run_logger()
    saved = gen_run()
    logger.info(f"저장 완료: {len(saved)}개 파일")
    return [str(p) for p in saved]


@flow(name="jeju-gen-monthly-collection", log_prints=True)
def jeju_gen_monthly_flow():
    """매월 1일 연료원별 전력거래량 연간 파일 수집."""
    collect_jeju_gen()


# ─── 시간별 전력수요 (분기마다) ────────────────────────────────────────────────

@task(name="제주 시간별 전력수요 수집", retries=2, retry_delay_seconds=120)
def collect_jeju_demand(from_sukub: bool = False):
    """data.go.kr 15065239 분기파일 + 선택적으로 sukub 5분 집계."""
    logger = get_run_logger()
    saved = demand_run(quarterly=True, from_sukub=from_sukub)
    logger.info(f"저장 완료: {len(saved)}개 파일")
    return [str(p) for p in saved]


@flow(name="jeju-demand-quarterly-collection", log_prints=True)
def jeju_demand_flow(from_sukub: bool = False):
    """분기마다 시간별 제주 전력수요 수집 (data.go.kr 15065239)."""
    collect_jeju_demand(from_sukub=from_sukub)


# ─── 제주 수급 5분 → demand-postgres 동기화 (10분, energy_hub FDW 소비용) ──────

from fetch_data.jeju.load_jeju_demand_db import run as _jeju_demand_db_run


@task(name="제주 수급 → demand DB 동기화", retries=2, retry_delay_seconds=60)
def sync_jeju_supply_demand() -> int:
    n = _jeju_demand_db_run(months_back=2)
    get_run_logger().info(f"[제주수급→demand DB] {n}행 upsert")
    return n


@flow(name="jeju-supply-demand-db-sync", log_prints=True)
def jeju_supply_demand_db_flow() -> int:
    """10분마다 제주 수급 5분 CSV를 demand-postgres로 동기화 (energy_hub가 FDW로 소비)."""
    return sync_jeju_supply_demand()
