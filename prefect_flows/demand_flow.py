"""Prefect flow for current nationwide demand collection and hourly repair."""

from __future__ import annotations

from datetime import datetime

from prefect import flow, task

from fetch_data.demand.aggregate import aggregate_demand_weather, refresh_demand_views
from fetch_data.demand.collect import collect_latest
from fetch_data.demand.database import get_demand_engine
from prefect_flows.merge_to_all import DEFAULT_MERGED_CSV
from prefect_flows.notify_tasks import notify_slack_failure


@task(name="전국 5분 전력수요 수집 실행", retries=2, retry_delay_seconds=300)
async def run_demand_collection_task(engine) -> int:
    return await collect_latest(engine)


@task(name="수요-기상 시간별 집계 실행", retries=2, retry_delay_seconds=300)
def run_hourly_aggregation_task(engine) -> int:
    return aggregate_demand_weather(engine, DEFAULT_MERGED_CSV)


@flow(name="Unified Demand Collection Flow", log_prints=True)
async def unified_demand_collection_flow(force_hourly: bool = False) -> dict[str, int]:
    """Collect current demand and refresh hourly demand-weather views when due."""
    try:
        engine = get_demand_engine()
        demand_5min = await run_demand_collection_task(engine)
        demand_weather_1h = 0
        if force_hourly or datetime.now().minute < 10:
            demand_weather_1h = run_hourly_aggregation_task(engine)
            refresh_demand_views(engine)
        return {
            "demand_5min": demand_5min,
            "demand_weather_1h": demand_weather_1h,
        }
    except Exception as error:
        notify_slack_failure.submit(
            "Unified Demand Collection", f"{type(error).__name__}: {error}"
        )
        raise
