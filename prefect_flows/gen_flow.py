"""
Prefect Flow: KOEN 비태양광(화력/연료전지/해양소수력) 월별 수집.

수집(koenergy.kr) -> 변환(long CSV) -> generation 코어 직접 적재.
스케줄: 매월 10일 오전 10시 (KST)

- 수집·변환: fetch_data.gen.pipeline.run_pipeline (latest 증분)
- 적재      : fetch_data.gen.load_gen.load_all (plants/generation upsert, operator='namdong')
"""

from __future__ import annotations

from typing import List, Optional

from prefect import flow, task

from fetch_data.gen.load_gen import load_all
from fetch_data.gen.pipeline import run_pipeline
from prefect_flows.notify_tasks import notify_slack_failure, notify_slack_success


@task(name="비태양광 수집·변환", retries=2, retry_delay_seconds=300)
def collect_transform(gen_keys: Optional[List[str]], mode: str) -> None:
    run_pipeline(gen_keys=gen_keys, mode=mode)


@task(name="비태양광 generation 적재", retries=2, retry_delay_seconds=120)
def load_generation(gen_keys: Optional[List[str]]) -> int:
    return load_all(categories=gen_keys)


@flow(name="Monthly KOEN Gen Collection Flow", log_prints=True)
def monthly_gen_flow(
    gen_keys: Optional[List[str]] = None,
    mode: str = "latest",
) -> int:
    """전월 KOEN 비태양광을 수집·변환해 generation 코어에 적재. 처리 행수 반환."""
    try:
        collect_transform(gen_keys, mode)
        inserted = load_generation(gen_keys)
        notify_slack_success.submit(
            "KOEN Gen", f"- 카테고리: {gen_keys or '전체'}\n- generation 적재(처리) 행수: {inserted}"
        )
        return inserted
    except Exception as e:
        error_msg = f"{type(e).__name__}: {e}"
        notify_slack_failure.submit("KOEN Gen", error_msg)
        raise
