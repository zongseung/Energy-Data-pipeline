"""
Prefect Flow: Namdong PV (남동발전 태양광) 월별 수집.

수집 로직은 fetch_data.pv.namdong_collect.run_namdong_collection 에 위임한다
(수집기에는 @flow를 두지 않는다 — flow는 prefect_flows/ 에만).
"""
from pathlib import Path
from typing import List, Optional

from prefect import flow

from fetch_data.pv.namdong_collect import run_namdong_collection


@flow(name="Monthly Namdong PV Collection Flow", log_prints=True)
def monthly_namdong_pv_flow(
    target_start: Optional[str] = None,
    target_end: Optional[str] = None,
    sleep_sec: int = 5,
) -> List[Path]:
    return run_namdong_collection(target_start, target_end, sleep_sec)
