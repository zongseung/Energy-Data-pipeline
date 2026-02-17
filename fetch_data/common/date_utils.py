"""
공통 날짜/시간 유틸리티

- extract_hour0: API 시간 컬럼(qhorgen01~24)에서 0-based 시간 인덱스 추출
- prev_month_range: 전월 시작일~종료일 반환
"""

import re
from datetime import date, timedelta
from typing import Optional


def extract_hour0(col: str) -> int:
    """
    시간 컬럼명에서 0-based 시간 인덱스를 추출합니다.

    Examples:
        qhorgen01 -> 0
        qhorgen24 -> 23
    """
    m = re.search(r"(\d+)$", col)
    if not m:
        raise ValueError(f"시간 컬럼 파싱 실패: {col}")
    return int(m.group(1)) - 1


def prev_month_range(ref_dt: Optional[date] = None) -> tuple[date, date]:
    """전월 시작일~종료일 반환"""
    if ref_dt is None:
        ref_dt = date.today()
    first_of_this_month = date(ref_dt.year, ref_dt.month, 1)
    prev_end = first_of_this_month - timedelta(days=1)
    prev_start = date(prev_end.year, prev_end.month, 1)
    return prev_start, prev_end
