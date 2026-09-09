"""
공통 날짜/시간 유틸리티

- prev_month_range: 전월 시작일~종료일 반환
- month_end / add_months / to_yyyymmdd / validate_yyyymmdd / to_date_yyyymmdd: 월·날짜 변환 유틸
"""

import re
from datetime import date, datetime, timedelta
from typing import Optional


def prev_month_range(ref_dt: Optional[date] = None) -> tuple[date, date]:
    """전월 시작일~종료일 반환"""
    if ref_dt is None:
        ref_dt = date.today()
    first_of_this_month = date(ref_dt.year, ref_dt.month, 1)
    prev_end = first_of_this_month - timedelta(days=1)
    prev_start = date(prev_end.year, prev_end.month, 1)
    return prev_start, prev_end


def month_end(d: date) -> date:
    """d가 속한 달의 말일."""
    nxt = date(d.year + 1, 1, 1) if d.month == 12 else date(d.year, d.month + 1, 1)
    return nxt - timedelta(days=1)


def add_months(d: date, n: int) -> date:
    """d가 속한 달의 1일에서 n개월 이동한 1일(음수 가능)."""
    idx = d.year * 12 + (d.month - 1) + n
    y, m = divmod(idx, 12)
    return date(y, m + 1, 1)


def to_yyyymmdd(d: date) -> str:
    """date → 'YYYYMMDD' 문자열."""
    return d.strftime("%Y%m%d")


def validate_yyyymmdd(s: str) -> str:
    """'YYYYMMDD' 형식 검증 후 그대로 반환(아니면 ValueError)."""
    if not re.fullmatch(r"\d{8}", s):
        raise ValueError(f"YYYYMMDD 형식이어야 합니다: {s!r}")
    datetime.strptime(s, "%Y%m%d")
    return s


def to_date_yyyymmdd(s: str) -> date:
    """'YYYYMMDD' 문자열 → date (형식 검증 포함)."""
    return datetime.strptime(validate_yyyymmdd(s), "%Y%m%d").date()
