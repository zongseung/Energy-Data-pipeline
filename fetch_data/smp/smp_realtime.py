"""
제주 실시간시장 SMP(15분 단위) 수집.

제주 전력시장 선진화 시범사업(2024.6~)의 실시간시장 SMP. 거래단위 15분,
1시간=4구간, 1일 96구간. 음수 가격 가능(재생에너지 과잉). 확정값은 D+1 18시 공표.

KPX 페이지(bidSmpLfdDataRt.es, GET)는 최근 6일치를 한 표로 준다.
표 구조(rowspan 확장 후): r0=['구분','구분',MM.DD...], 데이터행=[Nh, K구간, 날짜별 값...]
구간 행은 위->아래로 1h1구간..24h4구간 = 슬롯 1..96 순서.
미확정(당일/미래) 칸은 "확정가격은 D+1일..." 플레이스홀더 → 숫자 아니면 skip.

시간 변환: 슬롯 K(0-based) -> timestamp = 당일 00:00 + K*15분.
  (Nh K구간 = (N-1)시 + (K-1)*15분 과 동치)

기본 동작: 96슬롯이 모두 확정(숫자)인 날짜만 is_confirmed=True로 upsert.
매일 1회 실행해도 최근 며칠을 자가보정한다.

사용 예:
    uv run python -m fetch_data.smp.smp_realtime
"""

from __future__ import annotations

import argparse
import re
from datetime import date, datetime, timedelta
from typing import List, Optional

import pandas as pd

from fetch_data.common.logger import get_logger
from fetch_data.constants import SMPAPI
from fetch_data.smp import _common as C
from fetch_data.smp.smp_scraper import fetch_grid, make_session

logger = get_logger(__name__)

_MMDD_RE = re.compile(r"^\s*(\d{1,2})\.(\d{1,2})")
_HOUR_RE = re.compile(r"^\s*(\d+)\s*[hH]\s*$")
_GUGAN_RE = re.compile(r"^\s*(\d+)\s*구간\s*$")
SLOTS = SMPAPI.REALTIME_SLOTS_PER_DAY  # 96


def _header_dates(header: List[str], ref: date) -> List[Optional[date]]:
    """헤더 각 셀의 MM.DD를 실제 date로 변환. MM.DD가 아닌 셀은 None.

    날짜 열은 좌->우로 '연속된 매일'이다(KPX 실시간 표). 그래서 첫 날짜 열의
    절대날짜(date0)만 정하고, 이후 날짜 열은 +1일씩 순차 증가시킨다.
    이렇게 하면 한 창이 1년을 넘겨도(예: 2024-03~2025-05) 연도 경계가
    자동으로 맞는다. (MM.DD를 셀마다 ref로 추정하면 1년 초과 창에서 어긋남.)

    date0 결정: 첫 MM.DD를 ref.year에 붙이되, ref보다 미래면 작년으로(연말 창 대비).
    """
    # 첫 MM.DD 위치/값
    first_idx = next((i for i, c in enumerate(header) if _MMDD_RE.match(c)), None)
    if first_idx is None:
        return [None] * len(header)
    m = _MMDD_RE.match(header[first_idx])
    mm0, dd0 = int(m.group(1)), int(m.group(2))
    try:
        date0 = date(ref.year, mm0, dd0)
    except ValueError:
        return [None] * len(header)
    if date0 > ref:  # 첫 열이 ref보다 미래면 직전 해가 시작
        date0 = date(ref.year - 1, mm0, dd0)

    out: List[Optional[date]] = []
    seq = 0  # 날짜 열 순번
    for c in header:
        if _MMDD_RE.match(c):
            out.append(date0 + timedelta(days=seq))
            seq += 1
        else:
            out.append(None)
    return out


def parse_realtime_grid(grid: List[List[str]], ref: date) -> pd.DataFrame:
    """실시간 그리드 -> DataFrame(timestamp, region, price, is_confirmed).

    96슬롯이 모두 숫자인(확정) 날짜 열만 채택한다.
    ref: 연도 추정 기준일(수집 시점).
    """
    if not grid or len(grid) < 2:
        raise RuntimeError("제주 실시간 SMP 원천 데이터 형식이 올바르지 않습니다")

    header = grid[0]
    col_dates = _header_dates(header, ref)
    date_list = [d for d in col_dates if d is not None]  # 좌->우(과거->오늘)
    n_dates = len(date_list)
    if n_dates == 0:
        raise RuntimeError("제주 실시간 SMP 원천 데이터 형식이 올바르지 않습니다")

    # 구간(슬롯) 행만 순서대로 수집
    slot_rows = [r for r in grid[1:] if any(_GUGAN_RE.match(c) for c in r)]
    expected_slots = [(slot // 4 + 1, slot % 4 + 1) for slot in range(SLOTS)]
    actual_slots = []
    for row in slot_rows:
        hour = _HOUR_RE.match(row[0]) if row else None
        interval = _GUGAN_RE.match(row[1]) if len(row) > 1 else None
        actual_slots.append(
            (int(hour.group(1)), int(interval.group(1)))
            if hour and interval
            else None
        )
    if (
        len(slot_rows) != SLOTS
        or any(len(row) < n_dates + 2 for row in slot_rows)
        or actual_slots != expected_slots
    ):
        logger.warning(f"[realtime] 잘못된 구간 구조: {len(slot_rows)} rows")
        raise RuntimeError("제주 실시간 SMP 원천 데이터 형식이 올바르지 않습니다")

    # 날짜별 96 슬롯 가격 수집 (각 행의 날짜값 = 마지막 n_dates 셀)
    by_date: dict = {d: [None] * SLOTS for d in date_list}
    for slot_idx, row in enumerate(slot_rows):  # slot_idx 0..95
        values = row[-n_dates:] if len(row) >= n_dates else row
        for j, d in enumerate(date_list):
            by_date[d][slot_idx] = C.parse_price(values[j]) if j < len(values) else None

    records = []
    for d in date_list:
        prices = by_date[d]
        if not all(p is not None for p in prices):
            continue  # 미확정(플레이스홀더 포함) 날짜는 skip
        day_start = datetime(d.year, d.month, d.day)
        for slot_idx, price in enumerate(prices):
            ts = day_start + timedelta(minutes=15 * slot_idx)
            records.append(
                {"timestamp": ts, "region": "jeju", "price": price, "is_confirmed": True}
            )
    return pd.DataFrame(records)


def run_realtime_collection(db_url: Optional[str] = None) -> int:
    """제주 실시간 15분 SMP 수집/적재. 반환: 적재 행수."""
    session = make_session()
    grid = fetch_grid(
        session,
        SMPAPI.REALTIME_JEJU_URL,
        SMPAPI.REALTIME_JEJU_MID,
        extra_params=SMPAPI.REALTIME_JEJU_PARAMS,
    )
    if grid is None:
        logger.warning("[realtime] 그리드 수집 실패")
        raise RuntimeError("제주 실시간 SMP 원천 데이터가 비어 있습니다")
    df = parse_realtime_grid(grid, ref=date.today())
    if df.empty:
        logger.info("[realtime] 확정된 신규 데이터 없음")
        return 0
    n = C.upsert_realtime_jeju(df, db_url=db_url)
    logger.info(
        f"[realtime] 완료: {n}행 ({df['timestamp'].min()} ~ {df['timestamp'].max()})"
    )
    return n


# 제주 실시간시장 시범사업 개시일(데이터 하한)
REALTIME_FLOOR = date(2024, 3, 1)


def run_realtime_backfill(
    start: Optional[date] = None,
    end: Optional[date] = None,
    db_url: Optional[str] = None,
) -> int:
    """제주 실시간 15분 SMP 과거 백필.

    KPX 실시간 페이지는 gubun=day&issue_date 로 그 issue_date가 포함된
    '연간(시범사업 개시~)' 전체 표를 한 번에 준다(확인됨: 한 호출에 822일).
    따라서 start의 issue_date 한 번 호출로 대부분 커버되지만, 안전하게
    start~end 를 1년 간격으로 순회하며 확정 96구간만 upsert한다.
    각 호출의 ref는 해당 issue_date로 넘겨 MM.DD->연도 매핑을 정확히 한다.

    기본: 2024-03-01(시범사업 개시) ~ 어제.
    """
    start = start or REALTIME_FLOOR
    end = end or (date.today() - timedelta(days=1))
    session = make_session()
    engine = C.get_engine_for(db_url)

    issue = start
    total = 0
    while issue <= end:
        grid = fetch_grid(
            session,
            SMPAPI.REALTIME_JEJU_URL,
            SMPAPI.REALTIME_JEJU_MID,
            extra_params={
                "device": "pc",
                "division": "smpDataRt",
                "gubun": "day",
                "issue_date": issue.strftime("%Y-%m-%d"),
            },
        )
        if grid is not None:
            df = parse_realtime_grid(grid, ref=issue)
            if not df.empty:
                # end 이후 날짜는 제외(미래/미확정 방지)
                df = df[df["timestamp"].dt.date <= end]
                if not df.empty:
                    n = C.upsert_realtime_jeju(df, engine=engine)
                    total += n
                    logger.info(
                        f"[realtime-backfill] issue={issue} -> {n}행 "
                        f"({df['timestamp'].min()}~{df['timestamp'].max()})"
                    )
        issue = date(issue.year + 1, issue.month, issue.day)

    logger.info(f"[realtime-backfill] 완료: 총 {total}행 ({start}~{end})")
    return total


def main() -> None:
    parser = argparse.ArgumentParser(description="제주 실시간 15분 SMP 수집")
    parser.add_argument("--backfill", action="store_true", help="과거 전체 백필(2024-03-01~어제)")
    parser.add_argument("--start", default=None, help="백필 시작일 YYYY-MM-DD")
    parser.add_argument("--end", default=None, help="백필 종료일 YYYY-MM-DD")
    parser.add_argument("--db-url", default=None)
    args = parser.parse_args()
    if args.backfill or args.start or args.end:
        from datetime import datetime as _dt
        start = _dt.strptime(args.start, "%Y-%m-%d").date() if args.start else None
        end = _dt.strptime(args.end, "%Y-%m-%d").date() if args.end else None
        run_realtime_backfill(start=start, end=end, db_url=args.db_url)
    else:
        run_realtime_collection(args.db_url)


if __name__ == "__main__":
    main()
