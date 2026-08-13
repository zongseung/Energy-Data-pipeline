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
_UNCONFIRMED_MARKER = "확정가격은D+1일18시까지공표예정입니다."
_UNCONFIRMED = object()
_EMPTY = object()
SLOTS = SMPAPI.REALTIME_SLOTS_PER_DAY  # 96


def _header_dates(header: List[str], ref: date) -> List[Optional[date]]:
    """헤더 각 셀의 MM.DD를 실제 date로 변환. MM.DD가 아닌 셀은 None.

    날짜 열은 좌->우로 시간순이지만 **연속된 매일이 아니다.** KPX 는 열을
    통째로 건너뛰고 준다 (2026-08-13 실측: '08.07(금), 08.10(월), 08.11(화)'
    — 주말 두 칸이 없다).

    예전 구현은 첫 열에서 +1일씩 증가시키며 MM.DD 가 어긋나면 예외를 던졌다.
    주말이 낀 창이 오면 수집 전체가 죽었고, 실제로 2026-08-07 부터 매일 실패했다.

    연도는 셀마다 ref 로 추정할 수 없다 — 창이 1년을 넘기 때문이다
    (실측 852열 = 2024-03-01 ~ 2026-08-11). 그래서 **MM.DD 가 뒤로 감기는
    지점에서 연도를 +1** 하고, 마지막 열을 ref 에 맞춰 기준연도를 역산한다.
    이러면 결번과 연도 경계를 동시에 견딘다.
    """
    parsed: List[tuple[int, tuple[int, int]]] = []
    for i, c in enumerate(header):
        m = _MMDD_RE.match(c)
        if m:
            parsed.append((i, (int(m.group(1)), int(m.group(2)))))
    if not parsed:
        return [None] * len(header)

    # 좌->우로 훑으며 롤오버 누적 횟수를 기록한다.
    offsets: List[int] = []
    rollovers = 0
    prev: Optional[tuple[int, int]] = None
    for _, md in parsed:
        if prev is not None and md < prev:
            rollovers += 1
        offsets.append(rollovers)
        prev = md

    # 마지막(가장 최근) 열의 연도를 ref 기준으로 확정한 뒤 기준연도를 역산한다.
    last_md = parsed[-1][1]
    last_year = ref.year
    try:
        if date(last_year, *last_md) > ref:
            last_year -= 1
    except ValueError:
        pass  # 2/29 같은 경우 — 아래 개별 변환에서 걸러진다
    base_year = last_year - rollovers

    out: List[Optional[date]] = [None] * len(header)
    resolved: List[date] = []
    for (idx, (mm, dd)), off in zip(parsed, offsets):
        try:
            d = date(base_year + off, mm, dd)
        except ValueError:
            continue  # 존재하지 않는 날짜(2/29 등)는 그 열만 버린다
        out[idx] = d
        resolved.append(d)

    # 결번은 허용하되 **순서·중복·과대점프는 막는다.** 연속성 가정을 걷어내면서
    # 이 방어까지 잃으면 두 가지 사고가 난다.
    #   (1) 같은 날짜 열이 두 번 오면 그 날 96슬롯이 중복 적재된다.
    #   (2) 'MM.DD 가 뒤로 감기면 +1년' 규칙이 오작동한다. 예컨대 08.05 뒤에
    #       08.04 가 오면 진짜 롤오버가 아니라 원천이 깨진 것인데, 그대로 두면
    #       365일을 통째로 건너뛴 날짜가 만들어진다.
    # 진짜 연도 경계(12.31 -> 01.01)는 간격이 1일이므로, 상한을 넉넉히 둬도
    # 정상 데이터는 걸리지 않는다(휴일·점검으로 며칠씩 비는 것은 정상).
    MAX_GAP_DAYS = 60
    for a, b in zip(resolved, resolved[1:]):
        if b <= a or (b - a).days > MAX_GAP_DAYS:
            raise RuntimeError("제주 실시간 SMP 원천 데이터 형식이 올바르지 않습니다")
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
            raw = values[j] if j < len(values) else None
            price = C.parse_price(raw)
            if price is not None:
                by_date[d][slot_idx] = price
            else:
                normalized = "" if raw is None else re.sub(r"\s+", "", str(raw))
                if normalized == _UNCONFIRMED_MARKER:
                    by_date[d][slot_idx] = _UNCONFIRMED
                elif raw is not None and normalized == "":
                    by_date[d][slot_idx] = _EMPTY
                else:
                    raise RuntimeError("제주 실시간 SMP 원천 데이터 형식이 올바르지 않습니다")

    records = []
    confirmed_dates = []
    for d in date_list:
        prices = by_date[d]
        if any(price is _UNCONFIRMED for price in prices) and all(
            price is _UNCONFIRMED or price is _EMPTY for price in prices
        ):
            continue
        if any(price is _UNCONFIRMED or price is _EMPTY or price is None for price in prices):
            raise RuntimeError("제주 실시간 SMP 원천 데이터 형식이 올바르지 않습니다")
        confirmed_dates.append(d)
        day_start = datetime(d.year, d.month, d.day)
        for slot_idx, price in enumerate(prices):
            ts = day_start + timedelta(minutes=15 * slot_idx)
            records.append(
                {"timestamp": ts, "region": "jeju", "price": price, "is_confirmed": True}
            )

    df = pd.DataFrame(records)
    # 원천 진단을 결과에 실어 보낸다. 0행일 때 "우리 파서가 깨졌나 / KPX 가
    # 확정을 멈췄나" 를 로그만 보고 구분할 수 있어야 한다 — 이 구분이 없어서
    # 2026-06~08 에 72회 연속 0행 수집이 '성공'으로 보고됐다.
    df.attrs["source_first"] = date_list[0]
    df.attrs["source_last"] = date_list[-1]
    df.attrs["confirmed_last"] = confirmed_dates[-1] if confirmed_dates else None
    df.attrs["unconfirmed_days"] = len(date_list) - len(confirmed_dates)
    return df


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
        # 0행은 정상일 수도(아직 D+1 18시 전) 비정상일 수도(원천이 확정을 멈춤) 있다.
        # 어느 쪽인지 로그만 보고 판별되게 원천 상태를 함께 남긴다.
        last_conf = df.attrs.get("confirmed_last")
        stale = (date.today() - last_conf).days if last_conf else None
        logger.warning(
            "[realtime] 확정된 신규 데이터 없음 — 원천 게시 %s~%s, "
            "마지막 확정 %s(%s일 전), 미확정 %s일치. "
            "며칠 이상 지속되면 KPX 가 확정 공표를 중단한 것이다(우리 쪽 버그 아님).",
            df.attrs.get("source_first"), df.attrs.get("source_last"),
            last_conf or "없음", stale if stale is not None else "?",
            df.attrs.get("unconfirmed_days"),
        )
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
        if grid is None:
            raise RuntimeError("제주 실시간 SMP 원천 데이터가 비어 있습니다")
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
