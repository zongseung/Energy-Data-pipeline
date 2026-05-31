"""
하루전시장 시간별 SMP 수집 (육지/제주) + 일별 가중평균 동시 적재.

KPX 일별 페이지(smpInland.es / smpJeju.es)는 한 번의 조회로 최근 7일치
(27행 = 1~24시 + 최대/최소/가중평균 × 7일 열)를 돌려준다.
  - 1~24시  -> smp_hourly  (라벨 N -> (N-1)시 구간시작 변환)
  - 최대/최소/가중평균 -> smp_weighted_avg(period_type=daily)

증분: 공통 DB smp_hourly의 region별 max(timestamp) 다음날부터 어제까지.
한 번의 페이지 조회로 최근 7일이 들어오므로, 보통은 그 범위로 충분하다.
더 과거가 필요하면 issue_date를 과거로 바꿔 여러 번 조회한다(외부 레포 방식).

과거 전체(2001~)는 smp_backfill(master CSV)로 채운다.

사용 예:
    uv run python -m fetch_data.smp.smp_collect             # 증분(최근 며칠)
    uv run python -m fetch_data.smp.smp_collect --days 7    # 최근 7일 강제 수집
    uv run python -m fetch_data.smp.smp_collect --region jeju
"""

from __future__ import annotations

import argparse
from datetime import date, datetime, timedelta
from typing import List, Optional

import pandas as pd

from fetch_data.common.logger import get_logger
from fetch_data.constants import SMPAPI
from fetch_data.smp import _common as C
from fetch_data.smp.smp_scraper import fetch_grid, make_session

logger = get_logger(__name__)

REGIONS = ("land", "jeju")
HOURS_PER_DAY = 24


def _date_columns_from_grid(grid: List[List[str]], issue_dt: date) -> List[date]:
    """헤더(r0)의 MM.DD 열을 issue_date 기준으로 실제 date로 환산.

    KPX 헤더는 MM.DD만 주고 가장 오른쪽 열이 issue_date(7일 창)다.
    연 경계(예: 12.28~01.03)도 위치 역산으로 안전하게 처리한다.
    """
    header = grid[0]
    # 첫 칸('구분') 제외, 나머지가 날짜 열
    n_dates = len(header) - 1
    # 오른쪽 끝 = issue_dt, 왼쪽으로 하루씩 감소
    return [issue_dt - timedelta(days=(n_dates - 1 - j)) for j in range(n_dates)]


def _parse_daily_grid(grid: List[List[str]], region: str, issue_dt: date):
    """일별 그리드를 (hourly_rows, wavg_rows)로 파싱.

    Returns:
        hourly_rows: list[dict(timestamp, region, price)]
        wavg_rows:   list[dict(period_type, period, region, weighted_avg, smp_max, smp_min)]
    """
    if not grid or len(grid) < 2:
        return [], []

    dates = _date_columns_from_grid(grid, issue_dt)
    n_dates = len(dates)

    # 라벨 -> 행 인덱스
    label_row = {}
    for row in grid[1:]:
        if not row:
            continue
        label_row[row[0]] = row

    hourly_rows = []
    # 1h ~ 24h
    for hour_label in range(1, HOURS_PER_DAY + 1):
        row = label_row.get(f"{hour_label}h")
        if row is None:
            continue
        values = row[1 : 1 + n_dates]
        for j, d in enumerate(dates):
            price = C.parse_price(values[j]) if j < len(values) else None
            if price is None:
                continue
            # hour-ending -> 구간시작: 라벨 N -> (N-1)시
            ts = datetime(d.year, d.month, d.day) + timedelta(hours=hour_label - 1)
            hourly_rows.append({"timestamp": ts, "region": region, "price": price})

    # 최대/최소/가중평균 -> 일별 가중평균 테이블
    # 27행 중 '가중평균'만 일별 공식 가중평균(price_type='smp')으로 적재.
    # 최대/최소는 스키마에서 제외(필요 시 smp_hourly에서 SQL 집계).
    wavg_row = label_row.get("가중평균")
    wavg_rows = []
    if wavg_row is not None:
        values = wavg_row[1 : 1 + n_dates]
        for j, d in enumerate(dates):
            wa = C.parse_price(values[j]) if j < len(values) else None
            if wa is None:
                continue
            wavg_rows.append(
                {
                    "period_type": "daily",
                    "period": d,
                    "region": region,
                    "price_type": "smp",
                    "weighted_avg": wa,
                }
            )
    return hourly_rows, wavg_rows


def collect_region(
    session,
    region: str,
    issue_dt: date,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """한 region의 일별 페이지를 조회해 (hourly_df, wavg_df) 반환."""
    url, mid = SMPAPI.DAILY[region]
    grid = fetch_grid(
        session,
        url,
        mid,
        post=True,
        post_data={"issue_date": issue_dt.strftime("%Y-%m-%d"), "yy": issue_dt.strftime("%Y")},
        extra_params={"gubun": "year"},
    )
    if grid is None:
        logger.warning(f"[SMP][{region}] 그리드 수집 실패 (issue={issue_dt})")
        return pd.DataFrame(), pd.DataFrame()
    hourly_rows, wavg_rows = _parse_daily_grid(grid, region, issue_dt)
    return pd.DataFrame(hourly_rows), pd.DataFrame(wavg_rows)


def run_smp_collection(
    regions: Optional[List[str]] = None,
    days: Optional[int] = None,
    db_url: Optional[str] = None,
) -> int:
    """시간별 SMP + 일별 가중평균 수집/적재.

    Args:
        regions: 대상 지역(기본 land, jeju).
        days: 지정 시 issue_date를 days 간격으로 과거까지 여러 번 조회.
              미지정 시 어제 issue_date 1회(최근 7일 창)만 조회하되,
              DB max+1보다 과거가 비면 추가 조회.
    Returns:
        smp_hourly 적재 행수.
    """
    regions = regions or list(REGIONS)
    session = make_session()
    engine = C.get_engine_for(db_url)

    yesterday = date.today() - timedelta(days=1)
    total_hourly = 0

    for region in regions:
        # 조회할 issue_date 목록 결정
        issue_dates: List[date] = []
        if days is not None and days > 0:
            # KPX 일별 페이지는 issue_date 기준 7일창을 준다.
            # 점프 간격을 6일로 두어 창끼리 1일씩 겹치게 해 경계 누락(갭)을 방지.
            d = yesterday
            covered = 0
            while covered < days:
                issue_dates.append(d)
                d = d - timedelta(days=6)
                covered += 6
        else:
            issue_dates = [yesterday]

        region_hourly = []
        region_wavg = []
        for issue_dt in issue_dates:
            h_df, w_df = collect_region(session, region, issue_dt)
            if not h_df.empty:
                region_hourly.append(h_df)
            if not w_df.empty:
                region_wavg.append(w_df)

        if region_hourly:
            h_all = pd.concat(region_hourly, ignore_index=True).drop_duplicates(
                subset=["timestamp", "region"]
            )
            n = C.upsert_hourly(h_all, engine=engine)
            total_hourly += n
            logger.info(f"[SMP][{region}] 시간별 {n}행 적재")
        if region_wavg:
            w_all = pd.concat(region_wavg, ignore_index=True).drop_duplicates(
                subset=["period_type", "period", "region", "price_type"]
            )
            C.upsert_weighted_avg(w_all, engine=engine)

    logger.info(f"[SMP] 시간별 수집 완료: 총 {total_hourly}행")
    return total_hourly


def main() -> None:
    parser = argparse.ArgumentParser(description="하루전시장 시간별 SMP 수집")
    parser.add_argument("--region", choices=list(REGIONS), default=None, help="대상 지역(기본 전체)")
    parser.add_argument("--days", type=int, default=None, help="최근 N일 강제 수집(기본: 어제 1회=최근 7일창)")
    parser.add_argument("--db-url", default=None, help="DB URL 직접 지정")
    args = parser.parse_args()

    regions = [args.region] if args.region else None
    run_smp_collection(regions=regions, days=args.days, db_url=args.db_url)


if __name__ == "__main__":
    main()
