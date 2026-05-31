"""
월별/연도별 SMP·BLMP 가중평균 수집 (EPSIS 공식 API).

출처: EPSIS 가중평균 SMP (selectEkmaSmpSmp.ajax). KPX 공식값과 100% 일치 검증.
응답은 JS 코드 문자열이며 각 기간(연 또는 월)마다 c1~c4 + year/month가 들어있다:
  c1 = 통합 SMP, c2 = 육지 SMP, c3 = 제주 SMP, c4 = BLMP(육지)
적재: smp_weighted_avg (period_type, period, region, price_type, weighted_avg)
  - 통합/제주 SMP : 2010~ 존재 (이전 0)
  - BLMP         : 2001~2006 존재 (이후 0)
  - 0 값은 '데이터 없음'으로 skip
selYear: 연='Y'(beginDate/endDate=YYYY), 월='N'(=YYYYMM).

일별(daily) 가중평균은 이 API로 제공되지 않는다(연/월만). 일별은
smp_collect(KPX 27열의 가중평균) 또는 smp_backfill(EPSIS 시간별 CSV)로 채운다.

사용 예:
    uv run python -m fetch_data.smp.smp_aggregate                       # 전체(연+월)
    uv run python -m fetch_data.smp.smp_aggregate --period yearly
    uv run python -m fetch_data.smp.smp_aggregate --begin 2001 --end 2026 --period yearly
"""

from __future__ import annotations

import argparse
import re
from datetime import date
from typing import List, Optional, Tuple

import pandas as pd
import requests

from fetch_data.common.logger import get_logger
from fetch_data.constants import SMPAPI
from fetch_data.smp import _common as C

logger = get_logger(__name__)

# c1=통합SMP, c2=육지SMP, c3=제주SMP, c4=BLMP(육지) 를 추출하는 정규식
_ROW_RE = re.compile(
    r'c1 = textFormmat\("([^"]*)".*?'
    r'c2 = textFormmat\("([^"]*)".*?'
    r'c3 = textFormmat\("([^"]*)".*?'
    r'c4 = textFormmat\("([^"]*)".*?'
    r'"year":"(\d+)",\s*"month":"(\d+)"',
    re.S,
)


def _make_epsis_session() -> requests.Session:
    """EPSIS 세션(페이지 GET으로 쿠키 확보)."""
    s = requests.Session()
    s.headers.update(
        {
            "User-Agent": SMPAPI.USER_AGENT,
            "X-Requested-With": "XMLHttpRequest",
            "Referer": SMPAPI.EPSIS_PAGE,
        }
    )
    s.get(SMPAPI.EPSIS_PAGE, timeout=SMPAPI.TIMEOUT_SEC)
    return s


def _fetch_rows(session, begin: str, end: str, sel_year: str) -> List[Tuple]:
    """EPSIS ajax 호출 -> [(통합, 육지, 제주, blmp, year, month)] 문자열 튜플."""
    r = session.post(
        SMPAPI.EPSIS_AJAX,
        data={"beginDate": begin, "endDate": end, "selYear": sel_year},
        timeout=SMPAPI.TIMEOUT_SEC,
    )
    return _ROW_RE.findall(r.text)


def _rows_to_records(raw_rows: List[Tuple], period_type: str) -> List[dict]:
    """원시 튜플 -> smp_weighted_avg 레코드. 0/빈값은 skip.

    EPSIS_COLS 순서(통합SMP, 육지SMP, 제주SMP, BLMP육지)대로 매핑.
    """
    records = []
    for total, land, jeju, blmp, yr, mo in raw_rows:
        year = int(yr)
        month = int(mo)
        if period_type == "yearly":
            period = date(year, 1, 1)
        else:  # monthly, month 99는 연 데이터이므로 제외
            if month < 1 or month > 12:
                continue
            period = date(year, month, 1)
        for raw_val, (region, price_type) in zip(
            (total, land, jeju, blmp), SMPAPI.EPSIS_COLS
        ):
            val = C.parse_price(raw_val)
            if val is None or val == 0.0:  # 0 = 데이터 없음
                continue
            records.append(
                {
                    "period_type": period_type,
                    "period": period,
                    "region": region,
                    "price_type": price_type,
                    "weighted_avg": val,
                }
            )
    return records


def collect_yearly(session, begin_year: int, end_year: int, db_url=None, engine=None) -> int:
    raw = _fetch_rows(session, str(begin_year), str(end_year), "Y")
    recs = _rows_to_records(raw, "yearly")
    df = pd.DataFrame(recs).drop_duplicates(
        subset=["period_type", "period", "region", "price_type"]
    )
    n = C.upsert_weighted_avg(df, db_url=db_url, engine=engine)
    logger.info(f"[aggregate] 연도별 {n}행 적재 ({begin_year}~{end_year})")
    return n


def collect_monthly(session, begin_ym: str, end_ym: str, db_url=None, engine=None) -> int:
    raw = _fetch_rows(session, begin_ym, end_ym, "N")
    recs = _rows_to_records(raw, "monthly")
    df = pd.DataFrame(recs).drop_duplicates(
        subset=["period_type", "period", "region", "price_type"]
    )
    n = C.upsert_weighted_avg(df, db_url=db_url, engine=engine)
    logger.info(f"[aggregate] 월별 {n}행 적재 ({begin_ym}~{end_ym})")
    return n


def run_aggregate(
    period: str = "all",
    begin: Optional[str] = None,
    end: Optional[str] = None,
    db_url: Optional[str] = None,
) -> int:
    """EPSIS에서 월/연 SMP·BLMP 가중평균 수집.

    period: monthly | yearly | all
    begin/end: 연도(YYYY) 또는 연월(YYYYMM). 미지정 시 전체 가용범위(2001~현재).
    """
    engine = C.get_engine_for(db_url)
    session = _make_epsis_session()
    this_year = date.today().year
    total = 0

    if period in ("yearly", "all"):
        by = int(begin[:4]) if begin else 2001
        ey = int(end[:4]) if end else this_year
        total += collect_yearly(session, by, ey, engine=engine)

    if period in ("monthly", "all"):
        bym = begin if (begin and len(begin) >= 6) else "200101"
        eym = end if (end and len(end) >= 6) else f"{this_year}12"
        total += collect_monthly(session, bym[:6], eym[:6], engine=engine)

    logger.info(f"[aggregate] 완료: 총 {total}행")
    return total


def main() -> None:
    parser = argparse.ArgumentParser(description="EPSIS 월/연 SMP·BLMP 가중평균 수집")
    parser.add_argument("--period", choices=["monthly", "yearly", "all"], default="all")
    parser.add_argument("--begin", default=None, help="YYYY 또는 YYYYMM")
    parser.add_argument("--end", default=None, help="YYYY 또는 YYYYMM")
    parser.add_argument("--db-url", default=None)
    args = parser.parse_args()
    run_aggregate(period=args.period, begin=args.begin, end=args.end, db_url=args.db_url)


if __name__ == "__main__":
    main()
