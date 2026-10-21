"""남부 PwrGenTran (시간대별 발전량및송전량) 원본 수집 — 적재 없음.

`data/nambu/pwrgentran/{year}.parquet` 로만 쌓는다. generation 코어 적재는 아직 안 한다:
  · 송전량(`qhortrn01~24`)을 둘 자리가 generation 에 없다 — 별도 테이블로 분리 예정.
  · `ins`(연료구분)를 fuel_type 으로 쓸 수 없다 — 소수력·연료전지·태양광 일부가 '복합' 으로 온다.
    coal/oil/combined 를 추가하면서 ipptnm/hogigb 로 교정해야 한다.

## 왜 받는가 (2026-10-21 샘플 6일로 판단)

71계열 중 **석탄 8 · 중유 2 · 복합 48 = 58계열이 우리 DB 에 아예 없다.**
남부는 지금 태양광 35 · 풍력 4 뿐이다. 송전량도 이 API 에만 있다.

시간별 값이 일 집계와 소수점까지 맞는다 — 하동화력 1호기 2026-08-03:
`qhorgen01~24` 합 10,305,192 == `qvodgen`, 송전 합 9,766,552 == `qvodtrn`.
시간당 최대 508MW(정격 500MW)로 스케일도 맞다. 송전/발전 비는
석탄 0.9508 · 복합 0.9495 · 중유 0.9265 · **태양광 1.0000**(소내소비 없음).

## 유일키는 (ipptnm, hogigb, hogi) 3개 조합이다

`ipptnm` 발전소 → `hogigb` 호기구분(CC 축) → `hogi` 터빈. 복합은 한 축에 GT 2기 + ST 1기가 묶인다.
`(ipptnm, hogi)` 만으로는 69/71 로 부족하다.

## 함정

- 제공 시작일 **2015-10-23** (이진탐색 확정). 그 이전은 응답 0건.
- **원천 공백이 있다** — 2021-03-20 이 0건이다. PwrSunLightInfo 의 공백 구간
  (2020-12-29~2021-03-29)과 겹친다. 같은 사내 시스템이라 그렇고, 파일데이터로 메운 그 구간이다.
  → 0건을 실패로 세지 않는다.
- 계열 수가 과거로 갈수록 적다 (2015: 47 → 2026: 71).
- `strSdate`/`strEdate` 는 **YYYYMMDD**. YYYYMM 은 0건이 온다.
- 키는 `NAMBU_PWRGENTRAN_KEY` (`NAMBU_API_KEY` 와 다르다).
- 일일트래픽 10,000. 하루 1콜로 71계열 전부 오므로 11년치가 ~4,000콜이다.

사용:
    uv run python -m pipeline.fetch_data.nambu.pwrgentran_collect                    # 전 기간 이어받기
    uv run python -m pipeline.fetch_data.nambu.pwrgentran_collect --start 20200101 --end 20201231
"""
from __future__ import annotations

import argparse
import os
import time
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from datetime import date, datetime, timedelta

import pandas as pd

from pipeline.fetch_data.common import config  # noqa: F401  (.env 로딩)
from pipeline.fetch_data.common.logger import get_logger
from pipeline.fetch_data.common.paths import DATA_DIR

logger = get_logger(__name__)

BASE = "https://apis.data.go.kr/B552520/PwrGenTran/getDataService"
OUT_DIR = DATA_DIR / "nambu" / "pwrgentran"
EARLIEST = date(2015, 10, 23)      # 이진탐색으로 확정한 제공 시작일
MAX_CONSECUTIVE_FAILS = 10


def _key() -> str:
    k = os.getenv("NAMBU_PWRGENTRAN_KEY", "")
    if not k:
        raise RuntimeError("NAMBU_PWRGENTRAN_KEY 가 설정되어 있지 않다")
    return k


def fetch_day(d: date, timeout: int = 60) -> list[dict]:
    """하루치 전 계열. 0건은 원천 공백이므로 실패가 아니다."""
    p = urllib.parse.urlencode({
        "serviceKey": _key(), "pageNo": 1, "numOfRows": 300,
        "strSdate": d.strftime("%Y%m%d"), "strEdate": d.strftime("%Y%m%d"),
    })
    with urllib.request.urlopen(f"{BASE}?{p}", timeout=timeout) as r:
        x = ET.fromstring(r.read())
    return [{c.tag: c.text for c in i} for i in x.findall(".//item")]


def _path(year: int):
    return OUT_DIR / f"{year}.parquet"


def load_existing(year: int) -> tuple[pd.DataFrame, set]:
    p = _path(year)
    if not p.exists():
        return pd.DataFrame(), set()
    d = pd.read_parquet(p)
    return d, set(pd.to_datetime(d["ymd"]).dt.date)


def run(start: date, end: date, budget: int = 9000) -> int:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    calls = fails = 0

    for year in range(start.year, end.year + 1):
        old, have = load_existing(year)
        lo = max(start, date(year, 1, 1))
        hi = min(end, date(year, 12, 31))
        new: list[dict] = []
        day = lo
        while day <= hi:
            if calls >= budget or fails >= MAX_CONSECUTIVE_FAILS:
                break
            if day in have:
                day += timedelta(days=1)
                continue
            try:
                rows = fetch_day(day)
                calls += 1
                fails = 0
                new += rows                      # 0건이면 그대로 비워 둔다(원천 공백)
            except Exception as e:
                logger.warning(f"{day} 호출 실패: {e}")
                calls += 1
                fails += 1
            day += timedelta(days=1)
            time.sleep(0.1)

        if new:
            frames = ([old] if not old.empty else []) + [pd.DataFrame(new)]
            merged = pd.concat(frames, ignore_index=True).drop_duplicates(
                subset=["ymd", "ipptnm", "hogigb", "hogi"], keep="last")
            merged.to_parquet(_path(year), index=False)
            logger.info(f"[{year}] 누적 {len(merged):,}행 (이번에 {len(new):,}행)")
        if fails >= MAX_CONSECUTIVE_FAILS:
            logger.error(f"연속 {fails}회 실패 — {day} 에서 중단")
            break
        if calls >= budget:
            logger.info(f"예산 {budget} 소진 — {year}년 {day} 에서 중단")
            break

    logger.info(f"수집 종료 — 호출 {calls}회")
    return calls


def _to_date(s: str) -> date:
    return datetime.strptime(s, "%Y%m%d").date()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default=None)
    ap.add_argument("--end", default=None)
    ap.add_argument("--budget", type=int, default=9000)
    a = ap.parse_args()
    run(_to_date(a.start) if a.start else EARLIEST,
        _to_date(a.end) if a.end else date.today() - timedelta(days=1),
        a.budget)
