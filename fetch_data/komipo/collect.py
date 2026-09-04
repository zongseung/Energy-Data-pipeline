"""KOMIPO 신재생에너지 발전 현황 원본 수집 (적재 없음).

엔드포인트: https://apis.data.go.kr/B552521/renewEnergy/getData
  - DAILY 1회 호출 = **2일치**(조회일 + 다음날), 설비당 시간당 1건
  - dataTerm(MONTH/3MONTH/YEAR)은 서버가 무시한다 — 뭘 넣어도 같은 2일치
  - 개발계정 1,000회/일 제한 → --budget 으로 끊어 받고 다음 실행에서 이어받는다

daypower 의 단위가 확정되지 않아(원본 문서에 없음, 실측값이 설비용량을 초과)
generation 코어에는 넣지 않는다. 파싱한 long 데이터를
komipo_data_raw/{station}_{year}.parquet 로만 쌓는다.

사용:
    uv run python -m fetch_data.komipo.collect --budget 800
    uv run python -m fetch_data.komipo.collect --start 20260101 --end 20260831
"""
from __future__ import annotations

import argparse
import os
import time
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, List, Set, Tuple

import pandas as pd

from fetch_data.common.logger import get_logger

logger = get_logger(__name__)

BASE = "https://apis.data.go.kr/B552521/renewEnergy/getData"
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_OUT = PROJECT_ROOT / "komipo_data_raw"

# 발전본부 코드 (가이드 '라. 발전본부 코드표').
# 서울건설(8410)·서천건설(8570)은 2026-09 실측에서 전 기간 0행이라 기본 제외한다.
STATIONS: Dict[str, str] = {
    "8509": "보령", "8420": "인천", "8710": "제주", "9180": "신보령", "9830": "세종",
}
EMPTY_STATIONS = {"8410": "서울건설", "8570": "서천건설"}

# 실측 하한: 20181116 있음 / 20180101·20170101 없음
EARLIEST = date(2018, 11, 1)


def _key() -> str:
    key = os.getenv("KOMIPO_API_KEY", "")
    if not key:
        raise RuntimeError("KOMIPO_API_KEY가 설정되어 있지 않습니다.")
    return key


def fetch_day(station: str, day: date, timeout: int = 60) -> List[dict]:
    """한 본부의 하루를 요청한다. 응답은 2일치(조회일+다음날)."""
    q = urllib.parse.urlencode({
        "ServiceKey": _key(), "numOfRows": 2000, "pageNo": 1,
        "stationName": station, "dataDate": day.strftime("%Y%m%d"), "dataTerm": "DAILY",
    })
    body = urllib.request.urlopen(f"{BASE}?{q}", timeout=timeout).read()
    root = ET.fromstring(body)
    return [{c.tag: (c.text or "").strip() for c in item} for item in root.iter("item")]


def _parquet_path(out_dir: Path, station: str, year: int) -> Path:
    return out_dir / f"{station}_{year}.parquet"


def load_existing(out_dir: Path, station: str) -> Tuple[pd.DataFrame, Set[date]]:
    """이미 받아둔 데이터와 그 날짜 집합(재개용)."""
    files = sorted(out_dir.glob(f"{station}_*.parquet"))
    if not files:
        return pd.DataFrame(columns=["siteterm", "unitterm", "timestamp", "daypower"]), set()
    df = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)
    return df, set(pd.to_datetime(df["timestamp"]).dt.date.unique())


def to_long(rows: List[dict]) -> pd.DataFrame:
    df = pd.DataFrame(rows)
    if df.empty:
        return pd.DataFrame(columns=["siteterm", "unitterm", "timestamp", "daypower"])
    df["timestamp"] = pd.to_datetime(df["gathdtm"], errors="coerce").dt.floor("h")
    df["daypower"] = pd.to_numeric(df["daypower"], errors="coerce")
    return df[["siteterm", "unitterm", "timestamp", "daypower"]].dropna(subset=["timestamp"])


def run(start: date, end: date, budget: int, out_dir: Path = DEFAULT_OUT) -> int:
    """[start, end] 를 본부별로 훑는다. 호출 예산 소진 시 중단(다음 실행이 이어받음)."""
    out_dir.mkdir(parents=True, exist_ok=True)
    calls = 0

    for station, name in STATIONS.items():
        old, have = load_existing(out_dir, station)
        new: List[pd.DataFrame] = []
        day = start
        while day <= end:
            if calls >= budget:
                break
            # 한 번 부르면 day, day+1 이 온다 → 둘 다 있으면 건너뛴다
            if day in have and (day + timedelta(days=1)) in have:
                day += timedelta(days=2)
                continue
            try:
                rows = fetch_day(station, day)
            except Exception as e:
                logger.warning(f"[{name}] {day} 호출 실패: {e}")
                calls += 1
                day += timedelta(days=2)
                continue
            calls += 1
            long = to_long(rows)
            if not long.empty:
                new.append(long)
            day += timedelta(days=2)
            time.sleep(0.2)

        if new:
            frames = ([old] if not old.empty else []) + new
            merged = pd.concat(frames, ignore_index=True).drop_duplicates(
                subset=["unitterm", "timestamp"], keep="last"
            )
            for year, g in merged.groupby(pd.to_datetime(merged["timestamp"]).dt.year):
                _parquet_path(out_dir, station, int(year)).parent.mkdir(parents=True, exist_ok=True)
                g.to_parquet(_parquet_path(out_dir, station, int(year)), index=False)
            logger.info(f"[{name}] 누적 {len(merged):,}행 저장 (이번에 {sum(len(d) for d in new):,}행)")
        if calls >= budget:
            logger.info(f"호출 예산 {budget} 소진 — {name}({station}) {day} 에서 중단")
            break

    logger.info(f"수집 종료 — 호출 {calls}회")
    return calls


def _to_date(s: str) -> date:
    return datetime.strptime(s, "%Y%m%d").date()


def main() -> None:
    p = argparse.ArgumentParser(description="KOMIPO 신재생 발전 원본 수집 (적재 없음)")
    p.add_argument("--start", default=None, help="시작일 YYYYMMDD (기본 2018-11-01)")
    p.add_argument("--end", default=None, help="종료일 YYYYMMDD (기본 어제)")
    p.add_argument("--budget", type=int, default=800, help="이번 실행의 최대 호출 수 (기본 800)")
    p.add_argument("--out-dir", default=str(DEFAULT_OUT))
    a = p.parse_args()
    run(
        start=_to_date(a.start) if a.start else EARLIEST,
        end=_to_date(a.end) if a.end else date.today() - timedelta(days=1),
        budget=a.budget,
        out_dir=Path(a.out_dir),
    )


if __name__ == "__main__":
    main()
