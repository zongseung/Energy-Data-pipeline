"""KOMIPO 신재생에너지 발전 현황 원본 수집 (적재 없음).

엔드포인트: https://apis.data.go.kr/B552521/renewEnergy/getData
  - DAILY 1회 호출 = **2일치**(조회일 + 다음날), 설비당 시간당 1건
  - dataTerm(MONTH/3MONTH/YEAR)은 서버가 무시한다 — 뭘 넣어도 같은 2일치
  - 개발계정 1,000회/일 제한 → --budget 으로 끊어 받고 다음 실행에서 이어받는다

파싱한 long 데이터를 `data/komipo/{station}_{year}.parquet` 로 쌓는다.
generation 코어 적재는 `load.py` 가 맡는다 (전처리 규칙을 거친다).

**재개 커서는 DB + parquet 을 합쳐서 본다** (2026-10-21):
적재 전에는 parquet 이 유일한 상태 기록이었는데, 지금은 `operator='jungbu'` 로
DB 에 들어가 있다. DB 를 먼저 보므로 **parquet 이 사라져도 재수집하지 않는다**.
parquet 은 전처리 전 원본으로 남겨 둔다 — DB 에는 스파이크·야간값이 NULL 로 들어가서
교정 규칙을 바꿀 때 원본이 필요하다.

사용:
    uv run python -m pipeline.fetch_data.komipo.collect --budget 800
    uv run python -m pipeline.fetch_data.komipo.collect --start 20260101 --end 20260831
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

from pipeline.fetch_data.common import config  # noqa: F401  (.env 로딩)
from pipeline.fetch_data.common.paths import DATA_DIR
from pipeline.fetch_data.common.logger import get_logger

logger = get_logger(__name__)

BASE = "https://apis.data.go.kr/B552521/renewEnergy/getData"
PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_OUT = DATA_DIR / "komipo"

# 발전본부 코드 (가이드 '라. 발전본부 코드표').
# 서울건설(8410)·서천건설(8570)은 2026-09 실측에서 전 기간 0행이라 기본 제외한다.
STATIONS: Dict[str, str] = {
    "8509": "보령", "8420": "인천", "8710": "제주", "9180": "신보령", "9830": "세종",
}
EMPTY_STATIONS = {"8410": "서울건설", "8570": "서천건설"}

# 실측 하한: 20181115 부터 있음 (20181101~13 은 02 = 데이터 없음)
EARLIEST = date(2018, 11, 15)
NO_DATA_CODE = "02"      # 해당 날짜 데이터 없음 (실패 아님)
MAX_CONSECUTIVE_FAILS = 10


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
    # 에러는 HTTP 200 + resultCode 로 온다.
    #   02 "데이터베이스 에러" = 그 날짜에 데이터가 없다는 뜻 (2026-09-09 실측:
    #      보령 2022-09-21~10-31 이 02, 앞뒤 날짜는 정상. 쿼터와 무관)
    #   그 밖(22 요청제한 초과 등)은 진짜 오류라 올린다.
    code = (root.findtext(".//resultCode") or "").strip()
    if code == NO_DATA_CODE:
        return []
    if code and code != "00":
        raise RuntimeError(f"resultCode={code} {(root.findtext('.//resultMsg') or '').strip()}")
    return [{c.tag: (c.text or "").strip() for c in item} for item in root.iter("item")]


def _parquet_path(out_dir: Path, station: str, year: int) -> Path:
    return out_dir / f"{station}_{year}.parquet"


def db_dates(station: str) -> Set[date]:
    """DB(generation)에 이미 적재된 날짜 집합. 본부 단위로 센다.

    `load.py` 가 `plants.site_name` 에 원본 `siteterm`('보령발전본부')을 넣으므로
    STATIONS 의 값에 '발전본부' 를 붙여 맞춘다. DB 가 닫혀 있으면 빈 집합을 돌려주고
    parquet 커서만 쓴다 — 수집이 DB 때문에 멈추지는 않게 한다.
    """
    site = f"{STATIONS.get(station, '')}발전본부"
    try:
        from sqlalchemy import create_engine, text

        from pipeline.fetch_data.common.db_utils import resolve_db_url
        eng = create_engine(resolve_db_url())
        with eng.connect() as conn:
            rows = conn.execute(text("""
                SELECT DISTINCT g.timestamp::date
                  FROM generation g JOIN plants p USING (plant_id)
                 WHERE p.operator = 'jungbu' AND p.site_name = :site
            """), {"site": site}).fetchall()
        return {r[0] for r in rows}
    except Exception as e:  # noqa: BLE001
        logger.warning(f"[{site}] DB 커서 조회 실패 — parquet 커서만 쓴다: {e}")
        return set()


def load_existing(out_dir: Path, station: str) -> Tuple[pd.DataFrame, Set[date]]:
    """이미 받아둔 데이터와 그 날짜 집합(재개용). DB + parquet 합집합이다."""
    files = sorted(out_dir.glob(f"{station}_*.parquet"))
    have = db_dates(station)
    if not files:
        return pd.DataFrame(columns=["siteterm", "unitterm", "timestamp", "daypower"]), have
    df = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)
    return df, have | set(pd.to_datetime(df["timestamp"]).dt.date.unique())


def to_long(rows: List[dict]) -> pd.DataFrame:
    df = pd.DataFrame(rows)
    if df.empty:
        return pd.DataFrame(columns=["siteterm", "unitterm", "timestamp", "daypower"])
    df["timestamp"] = pd.to_datetime(df["gathdtm"], errors="coerce").dt.floor("h")
    df["daypower"] = pd.to_numeric(df["daypower"], errors="coerce")
    return df[["siteterm", "unitterm", "timestamp", "daypower"]].dropna(subset=["timestamp"])


def run(start: date, end: date, budget: int, out_dir: Path = DEFAULT_OUT,
        stations: Dict[str, str] = STATIONS) -> int:
    """[start, end] 를 본부별로 훑는다. 호출 예산 소진 시 중단(다음 실행이 이어받음)."""
    out_dir.mkdir(parents=True, exist_ok=True)
    calls = 0
    fails = 0  # 연속 실패 — 쿼터 소진이면 전 범위를 헛돌지 않게 끊는다

    for station, name in stations.items():
        old, have = load_existing(out_dir, station)
        new: List[pd.DataFrame] = []
        day = start
        while day <= end:
            if calls >= budget or fails >= MAX_CONSECUTIVE_FAILS:
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
                fails += 1
                day += timedelta(days=2)
                continue
            calls += 1
            fails = 0
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
        if fails >= MAX_CONSECUTIVE_FAILS:
            logger.error(f"연속 {fails}회 실패 — 쿼터 소진 의심, {name}({station}) {day} 에서 중단")
            break
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
    p.add_argument("--station", default=None,
                   help="본부 코드 쉼표목록 (기본 전체). 예: 8710,9180 — 앞 본부 재탐색 낭비를 막는다")
    a = p.parse_args()
    run(
        start=_to_date(a.start) if a.start else EARLIEST,
        end=_to_date(a.end) if a.end else date.today() - timedelta(days=1),
        budget=a.budget,
        out_dir=Path(a.out_dir),
        stations={k: v for k, v in STATIONS.items() if k in a.station.split(",")}
        if a.station else STATIONS,
    )


if __name__ == "__main__":
    main()
