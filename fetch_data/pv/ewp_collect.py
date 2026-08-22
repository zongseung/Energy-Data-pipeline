"""
한국동서발전(EWP) 일자별·지점별 태양광 발전량 수집 (공공데이터포털 15099650).

파일데이터 1개(연간 갱신)를 받아 24시간 wide → long 으로 펴서 generation/plants
코어에 멱등 적재한다. 발전기 5기, 2022-04 ~ 최신.

원본 컬럼: 날짜, 설비용량(MW), 경도, 위도, 발전기명, 01시…24시

원본의 함정 3개 (전부 여기서 보정한다):
  1. **경도/위도 컬럼이 서로 바뀌어 있다.** '경도'에 37.05(위도), '위도'에
     126.51(경도)이 들어 있다. 당진·동해·울산 실제 좌표로 확인했다.
  2. **단위가 kWh 가 아니라 Wh 다.** 포털 설명은 kWh 라고 하지만, 시간 최대값이
     설비용량(kW)의 685~829 배라 Wh 로 봐야 이용률 0.69~0.83 이 된다.
  3. 숫자에 천단위 쉼표가 섞여 있다.

시간 규약: 1~24시 **구간종료** 표기(KOEN 과 동일). 6월 프로파일이 06~20시,
정점 13~14시로 나오는 것으로 확인했다. 적재 시 (N-1)시 구간시작으로 옮긴다.

주의: '동해바이오화력본부 태양광' 1기는 6월 프로파일이 22시까지 이어지고 정점이
19시라 다른 4기와 다르다. 원본 자체의 특성으로 보이며 보정하지 않았다.

사용:
    uv run python -m fetch_data.pv.ewp_collect
"""
from __future__ import annotations

import argparse
import re
from typing import Optional

import pandas as pd
import requests
from sqlalchemy import create_engine, text

from fetch_data.common.db_utils import resolve_db_url
from fetch_data.common.generation_core import upsert_generation
from fetch_data.common.logger import get_logger
from fetch_data.common.utils import decode_bytes

logger = get_logger(__name__)

DATASET = "15099650"
DATASET_PAGE = f"https://www.data.go.kr/data/{DATASET}/fileData.do"
DOWNLOAD_URL = "https://www.data.go.kr/cmm/cmm/fileDownload.do"
OPERATOR = "ewp"   # 한국동서발전
FUEL = "solar"

# 페이지에서 atchFileId 를 못 찾을 때 쓰는 폴백 (2026-08 실측)
_KNOWN_FILE_ID = "FILE_000000003591080"
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"


def _find_file_id() -> str:
    """데이터셋 페이지에서 첨부파일 ID 를 찾는다. 실패하면 폴백을 쓴다.

    이 페이지는 jeju 수집기가 쓰는 fn_fileDown(...) 패턴이 아니라 atchFileId 가
    본문에 그대로 박혀 있다.
    """
    try:
        html = requests.get(DATASET_PAGE, headers={"User-Agent": USER_AGENT}, timeout=30).text
        m = re.search(r"FILE_\d+", html)
        if m:
            return m.group(0)
        logger.warning("[ewp] 페이지에서 atchFileId 를 못 찾음 — 폴백 사용")
    except Exception as e:
        logger.warning(f"[ewp] 데이터셋 페이지 접근 실패({e}) — 폴백 사용")
    return _KNOWN_FILE_ID


def _download() -> pd.DataFrame:
    file_id = _find_file_id()
    r = requests.get(
        DOWNLOAD_URL,
        params={"atchFileId": file_id, "fileDetailSn": "1", "insertDataPrcus": "N"},
        headers={"User-Agent": USER_AGENT, "Referer": DATASET_PAGE},
        timeout=180,
    )
    r.raise_for_status()
    if len(r.content) < 2000:
        raise RuntimeError(f"응답이 너무 짧다({len(r.content)} bytes) — 파일 ID 확인 필요")
    logger.info(f"[ewp] 다운로드 {len(r.content):,} bytes (atchFileId={file_id})")
    import io
    return pd.read_csv(io.StringIO(decode_bytes(r.content)))


def _to_long(raw: pd.DataFrame) -> pd.DataFrame:
    """24시간 wide → [timestamp, plant_name, generation] long."""
    df = raw.rename(columns={"발전기명": "plant_name"}).copy()
    df["date"] = pd.to_datetime(df["날짜"], errors="coerce")
    df["plant_name"] = df["plant_name"].astype(str).str.strip()

    hour_cols = [c for c in df.columns if re.match(r"^\s*\d+\s*시\s*$", str(c))]
    long = df.melt(id_vars=["plant_name", "date"], value_vars=hour_cols,
                   var_name="h", value_name="generation")
    long["hour"] = long["h"].str.extract(r"(\d+)").astype(int)
    # 1~24시 구간종료 → (N-1)시 구간시작
    long["timestamp"] = long["date"] + pd.to_timedelta(long["hour"] - 1, unit="h")
    # 천단위 쉼표 제거 후 Wh → kWh
    long["generation"] = (
        pd.to_numeric(long["generation"].astype(str).str.replace(",", ""), errors="coerce") / 1000.0
    )
    return long[["timestamp", "plant_name", "generation"]].dropna(
        subset=["timestamp", "plant_name"]
    )


def _update_master(raw: pd.DataFrame, engine) -> int:
    """발전기별 좌표·설비용량을 plants 에 반영. 갱신 행수 반환.

    원본의 '경도'가 위도, '위도'가 경도다(위 docstring 참고) — 뒤집어 넣는다.
    """
    master = (
        raw.rename(columns={"발전기명": "plant_name", "설비용량(MW)": "capacity_mw"})
        .groupby("plant_name")
        .agg(capacity_mw=("capacity_mw", "first"),
             lat=("경도", "first"),    # 원본 라벨이 뒤바뀜
             lon=("위도", "first"))
        .reset_index()
    )
    sql = text("""
        UPDATE plants SET lat = :lat, lon = :lon,
                          capacity_mw = :cap, capacity_confidence = '확실'
        WHERE plant_name = :name AND operator = :op
    """)
    n = 0
    with engine.begin() as conn:
        for r in master.itertuples(index=False):
            n += conn.execute(sql, {
                "lat": float(r.lat), "lon": float(r.lon),
                "cap": float(r.capacity_mw),
                "name": str(r.plant_name).strip(), "op": OPERATOR,
            }).rowcount
    logger.info(f"[ewp] plants 마스터 {n}행 갱신 (좌표·설비용량)")
    return n


def run(db_url: Optional[str] = None) -> int:
    """동서발전 PV 를 generation/plants 코어에 적재. 적재 행수 반환."""
    raw = _download()
    df = _to_long(raw)
    logger.info(f"[ewp] 변환 {len(df)}행 / 발전기 {df['plant_name'].nunique()}기")

    url = resolve_db_url(db_url)
    n = upsert_generation(df, operator=OPERATOR, fuel_type=FUEL, db_url=url)
    _update_master(raw, create_engine(url))
    return n


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="한국동서발전 지점별 태양광 수집 (data.go.kr 15099650)")
    ap.add_argument("--db-url", default=None)
    args = ap.parse_args()
    print("적재 행수:", run(db_url=args.db_url))
