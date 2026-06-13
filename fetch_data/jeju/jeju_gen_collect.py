"""
공공데이터포털 - 제주 연료원별 전력거래량 수집기.
데이터셋: https://www.data.go.kr/data/15100214/fileData.do
연간 단위 CSV 파일 다운로드 방식.

CSV 컬럼: 거래일자, 거래시간, 연료원, 전력거래량(MWh)
거래시간: 01~24 (hour-ending → 구간시작으로 변환: N → N-1시)
연료원: LNG, 유류, 기타, 신재생(기타), 신재생(태양광), 신재생(풍력)

저장 위치: /mnt/iscsi-renewable/jeju_data/gen/jeju_gen_YYYY.csv

사용 예:
    # 발견된 모든 연도 파일 다운로드
    uv run python -m fetch_data.jeju.jeju_gen_collect

    # 특정 연도만
    uv run python -m fetch_data.jeju.jeju_gen_collect --year 2024
"""

from __future__ import annotations

import argparse
import asyncio
import io
import re
from pathlib import Path
from typing import Dict, List, Optional

import aiohttp
import pandas as pd
from bs4 import BeautifulSoup

from fetch_data.common.logger import get_logger

logger = get_logger(__name__)

DATASET_PAGE = "https://www.data.go.kr/data/15100214/fileData.do"
DOWNLOAD_BASE = "https://www.data.go.kr/cmm/cmm/fileDownload.do"
OUT_DIR = Path("/mnt/iscsi-renewable/jeju_data/gen")
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
CONCURRENCY = 2  # 공공데이터포털 과부하 방지

# 알려진 파일 ID (페이지 스크래핑 실패 시 폴백)
# key=연도, value=(atchFileId, fileDetailSn)
_KNOWN_FILES: Dict[int, tuple] = {
    2024: ("FILE_000000003526367", "1"),
}

_FUEL_MAP = {
    "LNG": "lng",
    "유류": "oil",
    "기타": "other",
    "신재생(기타)": "renewable_other",
    "신재생(태양광)": "solar",
    "신재생(풍력)": "wind",
}


# ─── 파일 목록 탐지 ───────────────────────────────────────────────────────────

async def _discover_files(session: aiohttp.ClientSession) -> Dict[int, tuple]:
    """데이터셋 페이지를 파싱해 연도별 (atchFileId, fileDetailSn) 반환."""
    try:
        async with session.get(DATASET_PAGE, timeout=aiohttp.ClientTimeout(total=30)) as r:
            r.raise_for_status()
            html = await r.text()
    except Exception as e:
        logger.warning(f"[탐지] 데이터셋 페이지 접근 실패: {e}")
        return {}

    soup = BeautifulSoup(html, "html.parser")
    files: Dict[int, tuple] = {}

    for tag in soup.find_all(True, attrs={"href": re.compile(r"fileDown")}):
        href = tag.get("href", "")
        m = re.search(r"fn_fileDown\('([^']+)',\s*'([^']+)'\)", href)
        if m:
            atch_id, sn, label = m.group(1), m.group(2), tag.get_text(strip=True)
            ym = re.search(r"(20\d{2})", label)
            if ym:
                files[int(ym.group(1))] = (atch_id, sn)
                logger.info(f"  [탐지] {ym.group(1)}년: {atch_id}")

    # onclick 패턴 폴백
    if not files:
        for tag in soup.find_all(True, onclick=re.compile(r"fileDown")):
            onclick = tag.get("onclick", "")
            m = re.search(r"fn_fileDown\('([^']+)',\s*'([^']+)'\)", onclick)
            if m:
                atch_id, sn, label = m.group(1), m.group(2), tag.get_text(strip=True)
                ym = re.search(r"(20\d{2})", label)
                if ym:
                    files[int(ym.group(1))] = (atch_id, sn)

    return files


# ─── 다운로드 & 파싱 ─────────────────────────────────────────────────────────

from fetch_data.common.utils import decode_bytes as _decode


async def _download(
    session: aiohttp.ClientSession,
    sem: asyncio.Semaphore,
    atch_id: str,
    sn: str,
    year: int,
    retries: int = 2,
) -> Optional[bytes]:
    params = {"atchFileId": atch_id, "fileDetailSn": sn, "insertDataPrcus": "N"}
    async with sem:
        for attempt in range(1, retries + 2):
            try:
                async with session.get(
                    DOWNLOAD_BASE, params=params, timeout=aiohttp.ClientTimeout(total=120)
                ) as r:
                    r.raise_for_status()
                    body = await r.read()
                if len(body) < 500:
                    logger.warning(f"  {year}년 응답 너무 짧음 ({len(body)} bytes)")
                else:
                    logger.info(f"  {year}년 다운로드 완료 ({len(body):,} bytes)")
                    return body
            except Exception as e:
                logger.warning(f"  {year}년 시도 {attempt} 실패: {e}")
            if attempt <= retries:
                await asyncio.sleep(3.0 * attempt)
    logger.error(f"  {year}년 최종 실패")
    return None


def _parse(body: bytes, year: int) -> Optional[pd.DataFrame]:
    text = _decode(body)
    try:
        df = pd.read_csv(io.StringIO(text), dtype=str)
    except Exception as e:
        logger.warning(f"  {year}년 CSV 파싱 실패: {e}")
        return None

    if df.empty:
        return None

    df.columns = [c.strip() for c in df.columns]
    need = {"거래일자", "거래시간", "연료원", "전력거래량(MWh)"}
    if not need.issubset(df.columns):
        logger.warning(f"  {year}년 예상 컬럼 없음. 실제: {list(df.columns)}")
        return None

    df.rename(columns={"전력거래량(MWh)": "gen_mwh"}, inplace=True)
    df["거래일자"] = df["거래일자"].str.strip()
    df["거래시간"] = df["거래시간"].str.strip().str.zfill(2)
    df["연료원"] = df["연료원"].str.strip()

    # hour-ending → 구간시작 (N → N-1시)
    df["hour_int"] = pd.to_numeric(df["거래시간"], errors="coerce")
    df = df[df["hour_int"].between(1, 24)].copy()
    df["hour_start"] = (df["hour_int"] - 1).astype(int)

    df["timestamp"] = pd.to_datetime(
        df["거래일자"] + " " + df["hour_start"].astype(str).str.zfill(2) + ":00:00",
        errors="coerce",
    )
    df = df[df["timestamp"].notna()].copy()

    df["fuel_type"] = df["연료원"].map(_FUEL_MAP).fillna(df["연료원"])
    df["gen_mwh"] = pd.to_numeric(df["gen_mwh"], errors="coerce")

    return (
        df[["timestamp", "fuel_type", "gen_mwh"]]
        .sort_values(["timestamp", "fuel_type"])
        .reset_index(drop=True)
    )


# ─── 실행 ─────────────────────────────────────────────────────────────────────

async def _run_async(years: Optional[List[int]] = None) -> List[Path]:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    logger.info("=" * 60)
    logger.info("제주 연료원별 전력거래량 수집 시작")

    connector = aiohttp.TCPConnector(limit=CONCURRENCY)
    async with aiohttp.ClientSession(
        headers={"User-Agent": USER_AGENT}, connector=connector
    ) as session:
        discovered = await _discover_files(session)
        file_map = {**_KNOWN_FILES, **discovered}
        if years:
            file_map = {y: v for y, v in file_map.items() if y in years}

        if not file_map:
            logger.error("다운로드할 파일을 찾지 못했습니다.")
            return []

        logger.info(f"대상 연도: {sorted(file_map.keys())}")
        sem = asyncio.Semaphore(CONCURRENCY)

        async def handle(year: int, atch_id: str, sn: str) -> Optional[Path]:
            out_path = OUT_DIR / f"jeju_gen_{year}.csv"
            if out_path.exists():
                logger.info(f"  {year}년 이미 존재, 건너뜀")
                return out_path
            logger.info(f"  {year}년 다운로드 중...")
            body = await _download(session, sem, atch_id, sn, year)
            if not body:
                return None
            df = _parse(body, year)
            if df is None or df.empty:
                logger.warning(f"  {year}년 파싱 결과 없음")
                return None
            df.to_csv(out_path, index=False, encoding="utf-8-sig")
            logger.info(f"  저장: {out_path.name} ({len(df)}행, 연료원: {df['fuel_type'].unique().tolist()})")
            return out_path

        results = await asyncio.gather(*[handle(y, a, s) for y, (a, s) in file_map.items()])

    saved = [p for p in results if p]
    logger.info(f"완료: {len(saved)}개 파일 저장")
    return saved


def run(years: Optional[List[int]] = None) -> List[Path]:
    return asyncio.run(_run_async(years))


def main():
    parser = argparse.ArgumentParser(description="제주 연료원별 전력거래량 수집")
    parser.add_argument("--year", type=int, nargs="+", default=None, help="수집할 연도 (기본: 발견된 전체)")
    args = parser.parse_args()
    run(years=args.year)


if __name__ == "__main__":
    main()
