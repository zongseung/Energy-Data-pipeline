"""
제주 시간별 전력수요 수집기.

두 가지 수집 방식:
  1. data.go.kr 15065239 분기 파일 (최신 분기 wide→long 변환)
  2. KPX sukub 5분 데이터 집계 → 시간별 평균 (과거 이력 백필용)

저장 위치:
  /mnt/iscsi-renewable/jeju_data/demand/jeju_demand_YYYY.csv

컬럼:
  timestamp (KST, hour-start)  demand_mw  source

사용 예:
    # 최신 분기 파일 수집 (data.go.kr)
    uv run python -m fetch_data.jeju.jeju_demand_collect

    # sukub 5분 데이터로 과거 시간별 수요 백필
    uv run python -m fetch_data.jeju.jeju_demand_collect --from-sukub

    # 전체 (최신 분기 + sukub 백필)
    uv run python -m fetch_data.jeju.jeju_demand_collect --full
"""

from __future__ import annotations

import argparse
import asyncio
import io
from datetime import date, datetime
from pathlib import Path
from typing import List, Optional

import aiohttp
import pandas as pd

from fetch_data.common.logger import get_logger

logger = get_logger(__name__)

OUT_DIR = Path("/mnt/iscsi-renewable/jeju_data/demand")
SUKUB_DIR = Path("/mnt/iscsi-renewable/jeju_data/sukub")
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"

# data.go.kr 15065239
DATASET_PK = "15065239"
DOWNLOAD_BASE = "https://www.data.go.kr/cmm/cmm/fileDownload.do"
DATASET_PAGE = f"https://www.data.go.kr/tcs/dss/selectFileDataDetailView.do"

# 알려진 FILE ID (페이지 탐지 실패 시 폴백)
_KNOWN_FILE: tuple = ("FILE_000000003622497", "1")  # Q1 2026 (2026-03-31 기준)

# kWh→MWh 전환 기준: 2023-09-01 이전은 kWh 단위
_UNIT_SWITCH_DATE = date(2023, 9, 1)


# ─── data.go.kr 파일 수집 ────────────────────────────────────────────────────

async def _discover_file(session: aiohttp.ClientSession) -> Optional[tuple]:
    """데이터셋 페이지에서 현재 FILE ID 추출."""
    import re
    try:
        async with session.get(
            DATASET_PAGE,
            params={"publicDataPk": DATASET_PK},
            timeout=aiohttp.ClientTimeout(total=30),
        ) as r:
            r.raise_for_status()
            html = await r.text()
    except Exception as e:
        logger.warning(f"[탐지] 데이터셋 페이지 접근 실패: {e}")
        return None

    m = re.search(
        r'fileDownload\.do\?atchFileId=(FILE_\d+)&fileDetailSn=(\d+)',
        html,
    )
    if m:
        logger.info(f"[탐지] FILE ID: {m.group(1)} / sn={m.group(2)}")
        return (m.group(1), m.group(2))
    return None


from fetch_data.common.utils import decode_bytes as _decode


async def _download_quarterly(
    session: aiohttp.ClientSession,
    atch_id: str,
    sn: str,
    retries: int = 2,
) -> Optional[bytes]:
    params = {"atchFileId": atch_id, "fileDetailSn": sn, "insertDataPrcus": "N"}
    for attempt in range(1, retries + 2):
        try:
            async with session.get(
                DOWNLOAD_BASE, params=params, timeout=aiohttp.ClientTimeout(total=120)
            ) as r:
                r.raise_for_status()
                body = await r.read()
            if len(body) < 500:
                logger.warning(f"  응답 너무 짧음 ({len(body)} bytes, 시도 {attempt})")
            else:
                logger.info(f"  다운로드 완료 ({len(body):,} bytes)")
                return body
        except Exception as e:
            logger.warning(f"  시도 {attempt} 실패: {e}")
        if attempt <= retries:
            await asyncio.sleep(3.0 * attempt)
    logger.error("  최종 실패")
    return None


def _parse_quarterly(body: bytes) -> Optional[pd.DataFrame]:
    """Wide format (날짜 + 1시~24시) → long format (timestamp, demand_mw)."""
    text = _decode(body)
    try:
        df = pd.read_csv(io.StringIO(text), dtype=str)
    except Exception as e:
        logger.warning(f"  CSV 파싱 실패: {e}")
        return None

    df.columns = [c.strip() for c in df.columns]
    if "날짜" not in df.columns:
        logger.warning(f"  '날짜' 컬럼 없음: {list(df.columns)}")
        return None

    # wide → long melt
    hour_cols = [c for c in df.columns if c.endswith("시") and c != "날짜"]
    melted = df.melt(id_vars=["날짜"], value_vars=hour_cols, var_name="hour_label", value_name="demand_raw")
    melted["날짜"] = melted["날짜"].str.strip()
    melted["demand_raw"] = melted["demand_raw"].str.strip() if melted["demand_raw"].dtype == object else melted["demand_raw"]

    # "N시" → hour_ending int
    melted["hour_ending"] = pd.to_numeric(
        melted["hour_label"].str.replace("시", "", regex=False), errors="coerce"
    )
    melted = melted[melted["hour_ending"].between(1, 24)].copy()

    # hour-ending → 구간시작 (N → N-1시)
    melted["hour_start"] = (melted["hour_ending"] - 1).astype(int)
    melted["timestamp"] = pd.to_datetime(
        melted["날짜"] + " " + melted["hour_start"].astype(str).str.zfill(2) + ":00:00",
        errors="coerce",
    )
    melted = melted[melted["timestamp"].notna()].copy()

    # 단위 정규화: 2023-09 이전은 kWh → MWh
    melted["demand_raw"] = pd.to_numeric(melted["demand_raw"], errors="coerce")
    cutoff = pd.Timestamp(_UNIT_SWITCH_DATE)
    pre_mask = melted["timestamp"] < cutoff
    melted["demand_mw"] = melted["demand_raw"].copy()
    melted.loc[pre_mask, "demand_mw"] = melted.loc[pre_mask, "demand_raw"] / 1000.0
    melted["source"] = "datagokr"

    result = (
        melted[["timestamp", "demand_mw", "source"]]
        .sort_values("timestamp")
        .reset_index(drop=True)
    )
    logger.info(f"  파싱 완료: {len(result)}행, {result['timestamp'].min()} ~ {result['timestamp'].max()}")
    return result


async def _collect_quarterly(session: aiohttp.ClientSession) -> List[Path]:
    """15065239 최신 분기 파일 수집 → 연도별 CSV 저장."""
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    file_info = await _discover_file(session)
    if not file_info:
        logger.warning("[분기파일] 파일 탐지 실패, 알려진 ID 사용")
        file_info = _KNOWN_FILE

    atch_id, sn = file_info
    body = await _download_quarterly(session, atch_id, sn)
    if not body:
        return []

    df = _parse_quarterly(body)
    if df is None or df.empty:
        return []

    saved: List[Path] = []
    for year, grp in df.groupby(df["timestamp"].dt.year):
        out_path = OUT_DIR / f"jeju_demand_{year}.csv"
        if out_path.exists():
            # 기존 파일과 merge (dedup)
            existing = pd.read_csv(out_path)
            existing["timestamp"] = pd.to_datetime(existing["timestamp"], errors="coerce")
            combined = pd.concat([existing, grp], ignore_index=True)
            combined = combined.drop_duplicates(subset=["timestamp"]).sort_values("timestamp")
            combined.to_csv(out_path, index=False, encoding="utf-8-sig")
            logger.info(f"  [{year}] 기존 파일 병합 → {out_path.name} ({len(combined)}행)")
        else:
            grp.to_csv(out_path, index=False, encoding="utf-8-sig")
            logger.info(f"  [{year}] 저장: {out_path.name} ({len(grp)}행)")
        saved.append(out_path)

    return saved


# ─── sukub 5분 데이터 집계 ────────────────────────────────────────────────────

def _aggregate_sukub_to_hourly() -> List[Path]:
    """
    /mnt/iscsi-renewable/jeju_data/sukub/ 의 5분 데이터를 읽어
    시간별 평균 demand_mw 로 집계 → demand/ 에 저장.

    sukub CSV 컬럼: timestamp, supply_mw, demand_mw, ...
    """
    sukub_files = sorted(SUKUB_DIR.glob("jeju_sukub_*.csv"))
    if not sukub_files:
        logger.warning("[sukub집계] sukub 파일 없음, 먼저 jeju_sukub_collect.py 실행 필요")
        return []

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    logger.info(f"[sukub집계] {len(sukub_files)}개 월별 파일 처리")

    all_df = []
    for f in sukub_files:
        try:
            df = pd.read_csv(f, usecols=["timestamp", "demand_mw"], dtype=str)
            df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
            df["demand_mw"] = pd.to_numeric(df["demand_mw"], errors="coerce")
            df = df[df["timestamp"].notna() & df["demand_mw"].notna()]
            all_df.append(df)
        except Exception as e:
            logger.warning(f"  {f.name} 읽기 실패: {e}")

    if not all_df:
        return []

    combined = pd.concat(all_df, ignore_index=True)
    # 시간별 집계 (hour-start 기준)
    combined["hour_ts"] = combined["timestamp"].dt.floor("h")
    hourly = (
        combined.groupby("hour_ts")["demand_mw"]
        .mean()
        .reset_index()
        .rename(columns={"hour_ts": "timestamp"})
    )
    hourly["source"] = "sukub_5min"

    saved: List[Path] = []
    for year, grp in hourly.groupby(hourly["timestamp"].dt.year):
        out_path = OUT_DIR / f"jeju_demand_{year}.csv"
        grp = grp.sort_values("timestamp").reset_index(drop=True)
        if out_path.exists():
            existing = pd.read_csv(out_path)
            existing["timestamp"] = pd.to_datetime(existing["timestamp"], errors="coerce")
            # datagokr 데이터 우선, sukub는 빈 구간 보완
            missing_mask = ~grp["timestamp"].isin(existing["timestamp"])
            if missing_mask.any():
                fill = grp[missing_mask].copy()
                combined_out = pd.concat([existing, fill], ignore_index=True)
                combined_out = combined_out.sort_values("timestamp").reset_index(drop=True)
                combined_out.to_csv(out_path, index=False, encoding="utf-8-sig")
                logger.info(f"  [{year}] +{missing_mask.sum()}행 보완 → {out_path.name}")
            else:
                logger.info(f"  [{year}] 신규 없음 (건너뜀)")
        else:
            grp.to_csv(out_path, index=False, encoding="utf-8-sig")
            logger.info(f"  [{year}] 저장: {out_path.name} ({len(grp)}행)")
        saved.append(out_path)

    return saved


# ─── 실행 ─────────────────────────────────────────────────────────────────────

async def _run_async(quarterly: bool = True, from_sukub: bool = False) -> List[Path]:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    logger.info("=" * 60)
    logger.info("제주 시간별 전력수요 수집 시작")

    saved: List[Path] = []

    if quarterly:
        logger.info("[1단계] data.go.kr 15065239 분기 파일 수집")
        connector = aiohttp.TCPConnector()
        async with aiohttp.ClientSession(
            headers={"User-Agent": USER_AGENT}, connector=connector
        ) as session:
            paths = await _collect_quarterly(session)
        saved.extend(paths)

    if from_sukub:
        logger.info("[2단계] sukub 5분 → 시간별 집계")
        paths = _aggregate_sukub_to_hourly()
        saved.extend(paths)

    logger.info(f"완료: {len(saved)}개 파일")
    return saved


def run(quarterly: bool = True, from_sukub: bool = False) -> List[Path]:
    return asyncio.run(_run_async(quarterly=quarterly, from_sukub=from_sukub))


def main():
    parser = argparse.ArgumentParser(description="제주 시간별 전력수요 수집")
    parser.add_argument("--from-sukub", action="store_true",
                        help="sukub 5분 데이터에서 시간별 수요 집계 (과거 백필)")
    parser.add_argument("--full", action="store_true",
                        help="분기 파일 수집 + sukub 백필 모두 실행")
    parser.add_argument("--no-quarterly", action="store_true",
                        help="data.go.kr 분기 파일 수집 생략")
    args = parser.parse_args()

    quarterly = not args.no_quarterly
    from_sukub = args.from_sukub or args.full

    run(quarterly=quarterly, from_sukub=from_sukub)


if __name__ == "__main__":
    main()
