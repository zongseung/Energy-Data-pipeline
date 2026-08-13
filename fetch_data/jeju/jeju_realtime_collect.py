"""
KPX 제주 계통수급 실시간 수집기.

엔드포인트: POST https://openapi.kpx.or.kr/downloadChejuSukubCSV.do
- 당일 데이터를 5분 단위로 제공 (약 5분 딜레이)
- 매 5분마다 당일치 전체를 다시 받아 신규 행만 append

저장 위치: /mnt/iscsi-renewable/jeju_data/sukub/jeju_sukub_YYYYMM.csv
  - 기존 월별 파일에 이어쓰기 (upsert-like: timestamp 중복 제거)

실행 방법:
    # 5분마다 무한 루프 (프로세스 유지)
    uv run python -m fetch_data.jeju.jeju_realtime_collect

    # Prefect 또는 cron에서 1회 실행 (외부 스케줄러 사용 시)
    uv run python -m fetch_data.jeju.jeju_realtime_collect --once
"""

from __future__ import annotations

import argparse
import asyncio
import io
import signal
import sys
from datetime import date
from pathlib import Path
from typing import Optional

import aiohttp
import pandas as pd

from fetch_data.common.logger import get_logger
from fetch_data.jeju import jeju_csv_store

logger = get_logger(__name__)

URL = "https://openapi.kpx.or.kr/downloadChejuSukubCSV.do"
OUT_DIR = Path("/mnt/iscsi-renewable/jeju_data/sukub")
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"

POLL_INTERVAL_SEC = 300  # 5분
COLS_RENAME = {
    "기준일시": "ts_raw",
    "공급능력(MW)": "supply_mw",
    "현재수요(MW)": "demand_mw",
    "신재생총합(MW)": "renewable_total_mw",
    "신재생태양광(MW)": "solar_mw",
    "신재생풍력(MW)": "wind_mw",
}


def _out_path(d: date) -> Path:
    return OUT_DIR / f"jeju_sukub_{d.strftime('%Y%m')}.csv"


from fetch_data.common.utils import decode_bytes as _decode


def _parse(body: bytes) -> Optional[pd.DataFrame]:
    df = pd.read_csv(io.StringIO(_decode(body)), dtype=str)
    if df.empty:
        return None
    df.columns = [c.strip() for c in df.columns]
    df.rename(columns={k: v for k, v in COLS_RENAME.items() if k in df.columns}, inplace=True)
    if "ts_raw" in df.columns:
        df["timestamp"] = pd.to_datetime(df["ts_raw"], format="%Y%m%d%H%M%S", errors="coerce")
        df.drop(columns=["ts_raw"], inplace=True)
        df = df[df["timestamp"].notna()].copy()
        df = df[["timestamp"] + [c for c in df.columns if c != "timestamp"]]
    return df if not df.empty else None


def _load_existing(path: Path) -> Optional[pd.DataFrame]:
    if not path.exists():
        return pd.DataFrame(columns=["timestamp"])
    try:
        df = pd.read_csv(path, dtype=str)
        if "timestamp" not in df.columns:
            raise ValueError("timestamp 컬럼 없음")
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
        if df["timestamp"].isna().any():
            raise ValueError("유효하지 않은 timestamp")
        return df
    except Exception as e:
        logger.warning(f"[실시간] 기존 파일 읽기 실패, 저장 생략: {e}")
        return None


def _merge_and_save(existing: pd.DataFrame, fresh: pd.DataFrame, path: Path) -> int:
    """fresh의 신규 행만 append 후 저장. 추가된 행 수 반환."""
    known = set(existing["timestamp"].dropna().astype(str))
    new_rows = fresh[~fresh["timestamp"].astype(str).isin(known)].copy()
    if new_rows.empty:
        return 0
    merged = pd.concat([existing, new_rows], ignore_index=True)
    merged = merged.sort_values("timestamp").reset_index(drop=True)
    jeju_csv_store.atomic_to_csv(merged, path)
    return len(new_rows)


async def _fetch_today(session: aiohttp.ClientSession, target: date) -> Optional[bytes]:
    date_str = target.strftime("%Y-%m-%d")
    data = {"startDate": date_str, "endDate": date_str}
    try:
        async with session.post(
            URL, data=data, timeout=aiohttp.ClientTimeout(total=30)
        ) as r:
            r.raise_for_status()
            body = await r.read()
        if len(body) < 100:
            return None
        return body
    except Exception as e:
        logger.warning(f"  fetch 실패: {e}")
        return None


async def _poll_once(session: aiohttp.ClientSession) -> int:
    """당일 데이터를 받아 기존 파일에 신규 행 추가. 추가 행 수 반환."""
    today = date.today()
    body = await _fetch_today(session, today)
    if not body:
        return 0

    fresh = _parse(body)
    if fresh is None:
        return 0

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    path = _out_path(today)
    with jeju_csv_store.file_lock(path):
        existing = _load_existing(path)
        if existing is None:
            return 0
        added = _merge_and_save(existing, fresh, path)

    if added:
        latest = fresh["timestamp"].max()
        logger.info(f"[실시간] +{added}행 추가 (최신: {latest}) → {path.name}")
    else:
        logger.debug(f"[실시간] 신규 없음 (최신: {fresh['timestamp'].max()})")

    return added


async def _run_loop():
    """5분 간격 무한 폴링 루프."""
    logger.info(f"제주 실시간 수집 시작 (폴링 간격: {POLL_INTERVAL_SEC}초)")

    stop = asyncio.Event()

    def _handle_sig(*_):
        logger.info("종료 신호 수신")
        stop.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, _handle_sig)

    connector = aiohttp.TCPConnector()
    async with aiohttp.ClientSession(
        headers={"User-Agent": USER_AGENT}, connector=connector
    ) as session:
        while not stop.is_set():
            t0 = asyncio.get_event_loop().time()
            try:
                await _poll_once(session)
            except Exception as e:
                logger.warning(f"[실시간] 폴링 오류: {e}")

            elapsed = asyncio.get_event_loop().time() - t0
            wait = max(0, POLL_INTERVAL_SEC - elapsed)
            try:
                await asyncio.wait_for(asyncio.shield(stop.wait()), timeout=wait)
            except asyncio.TimeoutError:
                pass

    logger.info("수집 종료")


async def run_once():
    """1회만 실행 (외부 스케줄러 전용)."""
    connector = aiohttp.TCPConnector()
    async with aiohttp.ClientSession(
        headers={"User-Agent": USER_AGENT}, connector=connector
    ) as session:
        added = await _poll_once(session)
    logger.info(f"1회 실행 완료: +{added}행")


def main():
    parser = argparse.ArgumentParser(description="KPX 제주 계통수급 실시간 수집기")
    parser.add_argument("--once", action="store_true", help="1회 실행 후 종료 (cron/Prefect용)")
    args = parser.parse_args()

    if args.once:
        asyncio.run(run_once())
    else:
        asyncio.run(_run_loop())


if __name__ == "__main__":
    main()
