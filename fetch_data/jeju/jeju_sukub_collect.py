"""
KPX 제주 계통수급 5분 실적 수집기.

엔드포인트: POST https://openapi.kpx.or.kr/downloadChejuSukubCSV.do
파라미터: startDate=YYYY-MM-DD, endDate=YYYY-MM-DD
응답 CSV 컬럼:
    기준일시, 공급능력(MW), 현재수요(MW), 신재생총합(MW), 신재생태양광(MW), 신재생풍력(MW)
기준일시 형식: YYYYMMDDHHMMSS (5분 단위, 96구간/일)

저장 위치: /mnt/iscsi-renewable/jeju_data/sukub/jeju_sukub_YYYYMM.csv (월 단위)

사용 예:
    # 최근 3개월
    uv run python -m fetch_data.jeju.jeju_sukub_collect

    # 특정 기간
    uv run python -m fetch_data.jeju.jeju_sukub_collect --start 2024-01-01 --end 2024-12-31

    # 전체 이력 자동 수집 (탐지 방식)
    uv run python -m fetch_data.jeju.jeju_sukub_collect --full
"""

from __future__ import annotations

import argparse
import asyncio
import io
from datetime import date, timedelta
from pathlib import Path
from typing import List, Optional, Tuple

import aiohttp
import pandas as pd

from fetch_data.common.logger import get_logger

logger = get_logger(__name__)

URL = "https://openapi.kpx.or.kr/downloadChejuSukubCSV.do"
OUT_DIR = Path("/mnt/iscsi-renewable/jeju_data/sukub")
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
CONCURRENCY = 4
EARLIEST_FLOOR = date(2021, 1, 1)


# ─── 날짜 헬퍼 ───────────────────────────────────────────────────────────────

from fetch_data.common.date_utils import month_end as _month_end, add_months as _add_months


def _month_ranges(start: date, end: date) -> List[Tuple[date, date]]:
    ranges = []
    cur = start.replace(day=1)
    while cur <= end:
        me = _month_end(cur)
        ranges.append((cur, me if me <= end else end))
        cur = _add_months(cur, 1)
    return ranges


def _recent_months(n: int) -> Tuple[date, date]:
    today = date.today()
    return _add_months(today.replace(day=1), -(n - 1)), today - timedelta(days=1)


# ─── HTTP ─────────────────────────────────────────────────────────────────────

from fetch_data.common.utils import decode_bytes as _decode


async def _fetch_month(
    session: aiohttp.ClientSession,
    sem: asyncio.Semaphore,
    first: date,
    last: date,
    retries: int = 2,
) -> Optional[bytes]:
    data = {
        "startDate": first.strftime("%Y-%m-%d"),
        "endDate": last.strftime("%Y-%m-%d"),
    }
    async with sem:
        for attempt in range(1, retries + 2):
            try:
                async with session.post(URL, data=data, timeout=aiohttp.ClientTimeout(total=60)) as r:
                    r.raise_for_status()
                    body = await r.read()
                if len(body) < 200:
                    logger.warning(f"  {first}~{last} 응답 너무 짧음 ({len(body)} bytes, 시도 {attempt})")
                else:
                    return body
            except Exception as e:
                logger.warning(f"  {first}~{last} 시도 {attempt} 실패: {e}")
            if attempt <= retries:
                await asyncio.sleep(2.0 * attempt)
    logger.error(f"  {first}~{last} 최종 실패")
    return None


# ─── 저장 ─────────────────────────────────────────────────────────────────────

def _save_month(body: bytes, first: date) -> Optional[Path]:
    text = _decode(body)
    try:
        df = pd.read_csv(io.StringIO(text), dtype=str)
    except Exception as e:
        logger.warning(f"  CSV 파싱 실패: {e}")
        return None
    if df.empty:
        logger.info(f"  {first.strftime('%Y-%m')} 빈 응답, 저장 생략")
        return None

    df.columns = [c.strip() for c in df.columns]
    df.rename(columns={
        "기준일시": "ts_raw",
        "공급능력(MW)": "supply_mw",
        "현재수요(MW)": "demand_mw",
        "신재생총합(MW)": "renewable_total_mw",
        "신재생태양광(MW)": "solar_mw",
        "신재생풍력(MW)": "wind_mw",
    }, inplace=True)

    if "ts_raw" not in df.columns:
        logger.warning(f"  {first.strftime('%Y-%m')} 기준일시 컬럼 없음, 저장 생략")
        return None
    df["timestamp"] = pd.to_datetime(df["ts_raw"], format="%Y%m%d%H%M%S", errors="coerce")
    df.drop(columns=["ts_raw"], inplace=True)
    df = df[df["timestamp"].notna()].copy()
    if df.empty:
        logger.info(f"  {first.strftime('%Y-%m')} 유효 행 없음, 저장 생략")
        return None
    cols = ["timestamp"] + [c for c in df.columns if c != "timestamp"]
    df = df[cols]

    out_path = OUT_DIR / f"jeju_sukub_{first.strftime('%Y%m')}.csv"
    if out_path.exists():
        existing = pd.read_csv(out_path, dtype=str)
        existing["timestamp"] = pd.to_datetime(existing["timestamp"], errors="coerce")
        df = pd.concat([existing, df], ignore_index=True)
    df = df.drop_duplicates(subset="timestamp", keep="last").sort_values("timestamp")
    df.to_csv(out_path, index=False, encoding="utf-8-sig")
    logger.info(f"  저장: {out_path.name} ({len(df)}행)")
    return out_path


# ─── 탐지 ─────────────────────────────────────────────────────────────────────

async def _detect_start(session: aiohttp.ClientSession) -> date:
    """데이터 최초 가용월을 이분 탐색으로 탐지."""
    ref = date.today().replace(day=1)
    lo = EARLIEST_FLOOR.year * 12 + (EARLIEST_FLOOR.month - 1)
    hi = ref.year * 12 + (ref.month - 2)
    best = hi
    sem = asyncio.Semaphore(1)

    while lo <= hi:
        mid = (lo + hi) // 2
        y, m = divmod(mid, 12)
        probe = date(y, m + 1, 1)
        body = await _fetch_month(session, sem, probe, _month_end(probe))
        if body and len(body) > 500:
            best = mid
            hi = mid - 1
        else:
            lo = mid + 1
        await asyncio.sleep(0.5)

    y, m = divmod(best, 12)
    result = date(y, m + 1, 1)
    logger.info(f"[탐지] 최초 가용월: {result.strftime('%Y-%m')}")
    return result


# ─── 실행 ─────────────────────────────────────────────────────────────────────

async def _run_async(start: date, end: date) -> List[Path]:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    ranges = _month_ranges(start, end)
    logger.info(f"제주 계통수급 수집 시작: {start} ~ {end} ({len(ranges)}개월)")

    sem = asyncio.Semaphore(CONCURRENCY)
    connector = aiohttp.TCPConnector(limit=CONCURRENCY)
    async with aiohttp.ClientSession(
        headers={"User-Agent": USER_AGENT}, connector=connector
    ) as session:
        async def handle(first: date, last: date) -> Optional[Path]:
            logger.info(f"  {first.strftime('%Y-%m')} 수집 중...")
            body = await _fetch_month(session, sem, first, last)
            if body:
                return _save_month(body, first)
            return None

        results = await asyncio.gather(*[handle(f, l) for f, l in ranges])

    saved = [p for p in results if p]
    logger.info(f"완료: {len(saved)}개월 저장")
    return saved


def run(start: date, end: date) -> List[Path]:
    return asyncio.run(_run_async(start, end))


def main():
    parser = argparse.ArgumentParser(description="KPX 제주 계통수급 5분 실적 수집")
    parser.add_argument("--full", action="store_true", help="전체 이력 자동 탐지 수집")
    parser.add_argument("--start", default=None, help="시작일 YYYY-MM-DD")
    parser.add_argument("--end", default=None, help="종료일 YYYY-MM-DD")
    parser.add_argument("--months", type=int, default=3, help="최근 N개월 (기본 3)")
    args = parser.parse_args()

    if args.full:
        async def _full():
            async with aiohttp.ClientSession(headers={"User-Agent": USER_AGENT}) as session:
                start = await _detect_start(session)
            end = _month_end(_add_months(date.today().replace(day=1), -1))
            return await _run_async(start, end)
        asyncio.run(_full())
    elif args.start or args.end:
        if not (args.start and args.end):
            raise SystemExit("--start 와 --end 는 함께 지정해야 합니다.")
        run(date.fromisoformat(args.start), date.fromisoformat(args.end))
    else:
        start, end = _recent_months(args.months)
        run(start, end)


if __name__ == "__main__":
    main()
