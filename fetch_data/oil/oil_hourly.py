"""시간별 국제유가 OHLCV 수집 — Hyperliquid XYZ builder DEX 캔들.

브렌트(`xyz:BRENTOIL`)·WTI(`xyz:CL`) 1시간 캔들을 받아 `oil_hourly_all.csv` 에
누적한다. 매시 정각 실행.

    uv run python -m fetch_data.oil.oil_hourly

seri-data(`src/oil_hourly.py`)에서 가져왔다. 수집·병합 로직은 그대로 두고
Prefect·Slack 만 떼어 `prefect_flows/oil_flow.py` 로 옮겼다 — 이 저장소는
수집기를 순수 함수로 두고 flow 가 오케스트레이션을 맡는 구조다
(`fetch_data/smp/smp_realtime.py` ↔ `prefect_flows/smp_flow.py` 와 같다).

왜 CSV 인가
    이 파일은 pv-db 에 `file_fdw` 외부 테이블로 그대로 붙는다(sql/research/
    oil_fdw.sql). 적재 단계가 없으므로 수집기가 파일을 갱신하면 다음 조회부터
    바로 최신이다. 5천 행 규모라 파일 하나로 충분하고, 예보 3종과 달리 대상이
    한 파일이라 file_fdw 의 "파일명 고정" 제약이 문제가 되지 않는다.

주의
    seri-data 컨테이너도 같은 API 를 매시 호출한다. 두 곳이 각자 자기 CSV 를
    쌓는 상태이며, 원천이 같으므로 값은 같다. 한쪽으로 정리하려면 seri-data 의
    oil_hourly flow 를 끄면 된다.
"""

from __future__ import annotations

import argparse
import asyncio
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import aiohttp
import polars as pl

from fetch_data.common.logger import get_logger

logger = get_logger(__name__)

API_URL = "https://api.hyperliquid.xyz/info"

# short_name -> hyperliquid coin ticker
COINS: dict[str, str] = {
    "brent": "xyz:BRENTOIL",
    "wti": "xyz:CL",
}

INTERVAL = "1h"
INTERVAL_MS = 3600 * 1000

MAX_RETRIES = 3
RETRY_DELAY_SEC = 5.0
REQUEST_TIMEOUT_SEC = 30.0

# 한 번에 받을 수 있는 최대 캔들 수 (API 제한)
MAX_CANDLES_PER_CALL = 5000
# 증분 수집 시 안전 마진 — 진행 중 캔들이 확정되면 값이 바뀌므로 다시 받아 덮는다
INCREMENTAL_LOOKBACK_HOURS = 48

CUMULATIVE_FILE = "oil_hourly_all.csv"

# flow 컨테이너는 /mnt/nvme/Energy-Data-pipeline/data 를 /app/data 로 마운트한다
# (prefect_flows/deploy.py:get_job_variables). pv-db 는 같은 경로의 oil 하위를
# 읽기전용으로 본다 — 두 컨테이너가 같은 파일을 가리켜야 file_fdw 가 성립한다.
DEFAULT_DATA_DIR = "/app/data/oil"

KST = timezone(timedelta(hours=9))


def data_dir() -> Path:
    return Path(os.getenv("OIL_DATA_DIR", DEFAULT_DATA_DIR)).resolve()


# ---------------------------------------------------------------------------
# 수집
# ---------------------------------------------------------------------------


async def fetch_candles(
    session: aiohttp.ClientSession,
    coin: str,
    start_ms: int,
    end_ms: int,
) -> list[dict[str, Any]]:
    """단일 coin 의 캔들 리스트 수신. 실패 시 빈 리스트."""
    payload = {
        "type": "candleSnapshot",
        "req": {
            "coin": coin,
            "interval": INTERVAL,
            "startTime": start_ms,
            "endTime": end_ms,
        },
    }

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            async with session.post(
                API_URL,
                json=payload,
                headers={"Content-Type": "application/json"},
                timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT_SEC),
            ) as resp:
                if resp.status != 200:
                    logger.warning(
                        f"{coin}: HTTP {resp.status} (시도 {attempt}/{MAX_RETRIES})")
                else:
                    data = await resp.json(content_type=None)
                    if isinstance(data, list):
                        logger.info(f"{coin}: {len(data)} 캔들 수신")
                        return data
                    logger.warning(f"{coin}: 응답 형식 이상 ({type(data).__name__})")
        except Exception as exc:  # noqa: BLE001
            logger.error(f"{coin}: 예외 (시도 {attempt}/{MAX_RETRIES}) — {exc}")
        if attempt < MAX_RETRIES:
            await asyncio.sleep(RETRY_DELAY_SEC)

    logger.error(f"{coin}: {MAX_RETRIES}회 모두 실패")
    return []


def _candles_to_df(candles: list[dict[str, Any]], short_name: str) -> pl.DataFrame:
    schema = {
        "t": pl.Int64,
        f"{short_name}_o": pl.Float64,
        f"{short_name}_h": pl.Float64,
        f"{short_name}_l": pl.Float64,
        f"{short_name}_c": pl.Float64,
        f"{short_name}_v": pl.Float64,
        f"{short_name}_n": pl.Int64,
    }
    if not candles:
        return pl.DataFrame(schema=schema)
    rows = [
        {
            "t": int(c["t"]),
            f"{short_name}_o": float(c["o"]),
            f"{short_name}_h": float(c["h"]),
            f"{short_name}_l": float(c["l"]),
            f"{short_name}_c": float(c["c"]),
            f"{short_name}_v": float(c["v"]),
            f"{short_name}_n": int(c["n"]),
        }
        for c in candles
    ]
    return pl.DataFrame(rows, schema=schema)


async def collect_all_coins(start_ms: int, end_ms: int) -> pl.DataFrame:
    """모든 coin 을 동시 수집 후 t 기준 wide-format 으로 병합."""
    async with aiohttp.ClientSession() as session:
        results = await asyncio.gather(*[
            fetch_candles(session, coin, start_ms, end_ms) for coin in COINS.values()
        ])

    frames = [
        _candles_to_df(candles, short_name)
        for short_name, candles in zip(COINS.keys(), results)
    ]
    non_empty = [df for df in frames if not df.is_empty()]
    if not non_empty:
        return pl.DataFrame(schema={"t": pl.Int64})

    merged = non_empty[0]
    for df in non_empty[1:]:
        merged = merged.join(df, on="t", how="full", coalesce=True)

    merged = merged.with_columns(
        pl.from_epoch(pl.col("t"), time_unit="ms")
        .dt.replace_time_zone("UTC")
        .dt.convert_time_zone("Asia/Seoul")
        .dt.strftime("%Y-%m-%d %H:%M:%S")
        .alias("ts_kst")
    )

    front = ["t", "ts_kst"]
    rest = [c for c in merged.columns if c not in front]
    return merged.select(front + rest).sort("t", descending=True)


# ---------------------------------------------------------------------------
# 누적 파일
# ---------------------------------------------------------------------------


def load_existing(directory: Path | None = None) -> pl.DataFrame:
    path = (directory or data_dir()) / CUMULATIVE_FILE
    if path.exists():
        return pl.read_csv(path, schema_overrides={"t": pl.Int64})
    return pl.DataFrame(schema={"t": pl.Int64})


def merge_and_save(
    new_df: pl.DataFrame,
    existing: pl.DataFrame,
    directory: Path | None = None,
) -> Path:
    """new 가 existing 을 덮어쓰는 우선순위로 병합.

    진행 중이던 캔들은 다음 수집에서 확정값으로 다시 오므로 new 를 우선한다.
    """
    directory = directory or data_dir()
    directory.mkdir(parents=True, exist_ok=True)

    if existing.is_empty():
        merged = new_df
    elif new_df.is_empty():
        merged = existing
    else:
        merged = (
            pl.concat([new_df, existing], how="diagonal")
            .unique(subset=["t"], keep="first")
            .sort("t", descending=True)
        )

    path = directory / CUMULATIVE_FILE
    merged.write_csv(path)
    # file_fdw 로 붙어 있어 postgres(uid 999)가 읽어야 한다. 기본 umask 로는
    # 소유자만 읽는 파일이 나올 수 있어 명시적으로 열어 준다.
    path.chmod(0o644)
    logger.info(f"저장 완료: {path} ({len(merged)}행)")
    return path


# ---------------------------------------------------------------------------
# 실행
# ---------------------------------------------------------------------------


async def run_once(directory: Path | None = None) -> int:
    """한 번 수집해 누적 파일을 갱신한다. 반환: 신규 수집 행수."""
    existing = load_existing(directory)
    now_ms = int(datetime.now(tz=KST).timestamp() * 1000)

    if existing.is_empty():
        start_ms = now_ms - MAX_CANDLES_PER_CALL * INTERVAL_MS
        logger.info(f"초기 수집: 최근 {MAX_CANDLES_PER_CALL}시간")
    else:
        last_t = int(existing["t"].max())
        start_ms = min(last_t - INCREMENTAL_LOOKBACK_HOURS * INTERVAL_MS, now_ms)
        start_ms = max(start_ms, now_ms - MAX_CANDLES_PER_CALL * INTERVAL_MS)
        logger.info(f"증분 수집: {max((now_ms - start_ms) // INTERVAL_MS, 0)}시간 윈도우")

    new_df = await collect_all_coins(start_ms, now_ms)
    if new_df.is_empty():
        logger.warning(
            "수집 데이터 없음 — Hyperliquid 응답이 비었다. 며칠 이상 이어지면 "
            "티커(xyz:BRENTOIL / xyz:CL)가 바뀌었는지 확인하라."
        )
        return 0

    merge_and_save(new_df, existing, directory)
    logger.info(f"완료: 신규 {len(new_df)}행, 누적 {len(load_existing(directory))}행")
    return len(new_df)


def main() -> int:
    ap = argparse.ArgumentParser(description="시간별 국제유가 OHLCV 수집 (Hyperliquid)")
    ap.add_argument("--data-dir", default=None, help=f"기본: $OIL_DATA_DIR 또는 {DEFAULT_DATA_DIR}")
    args = ap.parse_args()
    directory = Path(args.data_dir).resolve() if args.data_dir else None
    return asyncio.run(run_once(directory))


if __name__ == "__main__":
    print("수집 행수:", main())
