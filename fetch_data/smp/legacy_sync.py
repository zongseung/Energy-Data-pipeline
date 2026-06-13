"""
공통 DB -> 개인 DB(SMP_LEGACY_DB_URL) 증분 백업 동기화.

개인 DB는 백업 용도이며, daily 수집 경로와 분리되어 있다(장애가 전파되지 않음).
weekly로 실행해 공통 DB의 SMP 3개 테이블을 개인 DB로 증분 복제한다.

동작:
  1) SMP_LEGACY_DB_URL 미설정 -> 즉시 skip(로그만).
  2) 설정 -> 개인 DB에 SMP 테이블 보장(create_all) 후, 테이블별로
     개인 DB의 max(timestamp) 이후 행만 공통 DB에서 읽어 upsert(첫 실행은 전량).

키 기준:
  - smp_hourly / smp_realtime_jeju : timestamp 증분
  - smp_weighted_avg               : 키가 timestamp가 아니므로 전량 비교 upsert
                                     (행 수가 작아 전량 upsert해도 저렴)

사용 예:
    uv run python -m fetch_data.smp.legacy_sync
"""

from __future__ import annotations

import os
from typing import Optional

import pandas as pd
from sqlalchemy import create_engine, text

from fetch_data.common.db_utils import redact_db_url, resolve_db_url
from fetch_data.common.logger import get_logger
from fetch_data.smp import _common as C
from fetch_data.smp.database import Base

logger = get_logger(__name__)

LEGACY_ENV = "SMP_LEGACY_DB_URL"


def _read_since(src_engine, table: str, since) -> pd.DataFrame:
    """공통 DB에서 since(timestamp) 이후 행을 읽는다. since=None이면 전량."""
    if since is None:
        return pd.read_sql(text(f"SELECT * FROM {table}"), src_engine)
    return pd.read_sql(
        text(f"SELECT * FROM {table} WHERE timestamp > :since"),
        src_engine,
        params={"since": since},
    )


def _sync_table(src_engine, dst_engine, table: str, upsert_fn, ts_keyed: bool) -> int:
    """한 테이블을 공통 DB -> 개인 DB로 증분 upsert."""
    if ts_keyed:
        since = C.get_max_timestamp(dst_engine, table)
        df = _read_since(src_engine, table, since)
    else:
        df = pd.read_sql(text(f"SELECT * FROM {table}"), src_engine)
    if "id" in df.columns:
        df = df.drop(columns=["id"])
    if df.empty:
        logger.info(f"[legacy_sync] {table}: 신규 없음")
        return 0
    # 개인 DB로의 동기화는 CSV 미러를 하지 않는다(CSV는 공통 DB를 대표).
    n = upsert_fn(df, engine=dst_engine, mirror=False)
    logger.info(f"[legacy_sync] {table}: {n}행 동기화")
    return n


def run_legacy_sync() -> int:
    """개인 DB 백업 동기화 실행. 반환: 총 동기화 행수(미설정 시 0)."""
    legacy_url = os.getenv(LEGACY_ENV)
    if not legacy_url:
        logger.info(f"[legacy_sync] {LEGACY_ENV} 미설정 -> 백업 skip")
        return 0

    src_engine = create_engine(resolve_db_url())
    dst_engine = create_engine(legacy_url)
    logger.info(f"[legacy_sync] 대상 개인 DB: {redact_db_url(legacy_url)}")

    # 개인 DB에 SMP 테이블 보장
    Base.metadata.create_all(dst_engine)

    total = 0
    total += _sync_table(src_engine, dst_engine, "smp_hourly", C.upsert_hourly, ts_keyed=True)
    total += _sync_table(
        src_engine, dst_engine, "smp_realtime_jeju", C.upsert_realtime_jeju, ts_keyed=True
    )
    total += _sync_table(
        src_engine, dst_engine, "smp_weighted_avg", C.upsert_weighted_avg, ts_keyed=False
    )
    logger.info(f"[legacy_sync] 완료: 총 {total}행 동기화")
    return total


def main() -> None:
    run_legacy_sync()


if __name__ == "__main__":
    main()
