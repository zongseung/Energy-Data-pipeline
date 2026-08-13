"""SMP 모듈 공통 헬퍼: 값 파싱, upsert, CSV 미러, 증분 시작점 계산."""

from __future__ import annotations

import math
from datetime import datetime
from pathlib import Path
from typing import List, Optional

import pandas as pd
from sqlalchemy import create_engine, text

from fetch_data.common.db_utils import resolve_db_url
from fetch_data.common.logger import get_logger

logger = get_logger(__name__)

BATCH_SIZE = 5000

# CSV 미러 저장 디렉터리 (다른 데이터처럼 프로젝트 루트의 <name>_data 폴더)
PROJECT_ROOT = Path(__file__).resolve().parents[2]
SMP_DATA_DIR = PROJECT_ROOT / "smp_data"

# 테이블별 CSV 정렬 키 (가독성/일관성)
_CSV_ORDER = {
    "smp_hourly": "order by region, timestamp",
    "smp_weighted_avg": "order by period_type, region, price_type, period",
    "smp_realtime_jeju": "order by region, timestamp",
}


def mirror_table_to_csv(engine, table: str) -> Optional[Path]:
    """DB 테이블 전체를 smp_data/<table>.csv 로 덮어쓴다(DB와 항상 일치).

    id 컬럼은 제외(자동 증가라 의미 없음). 실패해도 예외를 올리지 않고
    경고만 남긴다(CSV 미러는 보조 산출물이라 적재 자체를 막지 않음).
    """
    try:
        SMP_DATA_DIR.mkdir(parents=True, exist_ok=True)
        order = _CSV_ORDER.get(table, "")
        df = pd.read_sql(text(f"select * from {table} {order}"), engine)
        if "id" in df.columns:
            df = df.drop(columns=["id"])
        out = SMP_DATA_DIR / f"{table}.csv"
        df.to_csv(out, index=False, encoding="utf-8-sig")
        logger.info(f"[CSV] {table} -> {out} ({len(df)}행)")
        return out
    except Exception as e:  # noqa: BLE001
        logger.warning(f"[CSV] {table} 미러 실패(적재는 정상): {e}")
        return None


def parse_price(raw: Optional[str]) -> Optional[float]:
    """KPX 셀 텍스트를 float로 변환. 숫자가 아니면(플레이스홀더 등) None."""
    if raw is None:
        return None
    s = str(raw).replace(",", "").strip()
    if not s:
        return None
    try:
        value = float(s)
        return value if math.isfinite(value) else None
    except ValueError:
        return None  # "확정가격은 D+1일..." 같은 플레이스홀더


def get_engine_for(db_url: Optional[str] = None):
    """적재용 엔진(도커/호스트 자동 전환). seobu/wind와 동일 헬퍼 사용."""
    resolved = resolve_db_url(db_url)
    if not resolved:
        raise RuntimeError("DB_URL이 설정되지 않았습니다.")
    return create_engine(resolved)


def _upsert(engine, sql: str, records: List[dict], label: str) -> int:
    """배치 upsert 공통 루틴."""
    if not records:
        logger.info(f"[DB] {label}: 적재할 데이터 없음")
        return 0
    stmt = text(sql)
    total = 0
    with engine.begin() as conn:
        for i in range(0, len(records), BATCH_SIZE):
            batch = records[i : i + BATCH_SIZE]
            conn.execute(stmt, batch)
            total += len(batch)
    logger.info(f"[DB] {label}: {total}행 upsert 완료")
    return total


def upsert_hourly(df: pd.DataFrame, db_url: Optional[str] = None, engine=None, mirror: bool = True) -> int:
    """smp_hourly upsert. df 컬럼: timestamp, region, price."""
    if df.empty:
        return 0
    engine = engine or get_engine_for(db_url)
    sql = """
        INSERT INTO smp_hourly (timestamp, region, price)
        VALUES (:timestamp, :region, :price)
        ON CONFLICT (timestamp, region)
        DO UPDATE SET price = EXCLUDED.price
    """
    records = df[["timestamp", "region", "price"]].to_dict("records")
    n = _upsert(engine, sql, records, "smp_hourly")
    if n and mirror:
        mirror_table_to_csv(engine, "smp_hourly")
    return n


def upsert_weighted_avg(df: pd.DataFrame, db_url: Optional[str] = None, engine=None, mirror: bool = True) -> int:
    """smp_weighted_avg upsert. df 컬럼: period_type, period, region, price_type, weighted_avg.

    price_type 미지정 시 'smp'로 채운다(하위호환).
    """
    if df.empty:
        return 0
    engine = engine or get_engine_for(db_url)
    if "price_type" not in df.columns:
        df = df.copy()
        df["price_type"] = "smp"
    sql = """
        INSERT INTO smp_weighted_avg
            (period_type, period, region, price_type, weighted_avg)
        VALUES (:period_type, :period, :region, :price_type, :weighted_avg)
        ON CONFLICT (period_type, period, region, price_type)
        DO UPDATE SET weighted_avg = EXCLUDED.weighted_avg
    """
    records = df[
        ["period_type", "period", "region", "price_type", "weighted_avg"]
    ].to_dict("records")
    n = _upsert(engine, sql, records, "smp_weighted_avg")
    if n and mirror:
        mirror_table_to_csv(engine, "smp_weighted_avg")
    return n


def upsert_realtime_jeju(df: pd.DataFrame, db_url: Optional[str] = None, engine=None, mirror: bool = True) -> int:
    """smp_realtime_jeju upsert. df 컬럼: timestamp, region, price, is_confirmed."""
    if df.empty:
        return 0
    engine = engine or get_engine_for(db_url)
    sql = """
        INSERT INTO smp_realtime_jeju (timestamp, region, price, is_confirmed)
        VALUES (:timestamp, :region, :price, :is_confirmed)
        ON CONFLICT (timestamp, region)
        DO UPDATE SET
            price = EXCLUDED.price,
            is_confirmed = EXCLUDED.is_confirmed
    """
    records = df[["timestamp", "region", "price", "is_confirmed"]].to_dict("records")
    n = _upsert(engine, sql, records, "smp_realtime_jeju")
    if n and mirror:
        mirror_table_to_csv(engine, "smp_realtime_jeju")
    return n


def get_max_timestamp(engine, table: str, region: Optional[str] = None) -> Optional[datetime]:
    """테이블(또는 region)의 최신 timestamp를 반환. 없으면 None."""
    q = f"SELECT max(timestamp) FROM {table}"
    params = {}
    if region is not None:
        q += " WHERE region = :region"
        params["region"] = region
    try:
        with engine.connect() as conn:
            return conn.execute(text(q), params).scalar()
    except Exception as e:  # noqa: BLE001  (테이블 미생성 등)
        logger.info(f"[DB] {table} max(timestamp) 조회 실패(테이블 없음 가능): {e}")
        return None
