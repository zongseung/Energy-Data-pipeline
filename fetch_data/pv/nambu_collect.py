import asyncio
import aiohttp
import json
import pandas as pd
import xml.etree.ElementTree as ET
from pathlib import Path
from datetime import datetime, timedelta
from sqlalchemy import create_engine

from fetch_data.common.config import get_nambu_api_key
from fetch_data.common.db_utils import resolve_db_url
from fetch_data.common.logger import get_logger
from fetch_data.common.utils import now_kst, parse_hour_column
from fetch_data.constants import NamebuAPI
from fetch_data.common.generation_core import upsert_generation
from fetch_data.pv.nambu_state import (
    collection_start,
    count_hours_for_day,
    get_nambu_targets,
)

logger = get_logger(__name__)

# 1. 환경 및 DB 설정
current_file = Path(__file__).resolve()
PROJECT_ROOT = current_file.parent.parent.parent

# plant.json에서 gencd -> plant_name 매핑 로드 (파일이 없으면 빈 dict)
_PLANT_JSON = PROJECT_ROOT / "config" / "plant.json"
GENCD_TO_NAME: dict[str, str] = {}
if _PLANT_JSON.exists():
    with open(_PLANT_JSON, "r", encoding="utf-8") as _f:
        for _p in json.load(_f):
            GENCD_TO_NAME.setdefault(_p["plant_code"], _p["plant_name"])

ENDPOINT = NamebuAPI.ENDPOINT

# lazy 초기화: import 시점이 아닌 실행 시점에 검증
_engine = None


def _get_api_key() -> str:
    key = get_nambu_api_key()
    if not key:
        raise RuntimeError("NAMBU_API_KEY가 설정되어 있지 않습니다.")
    return key


def _get_engine():
    global _engine
    if _engine is None:
        _engine = create_engine(resolve_db_url())
    return _engine

def get_active_targets(engine_):
    """Core 테이블의 마지막 기록을 기준으로 수집 시작일을 결정합니다."""
    active_targets = []
    today = now_kst().date()
    yesterday = today - timedelta(days=1)

    for target in get_nambu_targets(engine_):
        last_dt = target["last_dt"]
        hours = count_hours_for_day(engine_, target["plant_id"], last_dt.date()) if last_dt else 0
        start_dt = collection_start(last_dt, hours, today)
        if start_dt is None:
            logger.info(
                f"{target['gencd']} ({target['hogi']}호기): "
                f"{last_dt.year}년 이후 기록 없음. 수집 제외."
            )
            continue

        if start_dt.date() <= yesterday:
            active_targets.append({**target, "start_dt": start_dt})

    logger.info(f"총 {len(active_targets)}개 발전소가 활성 상태이며 수집 대상입니다.")
    return active_targets

# --- [Task 2: API 데이터 수집 및 전처리] ---
async def fetch_api_data(session, date_str, gencd, hogi):
    """API 호출 (strHoki 파라미터 사용)"""
    params = {
        "serviceKey": _get_api_key(), "pageNo": "1", "numOfRows": "100",
        "strSdate": date_str, "strEdate": date_str,
        "strOrgCd": gencd, "strHoki": str(hogi)
    }
    try:
        async with session.get(ENDPOINT, params=params, timeout=15) as response:
            if response.status != 200: return None
            root = ET.fromstring(await response.text())
            items = root.find('.//items')
            return {child.tag: child.text for child in items} if items is not None else None
    except Exception as e:
        logger.error(f"API 호출 실패: {e}")
        return None

async def collect_and_save(engine_, targets):
    """활성 발전소들에 대해 API 데이터를 수집하고 전처리하여 DB에 저장"""
    yesterday = now_kst().replace(tzinfo=None) - timedelta(days=1)
    total_rows = 0

    async with aiohttp.ClientSession() as session:
        for target in targets:
            date_list = pd.date_range(start=target["start_dt"].date(), end=yesterday.date()).strftime("%Y%m%d").tolist()
            if not date_list: continue

            logger.info(f"{target.get('plant_name') or target['gencd']} ({target['hogi']}호기) {len(date_list)}일분 수집 중...")

            raw_data = []
            for d_str in date_list:
                data = await fetch_api_data(session, d_str, target["gencd"], target["hogi"])
                if data:
                    data["gencd"], data["hogi"] = target["gencd"], str(target["hogi"])
                    raw_data.append(data)
                await asyncio.sleep(0.05)

            if raw_data:
                # 전처리: 24시간 데이터를 세로로 변환
                df_raw = pd.DataFrame(raw_data)
                v_vars = [c for c in df_raw.columns if c.startswith('qhorgen')]
                df_long = df_raw.melt(id_vars=["ymd", "hogi", "gencd", "ipptnm", "qvodgen", "qvodavg", "qvodmax", "qvodmin"], value_vars=v_vars,
                                      var_name='h_str', value_name='generation')

                df_long["hour0"] = df_long["h_str"].apply(parse_hour_column).astype(int)
                df_long["datetime"] = pd.to_datetime(df_long["ymd"]) + pd.to_timedelta(df_long["hour0"], unit="h")
                df_long["generation"] = pd.to_numeric(df_long["generation"], errors="coerce").fillna(0)
                df_long["daily_total"] = pd.to_numeric(df_long["qvodgen"], errors="coerce")
                df_long["daily_avg"] = pd.to_numeric(df_long["qvodavg"], errors="coerce")
                df_long["daily_max"] = pd.to_numeric(df_long["qvodmax"], errors="coerce")
                df_long["daily_min"] = pd.to_numeric(df_long["qvodmin"], errors="coerce")
                df_long["plant_name"] = df_long["ipptnm"]
                # API 응답에 ipptnm이 없는 경우 plant.json 매핑으로 대체
                if GENCD_TO_NAME:
                    mask_na = df_long["plant_name"].isna() | (df_long["plant_name"].astype(str) == "None")
                    df_long.loc[mask_na, "plant_name"] = df_long.loc[mask_na, "gencd"].map(GENCD_TO_NAME)
                df_long["hogi"] = pd.to_numeric(df_long["hogi"], errors="coerce").astype("Int64")

                final_df = df_long[
                    [
                        "datetime",
                        "gencd",
                        "plant_name",
                        "hogi",
                        "generation",
                        "daily_total",
                        "daily_avg",
                        "daily_max",
                        "daily_min",
                    ]
                ].dropna(subset=["datetime", "gencd", "hogi"])

                # 신규 코어(plants/generation)에 직접 UPSERT
                core_df = final_df.rename(
                    columns={"datetime": "timestamp", "gencd": "plant_code", "hogi": "unit_no"}
                )[["timestamp", "plant_name", "unit_no", "plant_code", "generation"]].copy()
                core_df["plant_name"] = target["plant_name"]
                upsert_generation(core_df, operator="nambu", fuel_type="solar", engine=engine_)

                total_rows += len(final_df)
                logger.info(f"   -> {len(final_df)}행 저장 완료")

    return total_rows

def solar_automation_flow():
    engine = _get_engine()
    # 1. 수집 대상 분석
    targets = get_active_targets(engine)

    # 2. 데이터 수집 및 저장
    if targets:
        asyncio.run(collect_and_save(engine, targets))
    else:
        logger.info("모든 발전소가 최신 상태입니다.")

if __name__ == "__main__":
    solar_automation_flow()
