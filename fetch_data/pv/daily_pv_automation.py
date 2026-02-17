import asyncio
import aiohttp
import json
import pandas as pd
import xml.etree.ElementTree as ET
import os
import re
from pathlib import Path
from datetime import datetime, timedelta, date as date_
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

# 1. 환경 및 DB 설정
current_file = Path(__file__).resolve()
PROJECT_ROOT = current_file.parent.parent.parent
load_dotenv(PROJECT_ROOT / ".env")

# plant.json에서 gencd -> plant_name 매핑 로드 (파일이 없으면 빈 dict)
_PLANT_JSON = PROJECT_ROOT / "plant.json"
GENCD_TO_NAME: dict[str, str] = {}
if _PLANT_JSON.exists():
    with open(_PLANT_JSON, "r", encoding="utf-8") as _f:
        for _p in json.load(_f):
            GENCD_TO_NAME.setdefault(_p["plant_code"], _p["plant_name"])

ENDPOINT = "https://apis.data.go.kr/B552520/PwrSunLightInfo/getDataService"

# lazy 초기화: import 시점이 아닌 실행 시점에 검증
_engine = None


def _get_api_key() -> str:
    key = os.getenv("NAMBU_API_KEY")
    if not key:
        raise RuntimeError("NAMBU_API_KEY가 설정되어 있지 않습니다.")
    return key


def _get_engine():
    global _engine
    if _engine is None:
        db_url = os.getenv("DB_URL") or os.getenv("PV_DATABASE_URL") or os.getenv("LOCAL_DB_URL")
        if not db_url:
            raise RuntimeError("DB_URL(또는 PV_DATABASE_URL/LOCAL_DB_URL)이 설정되어 있지 않습니다.")
        _engine = create_engine(db_url)
    return _engine

def _count_hours_for_day(engine_, gencd: str, hogi: int, day: date_) -> int:
    q = text(
        """
        SELECT COUNT(DISTINCT EXTRACT(HOUR FROM datetime)) AS hours
        FROM nambu_generation
        WHERE gencd = :gencd
          AND hogi = :hogi
          AND datetime >= :day_start
          AND datetime < :day_end
        """
    )
    day_start = datetime.combine(day, datetime.min.time())
    day_end = day_start + timedelta(days=1)
    with engine_.connect() as conn:
        return int(
            conn.execute(
                q,
                {"gencd": gencd, "hogi": hogi, "day_start": day_start, "day_end": day_end},
            ).scalar()
            or 0
        )


def get_active_targets(engine_):
    """
    nambu_generation 테이블을 기준으로 수집 대상을 정하고,
    마지막 기록(또는 미완성 일자)을 확인하여 백필 시작일을 결정합니다.
    """
    query = """
    SELECT
        gencd,
        hogi,
        MAX(datetime) AS last_dt,
        MAX(plant_name) AS plant_name
    FROM nambu_generation
    GROUP BY gencd, hogi
    """
    with engine_.connect() as conn:
        df = pd.read_sql(text(query), conn)
    
    active_targets = []
    yesterday = datetime.now() - timedelta(days=1)

    for _, row in df.iterrows():
        gencd = str(row["gencd"]).strip()
        hogi = int(row["hogi"])
        plant_name = row.get("plant_name")
        last_dt = row["last_dt"]
        
        # 필터링: 마지막 기록이 2025년 이전이면 발전 중단으로 간주하여 스킵
        if last_dt and last_dt.year < 2025:
            print(f"⏩ {gencd} ({hogi}호기): {last_dt.year}년 이후 기록 없음. 수집 제외.")
            continue
            
        # 시작 날짜 결정:
        # - 마지막 날짜의 시간 데이터가 24개 미만이면 그 날짜부터 재수집(해당 일자 데이터 replace)
        # - 아니면 다음 날부터 수집
        if last_dt:
            last_day = last_dt.date()
            hours = _count_hours_for_day(engine_, gencd, hogi, last_day)
            if hours < 24:
                start_dt = datetime.combine(last_day, datetime.min.time())
            else:
                start_dt = datetime.combine(last_day + timedelta(days=1), datetime.min.time())
        else:
            start_dt = datetime.now() - timedelta(days=365)
        
        if start_dt.date() <= yesterday.date():
            active_targets.append({
                "gencd": gencd,
                "hogi": hogi,
                "plant_name": plant_name,
                "start_dt": start_dt,
            })
            
    print(f"✅ 총 {len(active_targets)}개 발전소가 활성 상태이며 수집 대상입니다.")
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
    except Exception:
        return None

def _extract_hour0(col: str) -> int:
    # qhorgen01 -> 0, qhorgen24 -> 23
    m = re.search(r"(\d+)$", col)
    if not m:
        raise ValueError(f"시간 컬럼 파싱 실패: {col}")
    hour_index = int(m.group(1))
    return hour_index - 1


async def collect_and_save(engine_, targets):
    """활성 발전소들에 대해 API 데이터를 수집하고 전처리하여 DB에 저장"""
    yesterday = datetime.now() - timedelta(days=1)
    total_rows = 0

    async with aiohttp.ClientSession() as session:
        for target in targets:
            date_list = pd.date_range(start=target["start_dt"].date(), end=yesterday.date()).strftime("%Y%m%d").tolist()
            if not date_list: continue

            print(f"📡 {target.get('plant_name') or target['gencd']} ({target['hogi']}호기) {len(date_list)}일분 수집 중...")
            
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
                
                df_long["hour0"] = df_long["h_str"].apply(_extract_hour0).astype(int)
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

                with engine_.begin() as conn:
                    # 같은 날짜 데이터가 이미 있으면 replace(특히 마지막 날짜가 미완성인 케이스)
                    for day in sorted(final_df["datetime"].dt.date.unique()):
                        day_start = datetime.combine(day, datetime.min.time())
                        day_end = day_start + timedelta(days=1)
                        conn.execute(
                            text(
                                """
                                DELETE FROM nambu_generation
                                WHERE gencd = :gencd
                                  AND hogi = :hogi
                                  AND datetime >= :day_start
                                  AND datetime < :day_end
                                """
                            ),
                            {
                                "gencd": target["gencd"],
                                "hogi": int(target["hogi"]),
                                "day_start": day_start,
                                "day_end": day_end,
                            },
                        )

                    final_df.to_sql("nambu_generation", con=conn, if_exists="append", index=False)
                
                total_rows += len(final_df)
                print(f"   ㄴ ✅ {len(final_df)}행 저장 완료")

    return total_rows

def solar_automation_flow():
    engine = _get_engine()
    # 1. 수집 대상 분석
    targets = get_active_targets(engine)

    # 2. 데이터 수집 및 저장
    if targets:
        asyncio.run(collect_and_save(engine, targets))
    else:
        print("☀️ 모든 발전소가 최신 상태입니다.")

if __name__ == "__main__":
    solar_automation_flow()
