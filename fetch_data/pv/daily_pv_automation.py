import asyncio
import aiohttp
import pandas as pd
import xml.etree.ElementTree as ET
import os
from pathlib import Path
from datetime import datetime, timedelta
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from prefect import flow, task
import os
from dotenv import load_dotenv

load_dotenv()

# 1. 환경 및 DB 설정
current_file = Path(__file__).resolve()
PROJECT_ROOT = current_file.parent.parent.parent # 폴더 구조에 맞춰 상위 폴더 지정
load_dotenv(PROJECT_ROOT / ".env")

API_KEY = os.getenv("NAMBU_API_KEY")
ENDPOINT = "https://apis.data.go.kr/B552520/PwrSunLightInfo/getDataService"
DB_URL = os.getenv("DB_URL")

engine = create_engine(DB_URL)

# --- [Task 1: 수집 대상 및 시작 시점 분석] ---
@task(name="Get Active Collection Targets", log_prints=True)
def get_active_targets():
    """
    plant_info 테이블을 기준으로 수집 대상을 정하고,
    pv_generation의 마지막 기록을 확인하여 가동 중단 여부와 시작일을 결정합니다.
    """
    query = """
    SELECT 
        p.gencd, 
        p.hogi, 
        p.ipptnm, 
        MAX(g.timestamp) as last_date
    FROM plant_info p
    LEFT JOIN pv_generation g ON p.ipptnm = g.ipptnm AND p.hogi = g.hogi
    GROUP BY p.gencd, p.hogi, p.ipptnm
    """
    with engine.connect() as conn:
        df = pd.read_sql(text(query), conn)
    
    active_targets = []
    yesterday = datetime.now() - timedelta(days=1)

    for _, row in df.iterrows():
        last_date = row['last_date']
        
        # 필터링: 마지막 기록이 2025년 이전이면 발전 중단으로 간주하여 스킵
        if last_date and last_date.year < 2025:
            print(f"⏩ {row['ipptnm']} ({row['hogi']}호기): {last_date.year}년 이후 기록 없음. 수집 제외.")
            continue
            
        # 시작 날짜 결정 (마지막 기록 다음날 혹은 데이터 없으면 1년 전)
        start_dt = (last_date + timedelta(days=1)) if last_date else (datetime.now() - timedelta(days=365))
        
        if start_dt.date() <= yesterday.date():
            active_targets.append({
                'gencd': str(row['gencd']),
                'hogi': str(row['hogi']),
                'ipptnm': row['ipptnm'],
                'start_dt': start_dt
            })
            
    print(f"✅ 총 {len(active_targets)}개 발전소가 활성 상태이며 수집 대상입니다.")
    return active_targets

# --- [Task 2: API 데이터 수집 및 전처리] ---
async def fetch_api_data(session, date_str, gencd, hogi):
    """API 호출 (strHoki 파라미터 사용)"""
    params = {
        "serviceKey": API_KEY, "pageNo": "1", "numOfRows": "100",
        "strSdate": date_str, "strEdate": date_str,
        "strOrgCd": gencd, "strHoki": hogi
    }
    try:
        async with session.get(ENDPOINT, params=params, timeout=15) as response:
            if response.status != 200: return None
            root = ET.fromstring(await response.text())
            items = root.find('.//items')
            return {child.tag: child.text for child in items} if items is not None else None
    except:
        return None

@task(name="Collect and Save Solar Data", log_prints=True)
async def collect_and_save(targets):
    """활성 발전소들에 대해 API 데이터를 수집하고 전처리하여 DB에 저장"""
    yesterday = datetime.now() - timedelta(days=1)
    total_rows = 0

    async with aiohttp.ClientSession() as session:
        for target in targets:
            date_list = pd.date_range(start=target['start_dt'], end=yesterday).strftime("%Y%m%d").tolist()
            if not date_list: continue

            print(f"📡 {target['ipptnm']} ({target['hogi']}호기) {len(date_list)}일분 수집 중...")
            
            raw_data = []
            for d_str in date_list:
                data = await fetch_api_data(session, d_str, target['gencd'], target['hogi'])
                if data:
                    data['gencd'], data['hogi'] = target['gencd'], target['hogi']
                    raw_data.append(data)
                await asyncio.sleep(0.05)

            if raw_data:
                # 전처리: 24시간 데이터를 세로로 변환
                df_raw = pd.DataFrame(raw_data)
                v_vars = [c for c in df_raw.columns if c.startswith('qhorgen')]
                df_long = df_raw.melt(id_vars=['ymd', 'hogi', 'gencd'], value_vars=v_vars, 
                                      var_name='h_str', value_name='generation')
                
                df_long['hour'] = df_long['h_str'].str.extract(r'(\d+)').astype(int)
                df_long['timestamp'] = pd.to_datetime(df_long['ymd']) + pd.to_timedelta(df_long['hour'], unit='h')
                df_long['generation'] = pd.to_numeric(df_long['generation'], errors='coerce').fillna(0)
                df_long['ipptnm'] = target['ipptnm'] # 마스터 테이블 명칭 강제 적용

                final_df = df_long[['timestamp', 'ipptnm', 'hogi', 'generation', 'gencd']]
                
                with engine.begin() as conn:
                    final_df.to_sql('pv_generation', con=conn, if_exists='append', index=False)
                
                total_rows += len(final_df)
                print(f"   ㄴ ✅ {len(final_df)}행 저장 완료")

    return total_rows

# --- [Flow: 전체 워크플로우 제어] ---
@flow(name="Daily Nambu Solar Collection Flow", log_prints=True)
def solar_automation_flow():
    # 1. 수집 대상 분석
    targets = get_active_targets()
    
    # 2. 데이터 수집 및 저장
    if targets:
        asyncio.run(collect_and_save(targets))
    else:
        print("☀️ 모든 발전소가 최신 상태입니다.")

if __name__ == "__main__":
    solar_automation_flow()

    #teset