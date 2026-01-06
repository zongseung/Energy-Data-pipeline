import asyncio
import aiohttp
import pandas as pd
import xml.etree.ElementTree as ET
import os
from pathlib import Path
from datetime import datetime, timedelta
from dotenv import load_dotenv
from tqdm import tqdm

# 1. 환경 설정
PROJECT_ROOT = Path(__file__).parent.parent.parent
load_dotenv(PROJECT_ROOT / ".env")

API_KEY = os.getenv("NAMBU_API_KEY")
ENDPOINT = "https://apis.data.go.kr/B552520/PwrSunLightInfo/getDataService"
INPUT_CSV = PROJECT_ROOT / "pv_start_dates.csv"
OUTPUT_DIR = PROJECT_ROOT / "pv_data_raw"

# ⭐ 오늘 완료한 발전소 제외 리스트 (발전소코드, 호기)

EXCLUDE_LIST = []

def get_missing_dates(file_path, start_date_str, end_date):
    """파일을 읽어 시작일부터 종료일까지 빠진 날짜 리스트를 반환"""
    start_dt = datetime.strptime(start_date_str, "%Y-%m-%d")
    full_range = pd.date_range(start=start_dt, end=end_date)
    expected_dates = set(full_range.strftime("%Y%m%d"))
    
    if not file_path.exists():
        return sorted(list(expected_dates))
    
    try:
        df = pd.read_csv(file_path, usecols=['ymd'])
        existing_dates = set(df['ymd'].astype(str).str.replace("-", "").str.strip())
        missing_dates = expected_dates - existing_dates
        return sorted(list(missing_dates))
    except Exception as e:
        print(f" [!] {file_path.name} 읽기 오류: {e}")
        return sorted(list(expected_dates))

async def fetch_nambu_data(session, date_str, org_cd, hoki):
    params = {
        "serviceKey": API_KEY, "pageNo": "1", "numOfRows": "100",
        "strSdate": date_str, "strEdate": date_str,
        "strOrgCd": org_cd, "strHoki": hoki
    }
    try:
        async with session.get(ENDPOINT, params=params, timeout=20) as response:
            if response.status != 200: return None
            text = await response.text()
            root = ET.fromstring(text)
            item = root.find('.//items')
            if item is not None:
                return {child.tag: child.text for child in item}
    except: return None
    return None

async def process_plant(session, org_cd, hoki, start_date_str):
    out_path = OUTPUT_DIR / f"nambu_bulk_{org_cd}_{hoki}.csv"
    yesterday = datetime.now() - timedelta(days=1)
    
    missing_dates = get_missing_dates(out_path, start_date_str, yesterday)
    
    if not missing_dates:
        return

    pbar = tqdm(total=len(missing_dates), desc=f"📡 {org_cd}_{hoki}", unit="day")
    
    for date_str in missing_dates:
        data = await fetch_nambu_data(session, date_str, org_cd, hoki)
        if data:
            df_day = pd.DataFrame([data])
            if not out_path.exists():
                df_day.to_csv(out_path, index=False, encoding="utf-8-sig")
            else:
                df_day.to_csv(out_path, mode='a', header=False, index=False, encoding="utf-8-sig")
        
        pbar.update(1)
        await asyncio.sleep(0.05)

    pbar.close()

async def main():
    if not API_KEY:
        print("[!] NAMBU_API_KEY를 확인하세요.")
        return

    df_targets = pd.read_csv(INPUT_CSV).dropna(subset=['시작일'])
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    
    print(f"\n🔍 총 {len(df_targets)}개 중 제외 대상을 뺀 나머지 수집을 시작합니다.")
    
    async with aiohttp.ClientSession() as session:
        for _, row in df_targets.iterrows():
            org_cd = str(row['발전소코드'])
            hoki = str(row['호기'])
            
            # ⭐ 제외 리스트에 있으면 건너뜀
            if (org_cd, hoki) in EXCLUDE_LIST:
                print(f" ⏩ [{org_cd}_{hoki}]는 이미 완료되어 건너뜁니다.")
                continue
                
            await process_plant(session, org_cd, hoki, str(row['시작일']))

    print("\n🎉 모든 작업이 완료되었습니다!")

if __name__ == "__main__":
     asyncio.run(main())