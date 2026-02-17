import asyncio
import aiohttp
import xml.etree.ElementTree as ET
import os
import pandas as pd
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv

# 1. .env 로드 및 설정
PROJECT_ROOT = Path(__file__).parent.parent.parent
load_dotenv(PROJECT_ROOT / ".env")

API_KEY = os.getenv("NAMBU_API_KEY")
ENDPOINT = "https://apis.data.go.kr/B552520/PwrSunLightInfo/getDataService"

# 2. 코드표 데이터 (image_218246.png 기반)
PLANT_LIST = [
    ("5707", "1"), ("8760", "1"), ("9977", "1"), ("9979", "1"), ("997D", "1"),
    ("997G", "1"), ("997H", "2"), ("997J", "3"), ("997K", "1"), ("997L", "1"),
    ("997M", "1"), ("997N", "1"), ("997Q", "1"), ("997R", "1"), ("997S", "1"),
    ("997V", "1"), ("997Y", "1"), ("9984", "1"), ("9985", "1"), ("9987", "1"),
    ("9988", "1"), ("9989", "1"), ("AC39", "1"), ("B997", "1"), ("B997", "2"),
    ("H997", "1"), ("H997", "2"), ("H997", "3"), ("S997", "1"), ("S997", "2"), ("S997", "3")
] # 

async def check_data_exists(session, target_date, org_cd, hoki):
    """API를 호출하여 해당 날짜에 데이터가 존재하는지 확인합니다."""
    params = {
        "serviceKey": API_KEY, "pageNo": "1", "numOfRows": "1",
        "strSdate": target_date, "strEdate": target_date,
        "strOrgCd": org_cd, "strHoki": hoki
    } # [cite: 2]
    try:
        async with session.get(ENDPOINT, params=params, timeout=10) as response:
            text = await response.text()
            root = ET.fromstring(text)
            ymd = root.find('.//ymd')
            return ymd is not None and ymd.text is not None
    except Exception:
        return False

async def find_start_date(session, org_cd, hoki):
    """특정 발전소의 가장 오래된 데이터 날짜를 추적합니다."""
    found_year = None
    # 1단계: 연도 탐색 (2025 ~ 2010)
    for year in range(2025, 2009, -1):
        exists = False
        for month in [12, 6, 1]: # 각 연도의 분기별로 빠르게 스캔
            if await check_data_exists(session, f"{year}{month:02d}01", org_cd, hoki):
                exists = True; break
        if exists: found_year = year
        else:
            if found_year: break
    
    if not found_year: return "N/A"

    # 2단계: 월 탐색
    first_month = 12
    for m in range(12, 0, -1):
        if await check_data_exists(session, f"{found_year}{m:02d}28", org_cd, hoki): first_month = m
        else: break
    
    # 3단계: 일 탐색
    first_day = 28
    for d in range(28, 0, -1):
        if await check_data_exists(session, f"{found_year}{first_month:02d}{d:02d}", org_cd, hoki): first_day = d
        else: break
        
    return f"{found_year}-{first_month:02d}-{first_day:02d}"

async def main():
    if not API_KEY:
        print("[Error] NAMBU_API_KEY가 없습니다."); return

    results = []
    async with aiohttp.ClientSession() as session:
        print(f"--- 총 {len(PLANT_LIST)}개 발전소 기점 조사 시작 ---")
        for org_cd, hoki in PLANT_LIST: # 
            print(f"조사 중: [{org_cd}] {hoki}호기...", end=" ", flush=True)
            start_date = await find_start_date(session, org_cd, hoki)
            print(f"결과: {start_date}")
            results.append({"발전소코드": org_cd, "호기": hoki, "시작일": start_date})
            await asyncio.sleep(0.2) # API 과부하 방지

    # CSV 저장
    df = pd.DataFrame(results)
    out_path = PROJECT_ROOT / "pv_start_dates.csv"
    df.to_csv(out_path, index=False, encoding="utf-8-sig")
    print(f"\n✅ 조사가 완료되었습니다! 파일 저장 위치: {out_path}")

if __name__ == "__main__":
    asyncio.run(main())

    