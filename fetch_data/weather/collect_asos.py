# 해당 파일의 localhost 주소는  http://localhost:4300 임.

import asyncio
import os
import sys
from pathlib import Path

import aiohttp
import pandas as pd
from dotenv import load_dotenv

# ===== 경로 / 모듈 세팅 =====

# 이 파일 경로: /app/fetch_data/weather/collect_asos.py
# 프로젝트 루트: /app
PROJECT_ROOT = Path(__file__).resolve().parents[2]

# fetch_data 모듈 임포트 가능하게 상위 디렉토리 추가
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from fetch_data.common.impute_missing import impute_missing_values
from prefect_flows.merge_to_all import merge_to_all_csv, DATA_DIR  # ★ 여기!

# ===== 환경변수 / API 키 =====

# .env 파일 로드 (.env는 프로젝트 루트(/app)에 있다고 가정)
load_dotenv(PROJECT_ROOT / ".env")
SERVICE_KEY = os.getenv("SERVICE_KEY")

if not SERVICE_KEY:
    raise RuntimeError("SERVICE_KEY 환경변수가 설정되어 있지 않습니다. .env 를 확인하세요.")

API_URL = "https://apis.data.go.kr/1360000/AsosHourlyInfoService/getWthrDataList"

# ===== 지점 목록 CSV 로드 =====
# station_list.csv 파일 읽기에 지속적으로 문제가 발생하여, station_ids를 하드코딩합니다.
# 이는 station_list.csv 파일 처리 과정의 문제를 우회하고 다음 단계를 진행하기 위함입니다.
station_ids = [
    '90', '93', '95', '98', '99', '100', '101', '104', '105', '106', '108', '112', '114',
    '119', '121', '127', '129', '130', '131', '133', '135', '136', '137', '138', '140',
    '143', '146', '152', '155', '156', '159', '162', '165', '168', '170', '172', '174',
    '177', '192', '201', '202', '203', '211'
]

# 일별 결과 저장 디렉토리 (data/ 아래)
OUTPUT_DIR = DATA_DIR  # 그냥 data/ 바로 아래에 저장
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


# ===== API 호출 함수들 =====

async def fetch_city(session, city_id, start, end, max_retries: int = 3) -> pd.DataFrame:
    """단일 지점 데이터를 비동기 수집 (실패 시 5초 후 재시도)"""
    params = {
        "serviceKey": SERVICE_KEY,
        "pageNo": "1",
        "numOfRows": "999",
        "dataType": "JSON",
        "dataCd": "ASOS",
        "dateCd": "HR",
        "startDt": start,
        "startHh": "00",
        "endDt": end,
        "endHh": "23",
        "stnIds": city_id,
    }

    for attempt in range(1, max_retries + 1):
        try:
            async with session.get(API_URL, params=params) as response:
                if response.status == 200:
                    data = await response.json()

                    # API 에러 응답 체크
                    header = data.get("response", {}).get("header", {})
                    result_code = header.get("resultCode", "")
                    result_msg = header.get("resultMsg", "")

                    if result_code != "00":
                        print(f"{city_id}: API 에러 - 코드={result_code}, 메시지={result_msg}")
                        return pd.DataFrame()

                    items = (
                        data.get("response", {})
                        .get("body", {})
                        .get("items", {})
                        .get("item", [])
                    )
                    if items:
                        print(f"{city_id}: 데이터 {len(items)}건 수집 완료")
                        return pd.DataFrame(items)
                    else:
                        print(f"{city_id}: 데이터 없음 (날짜: {start}~{end})")
                        return pd.DataFrame()
                else:
                    print(
                        f"{city_id}: 요청 실패 (시도 {attempt}/{max_retries}) "
                        f"상태코드={response.status}"
                    )
        except Exception as e:
            print(f"{city_id}: 예외 발생 (시도 {attempt}/{max_retries}) → {e}")

        # 재시도 전 5초 대기
        if attempt < max_retries:
            print(f"{city_id}: 5초 후 재시도...")
            await asyncio.sleep(5)

    print(f"{city_id}: {max_retries}회 실패 — 포기")
    return pd.DataFrame()


async def select_data_async(city_list, start: str, end: str) -> pd.DataFrame:
    """모든 지점 데이터를 비동기로 수집"""
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_city(session, city, start, end) for city in city_list]
        results = await asyncio.gather(*tasks)

    # 비어있는 DF가 섞여 있어도 concat 가능하게 필터링
    results = [r for r in results if not r.empty]
    if not results:
        print("수집된 데이터가 없습니다.")
        return pd.DataFrame()

    return pd.concat(results, ignore_index=True)


# ===== 스크립트 직접 실행 시 =====

if __name__ == "__main__":
    # 입력 예: 20250101,20250131
    raw = input("날짜를 입력하세요, start, end = 'YYYYMMDD,YYYYMMDD' : ")
    start, end = raw.split(",")
    start = start.strip()
    end = end.strip()

    # 메인 비동기 루프 실행
    df = asyncio.run(select_data_async(station_ids, start, end))

    if df.empty:
        print("다운로드된 데이터가 없어 종료합니다.")
        sys.exit(0)

    # 원하는 컬럼만 남기기
    df = df[["tm", "hm", "ta", "stnNm"]]

    # 결측치 처리 (원본 컬럼명 사용: ta, hm, tm, stnNm)
    print("\n결측치 처리 중...")
    result = impute_missing_values(
        df,
        columns=["ta", "hm"],
        date_col="tm",
        station_col="stnNm",
        debug=True,
    )
    if isinstance(result, tuple):
        df, debug_info = result
    else:
        df = result

    # 컬럼 이름 변경
    df.rename(
        columns={
            "tm": "date",          # 실제로는 datetime(시각 포함)
            "hm": "humidity",
            "ta": "temperature",
            "stnNm": "station_name",
        },
        inplace=True,
    )

    # 일별 CSV 저장 (컨테이너 기준: /app/data/asos_YYYYMMDD_YYYYMMDD.csv)
    daily_path = OUTPUT_DIR / f"asos_{start}_{end}.csv"
    df.to_csv(daily_path, index=False, encoding="utf-8-sig")
    print(f"\n일별 파일 저장 완료: {daily_path}")

    # 누적 CSV에 머지 (컨테이너 기준: /app/data/asos_all_merged.csv)
    merged_path = merge_to_all_csv(daily_path)
    print(f"누적 파일 갱신 완료: {merged_path}")

    print("\n샘플 데이터:")
    print(df.head())
