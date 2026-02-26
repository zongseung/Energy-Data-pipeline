# 해당 파일의 localhost 주소는  http://localhost:4300 임.

import asyncio
import sys
from pathlib import Path

import aiohttp
import pandas as pd

from fetch_data.common.config import get_service_key
from fetch_data.common.logger import get_logger
from fetch_data.common.impute_missing import impute_missing_values
from fetch_data.constants import WeatherAPI
from prefect_flows.merge_to_all import merge_to_all_csv, DATA_DIR

logger = get_logger(__name__)

# ===== 경로 / 모듈 세팅 =====
# 이 파일 경로: /app/fetch_data/weather/collect_asos.py
# 프로젝트 루트: /app

# ===== 환경변수 / API 키 =====
SERVICE_KEY = get_service_key()

if not SERVICE_KEY:
    logger.warning("[WARN] SERVICE_KEY 환경변수가 설정되지 않았습니다. 기상 데이터 수집 시 오류가 발생합니다.")

API_URL = WeatherAPI.ENDPOINT

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
                        logger.warning(f"{city_id}: API 에러 - 코드={result_code}, 메시지={result_msg}")
                        return pd.DataFrame()

                    items = (
                        data.get("response", {})
                        .get("body", {})
                        .get("items", {})
                        .get("item", [])
                    )
                    if items:
                        logger.info(f"{city_id}: 데이터 {len(items)}건 수집 완료")
                        return pd.DataFrame(items)
                    else:
                        logger.info(f"{city_id}: 데이터 없음 (날짜: {start}~{end})")
                        return pd.DataFrame()
                else:
                    logger.warning(
                        f"{city_id}: 요청 실패 (시도 {attempt}/{max_retries}) "
                        f"상태코드={response.status}"
                    )
        except Exception as e:
            logger.error(f"{city_id}: 예외 발생 (시도 {attempt}/{max_retries}) -> {e}")

        # 재시도 전 5초 대기
        if attempt < max_retries:
            logger.info(f"{city_id}: 5초 후 재시도...")
            await asyncio.sleep(5)

    logger.error(f"{city_id}: {max_retries}회 실패 — 포기")
    return pd.DataFrame()


async def select_data_async(city_list, start: str, end: str) -> pd.DataFrame:
    """모든 지점 데이터를 비동기로 수집"""
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_city(session, city, start, end) for city in city_list]
        results = await asyncio.gather(*tasks)

    # 비어있는 DF가 섞여 있어도 concat 가능하게 필터링
    results = [r for r in results if not r.empty]
    if not results:
        logger.warning("수집된 데이터가 없습니다.")
        return pd.DataFrame()

    return pd.concat(results, ignore_index=True)


# ===== 스크립트 직접 실행 시 =====

if __name__ == "__main__":
    import argparse
    from fetch_data.common.utils import today_kst

    parser = argparse.ArgumentParser(description="ASOS 기상 데이터 수집")
    parser.add_argument("--start", required=True, help="시작일 (YYYYMMDD)")
    parser.add_argument("--end", required=True, help="종료일 (YYYYMMDD)")
    args = parser.parse_args()
    start = args.start.strip()
    end = args.end.strip()

    # 메인 비동기 루프 실행
    df = asyncio.run(select_data_async(station_ids, start, end))

    if df.empty:
        logger.warning("다운로드된 데이터가 없어 종료합니다.")
        sys.exit(0)

    # 원하는 컬럼만 남기기
    df = df[["tm", "hm", "ta", "stnNm"]]

    # 결측치 처리 (원본 컬럼명 사용: ta, hm, tm, stnNm)
    logger.info("결측치 처리 중...")
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
    logger.info(f"일별 파일 저장 완료: {daily_path}")

    # 누적 CSV에 머지 (컨테이너 기준: /app/data/asos_all_merged.csv)
    merged_path = merge_to_all_csv(daily_path)
    logger.info(f"누적 파일 갱신 완료: {merged_path}")

    logger.info("샘플 데이터:")
    logger.info(str(df.head()))
