# 해당 파일의 localhost 주소는  http://localhost:4300 임.

import asyncio
import sys
from pathlib import Path

import aiohttp
import pandas as pd

from fetch_data.common.config import get_service_key
from fetch_data.common.logger import get_logger
from fetch_data.constants import WeatherAPI
from prefect_flows.merge_to_all import merge_to_all_csv, DATA_DIR

logger = get_logger(__name__)

# ===== 경로 / 모듈 세팅 =====
# 이 파일 경로: /app/fetch_data/weather/collect_asos.py
# 프로젝트 루트: /app

# ===== 환경변수 / API 키 =====
if not get_service_key():
    logger.warning("[WARN] SERVICE_KEY 환경변수가 설정되지 않았습니다. 기상 데이터 수집 시 오류가 발생합니다.")

API_URL = WeatherAPI.ENDPOINT

# ===== 지점 목록 CSV 로드 =====
PROJECT_ROOT = Path(__file__).resolve().parents[2]  # fetch_data/weather/collect_asos.py -> 프로젝트 루트
STATION_CSV = PROJECT_ROOT / "config" / "station_list.csv"


def load_station_ids() -> list[str]:
    """station_list.csv에서 지점 코드를 읽어옵니다."""
    if not STATION_CSV.exists():
        logger.error(f"지점 목록 파일이 없습니다: {STATION_CSV}")
        raise FileNotFoundError(f"station_list.csv not found: {STATION_CSV}")

    df = pd.read_csv(STATION_CSV, dtype=str)
    ids = df["지점"].str.strip().tolist()
    logger.info(f"station_list.csv에서 {len(ids)}개 지점 로드 완료")
    return ids


station_ids = load_station_ids()

# 일별 결과 저장 디렉토리 (data/ 아래)
OUTPUT_DIR = DATA_DIR  # 그냥 data/ 바로 아래에 저장
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


# ===== API 호출 함수들 =====

# 공공데이터포털 API rate limit 대응: 동시 요청 수 제한
MAX_CONCURRENT = 10


async def fetch_city(
    session, city_id, start, end, semaphore: asyncio.Semaphore, service_key: str, max_retries: int = 3
) -> pd.DataFrame:
    """단일 지점 데이터를 비동기 수집 (semaphore로 동시 요청 제한, 429 시 재시도)"""
    params = {
        "serviceKey": service_key,
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
        is_rate_limited = False
        async with semaphore:
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

                    elif response.status == 429:
                        is_rate_limited = True
                        logger.warning(
                            f"{city_id}: 429 Rate Limit (시도 {attempt}/{max_retries})"
                        )
                    else:
                        logger.warning(
                            f"{city_id}: 요청 실패 (시도 {attempt}/{max_retries}) "
                            f"상태코드={response.status}"
                        )
            except Exception as e:
                logger.error(f"{city_id}: 예외 발생 (시도 {attempt}/{max_retries}) -> {e}")

        # 재시도 전 대기 (429는 지수 백오프, 그 외 5초)
        if attempt < max_retries:
            delay = 2 ** attempt if is_rate_limited else 5
            await asyncio.sleep(delay)

    logger.error(f"{city_id}: {max_retries}회 실패 — 포기")
    return pd.DataFrame()


async def select_data_async(city_list, start: str, end: str) -> pd.DataFrame:
    """모든 지점 데이터를 비동기로 수집 (동시 요청 수 제한)"""
    service_key = get_service_key()
    if not service_key:
        raise RuntimeError("SERVICE_KEY 또는 NAMDONG_WIND_KEY가 설정되지 않았습니다.")

    semaphore = asyncio.Semaphore(MAX_CONCURRENT)
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_city(session, city, start, end, semaphore, service_key) for city in city_list]
        results = await asyncio.gather(*tasks)

    # 비어있는 DF가 섞여 있어도 concat 가능하게 필터링
    results = [r for r in results if not r.empty]
    if not results:
        logger.warning("수집된 데이터가 없습니다.")
        return pd.DataFrame()

    logger.info(f"총 {len(results)}개 지점에서 데이터 수집 완료")
    return pd.concat(results, ignore_index=True)


def normalize_weather_data(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize raw ASOS columns while preserving missing source values.

    일사량(icsr)은 기온/습도와 달리 일부 지점(관측 장비 미설치)이나 야간 시간대에
    API가 빈 문자열("")을 준다 — pd.to_numeric(errors="coerce")로 NaN이 되어
    weather_asos에는 NULL로 적재된다(정상). icsr 컬럼 자체가 없는 입력(예: 이
    필드를 추가하기 전 호출부, 기존 테스트)도 KeyError 없이 NaN으로 채운다.
    """
    result = df.copy()
    result["ta"] = pd.to_numeric(result["ta"], errors="coerce")
    result["hm"] = pd.to_numeric(result["hm"], errors="coerce")
    if "icsr" in result.columns:
        result["icsr"] = pd.to_numeric(result["icsr"], errors="coerce")
    else:
        result["icsr"] = float("nan")
    return result.rename(
        columns={
            "tm": "date",
            "hm": "humidity",
            "ta": "temperature",
            "stnNm": "station_name",
            "icsr": "solar radiation",
        }
    )


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

    # 원하는 컬럼만 남기기 (icsr=일사량은 응답에 없을 수도 있어 방어적으로 처리)
    keep_cols = ["tm", "hm", "ta", "stnNm"] + (["icsr"] if "icsr" in df.columns else [])
    df = df[keep_cols]

    df = normalize_weather_data(df)

    # 일별 CSV 저장 (컨테이너 기준: /app/data/asos_YYYYMMDD_YYYYMMDD.csv)
    daily_path = OUTPUT_DIR / f"asos_{start}_{end}.csv"
    df.to_csv(daily_path, index=False, encoding="utf-8-sig")
    logger.info(f"일별 파일 저장 완료: {daily_path}")

    # 누적 CSV에 머지 (컨테이너 기준: /app/data/asos_all_merged.csv)
    merged_path = merge_to_all_csv(daily_path)
    logger.info(f"누적 파일 갱신 완료: {merged_path}")

    logger.info("샘플 데이터:")
    logger.info(str(df.head()))
