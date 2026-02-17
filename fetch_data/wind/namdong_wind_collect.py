"""
남동발전 풍력 데이터 수집 모듈

- API 수집: apis.data.go.kr/B551893/wind-power-by-hour/list
- CSV 백필: nandong_wind_power.csv 초기 적재
- DB Upsert: wind_namdong 테이블
"""

import os
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import List, Optional

import pandas as pd
import requests
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

from fetch_data.common.db_utils import resolve_db_url
from notify.slack_notifier import send_slack_message

PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(PROJECT_ROOT / ".env")

# API 설정
API_BASE = "https://apis.data.go.kr/B551893/wind-power-by-hour/list"
NAMDONG_WIND_KEY = os.getenv("NAMDONG_WIND_KEY", "")
PAGE_SIZE = 100


# =========================================================
# API 수집
# =========================================================

def fetch_namdong_wind_api(
    start_date: str,
    end_date: str,
    service_key: Optional[str] = None,
    sleep_sec: float = 0.1,
) -> pd.DataFrame:
    """
    남동발전 풍력 API에서 시간별 발전 데이터를 수집합니다.

    Args:
        start_date: 시작일 (YYYYMMDD)
        end_date: 종료일 (YYYYMMDD)
        service_key: 공공데이터포털 API 키
        sleep_sec: 페이지 간 대기 시간(초)

    Returns:
        wide-format DataFrame (API 원본)
    """
    key = service_key or NAMDONG_WIND_KEY
    if not key:
        raise RuntimeError("NAMDONG_WIND_KEY가 설정되지 않았습니다.")

    all_records: list = []
    page = 1

    while True:
        params = {
            "serviceKey": key,
            "size": str(PAGE_SIZE),
            "page": str(page),
            "startD": start_date,
            "endD": end_date,
        }

        try:
            resp = requests.get(API_BASE, params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            print(f"[API 오류] page={page}: {e}")
            break

        # API 응답에 'reponse' (오타) 또는 'response' 키 사용
        body = data.get("reponse", data.get("response", {})).get("body", {})
        content = body.get("content", [])

        if not content:
            print(f"[API] page={page}: 데이터 없음 -> 수집 종료")
            break

        all_records.extend(content)
        print(f"[API] page={page}: {len(content)}건 수집 (누적 {len(all_records)}건)")
        page += 1
        time.sleep(sleep_sec)

    if not all_records:
        return pd.DataFrame()

    return pd.DataFrame(all_records)


def transform_wide_to_long(df_wide: pd.DataFrame) -> pd.DataFrame:
    """
    API 응답의 wide-format (qhorGen01~24) 데이터를 long-format으로 변환합니다.

    Returns:
        DataFrame[timestamp, plant_name, generation]
    """
    if df_wide.empty:
        return pd.DataFrame(columns=["timestamp", "plant_name", "generation"])

    id_cols = [c for c in ["dgenYmd", "ippt", "hogi", "ipptNam"] if c in df_wide.columns]
    value_cols = [c for c in df_wide.columns if "qhorGen" in c.lower() or "qhorgen" in c.lower()]

    if not value_cols:
        raise ValueError("시간별 발전량 컬럼(qhorGen*)을 찾을 수 없습니다.")

    df_long = pd.melt(
        df_wide,
        id_vars=id_cols,
        value_vars=value_cols,
        var_name="hour_col",
        value_name="generation",
    )

    # 시간 번호 추출 (qhorGen01 -> 1, qhorGen24 -> 24)
    df_long["hour_num"] = df_long["hour_col"].str.extract(r"(\d+)").astype(int)

    # timestamp 생성: hour 24 -> 다음 날 00시
    df_long["temp_hour"] = df_long["hour_num"].apply(lambda h: "00" if h == 24 else f"{h:02d}")
    df_long["timestamp"] = pd.to_datetime(
        df_long["dgenYmd"].astype(str) + " " + df_long["temp_hour"] + ":00:00",
        format="%Y%m%d %H:%M:%S",
        errors="coerce",
    )

    # hour 24인 경우 하루 더함
    mask_24 = df_long["hour_num"] == 24
    df_long.loc[mask_24, "timestamp"] += pd.Timedelta(days=1)

    # plant_name 생성: ipptNam + " " + hogi
    if "ipptNam" in df_long.columns and "hogi" in df_long.columns:
        df_long["plant_name"] = (
            df_long["ipptNam"].astype(str).str.strip()
            + " "
            + df_long["hogi"].astype(str).str.strip()
        )
    elif "ipptNam" in df_long.columns:
        df_long["plant_name"] = df_long["ipptNam"].astype(str).str.strip()
    else:
        df_long["plant_name"] = "unknown"

    df_long["generation"] = pd.to_numeric(df_long["generation"], errors="coerce")

    result = df_long[["timestamp", "plant_name", "generation"]].dropna(subset=["timestamp"])
    return result.reset_index(drop=True)


# =========================================================
# CSV 백필
# =========================================================

def load_namdong_wind_csv(csv_path: Optional[str] = None) -> pd.DataFrame:
    """
    nandong_wind_power.csv를 읽어 DB 적재용 DataFrame을 반환합니다.

    Returns:
        DataFrame[timestamp, plant_name, generation]
    """
    if csv_path is None:
        csv_path = PROJECT_ROOT / "nandong_wind_power.csv"
    else:
        csv_path = Path(csv_path)

    if not csv_path.exists():
        raise FileNotFoundError(f"CSV 파일을 찾을 수 없습니다: {csv_path}")

    df = pd.read_csv(csv_path, index_col=0)
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df["generation"] = pd.to_numeric(df["generation"], errors="coerce")
    df["plant_name"] = df["plant_name"].astype(str).str.strip()

    result = df[["timestamp", "plant_name", "generation"]].dropna(subset=["timestamp"])
    print(f"[CSV] 남동발전 풍력 CSV 로드: {len(result)}행")
    return result.reset_index(drop=True)


# =========================================================
# DB Upsert
# =========================================================

def upsert_wind_namdong(df: pd.DataFrame, db_url: Optional[str] = None) -> int:
    """
    wind_namdong 테이블에 upsert합니다.
    ON CONFLICT (timestamp, plant_name) DO UPDATE SET generation = EXCLUDED.generation
    """
    if df.empty:
        print("[DB] 적재할 데이터가 없습니다.")
        return 0

    resolved_url = resolve_db_url(db_url)
    if not resolved_url:
        raise RuntimeError("DB_URL이 설정되지 않았습니다.")

    engine = create_engine(resolved_url)

    upsert_sql = text("""
        INSERT INTO wind_namdong (timestamp, plant_name, generation)
        VALUES (:timestamp, :plant_name, :generation)
        ON CONFLICT (timestamp, plant_name)
        DO UPDATE SET generation = EXCLUDED.generation
    """)

    records = df[["timestamp", "plant_name", "generation"]].to_dict("records")
    batch_size = 5000
    total = 0

    with engine.begin() as conn:
        for i in range(0, len(records), batch_size):
            batch = records[i : i + batch_size]
            conn.execute(upsert_sql, batch)
            total += len(batch)
            print(f"[DB] wind_namdong upsert: {total}/{len(records)}")

    print(f"[DB] wind_namdong 적재 완료: {total}행")
    return total


# =========================================================
# 수집 실행 함수
# =========================================================

def prev_month_range(ref_dt: Optional[date] = None) -> tuple:
    """전월 시작일~종료일 반환"""
    if ref_dt is None:
        ref_dt = date.today()
    first_of_this_month = date(ref_dt.year, ref_dt.month, 1)
    prev_end = first_of_this_month - timedelta(days=1)
    prev_start = date(prev_end.year, prev_end.month, 1)
    return prev_start, prev_end


def run_namdong_wind_collection(
    target_start: Optional[str] = None,
    target_end: Optional[str] = None,
    db_url: Optional[str] = None,
) -> int:
    """
    남동발전 풍력 데이터 수집 메인 함수.

    Args:
        target_start: 시작일 (YYYYMMDD). None이면 전월 시작
        target_end: 종료일 (YYYYMMDD). None이면 전월 종료
        db_url: DB 접속 URL (override)

    Returns:
        적재 행 수
    """
    print(f"\n{'='*60}")
    print("남동발전 풍력 수집 시작")
    print(f"실행 시각: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*60}\n")

    try:
        if target_start and target_end:
            start_str = target_start
            end_str = target_end
        else:
            prev_start, prev_end = prev_month_range()
            start_str = prev_start.strftime("%Y%m%d")
            end_str = prev_end.strftime("%Y%m%d")

        print(f"수집 기간: {start_str} ~ {end_str}")

        # API 수집
        df_wide = fetch_namdong_wind_api(start_str, end_str)
        if df_wide.empty:
            print("API에서 수집된 데이터가 없습니다.")
            send_slack_message(
                f"[Namdong Wind 완료]\n"
                f"- 기간: {start_str}~{end_str}\n"
                f"- 수집 데이터 없음"
            )
            return 0

        # wide -> long 변환
        df_long = transform_wide_to_long(df_wide)
        print(f"변환 완료: {len(df_long)}행")

        # DB 적재
        inserted = upsert_wind_namdong(df_long, db_url)

        send_slack_message(
            f"[Namdong Wind 완료]\n"
            f"- 기간: {start_str}~{end_str}\n"
            f"- API 원본: {len(df_wide)}건\n"
            f"- 변환 후: {len(df_long)}행\n"
            f"- DB 적재: {inserted}행"
        )

        return inserted

    except Exception as e:
        error_msg = f"{type(e).__name__}: {e}"
        print(f"\n수집 중 에러 발생: {error_msg}")
        send_slack_message(f"[Namdong Wind 실패]\n- 에러: {error_msg}")
        raise


def backfill_from_csv(csv_path: Optional[str] = None, db_url: Optional[str] = None) -> int:
    """CSV 파일로 초기 백필"""
    df = load_namdong_wind_csv(csv_path)
    return upsert_wind_namdong(df, db_url)


if __name__ == "__main__":
    import sys

    if len(sys.argv) >= 3:
        run_namdong_wind_collection(sys.argv[1], sys.argv[2])
    elif len(sys.argv) == 2 and sys.argv[1] == "--csv":
        backfill_from_csv()
    else:
        run_namdong_wind_collection()
