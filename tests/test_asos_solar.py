"""ASOS 일사량(icsr) 수집 경로 테스트.

DB/API 없이 통과한다. 세 가지를 고정한다:
1. normalize_weather_data가 icsr(일사량)을 solar radiation 컬럼으로 매핑하는지
2. 일사 미관측 지점/야간(icsr 빈 문자열, 컬럼 자체 없음)이 NaN(→ DB에는 NULL)이 되는지
3. 기존 기온/습도 매핑이 icsr 추가로 인해 회귀하지 않는지
+ asos_solar_backfill의 날짜 청크 분할 로직(연속·무중복, API/DB 불필요)
"""

from datetime import date

import pandas as pd

from fetch_data.weather.asos_collect import normalize_weather_data
from fetch_data.weather.asos_solar_backfill import _date_chunks


# =========================================================
# normalize_weather_data: 일사량(icsr) 매핑
# =========================================================

def test_normalize_weather_data_maps_icsr_to_solar_radiation_column():
    """API 원본 컬럼 icsr(문자열 숫자)이 'solar radiation' 컬럼(float)으로 매핑된다."""
    source = pd.DataFrame({
        "tm": ["2025-07-01 14:00"],
        "stnNm": ["서울"],
        "ta": ["28.0"],
        "hm": ["55"],
        "icsr": ["2.83"],
    })

    result = normalize_weather_data(source)

    assert "solar radiation" in result.columns
    assert result.loc[0, "solar radiation"] == 2.83
    # 기존 기온/습도 매핑은 그대로 (회귀 확인)
    assert result.loc[0, "temperature"] == 28.0
    assert result.loc[0, "humidity"] == 55.0
    assert result.loc[0, "station_name"] == "서울"
    assert result.loc[0, "date"] == "2025-07-01 14:00"


def test_normalize_weather_data_empty_icsr_becomes_nan():
    """일사 미관측 지점/야간에 API가 주는 빈 문자열("")은 NaN이 되어야 한다(DB NULL로 이어짐)."""
    source = pd.DataFrame({
        "tm": ["2025-07-01 00:00", "2025-07-01 12:00"],
        "stnNm": ["서울", "강화"],
        "ta": ["20.0", "25.0"],
        "hm": ["70", "60"],
        "icsr": ["", ""],  # 서울: 야간(00시)이라 미관측, 강화: 일사계 미설치 지점
    })

    result = normalize_weather_data(source)

    assert result["solar radiation"].isna().all()


def test_normalize_weather_data_missing_icsr_column_defaults_to_nan():
    """icsr 컬럼 자체가 없는 입력(일사량 도입 이전 호출부·기존 테스트 픽스처)도 KeyError 없이 NaN."""
    source = pd.DataFrame({
        "tm": ["2025-07-01 00:00"],
        "stnNm": ["서울"],
        "ta": ["20.0"],
        "hm": ["70"],
    })

    result = normalize_weather_data(source)

    assert "solar radiation" in result.columns
    assert pd.isna(result.loc[0, "solar radiation"])


def test_normalize_weather_data_temperature_humidity_unaffected_by_icsr_addition():
    """icsr이 결측(None)이어도 기존 기온/습도 결측 보존 동작(비보간)은 그대로다 — 회귀 고정."""
    source = pd.DataFrame({
        "tm": ["2026-08-03 00:00", "2026-08-03 01:00"],
        "stnNm": ["Seoul", "Seoul"],
        "ta": [20.0, None],
        "hm": [60.0, None],
        "icsr": [None, None],
    })

    result = normalize_weather_data(source)

    assert result.loc[0, "temperature"] == 20.0
    assert pd.isna(result.loc[1, "temperature"])
    assert pd.isna(result.loc[1, "humidity"])


# =========================================================
# asos_solar_backfill: 날짜 청크 분할
# =========================================================

def test_date_chunks_single_day_range():
    chunks = list(_date_chunks(date(2019, 1, 1), date(2019, 1, 1), chunk_days=30))
    assert chunks == [(date(2019, 1, 1), date(2019, 1, 1))]


def test_date_chunks_splits_by_chunk_size_with_shorter_last_chunk():
    chunks = list(_date_chunks(date(2019, 1, 1), date(2019, 2, 15), chunk_days=30))

    assert chunks[0] == (date(2019, 1, 1), date(2019, 1, 30))
    assert chunks[1] == (date(2019, 1, 31), date(2019, 2, 15))
    assert len(chunks) == 2


def test_date_chunks_are_contiguous_and_non_overlapping():
    chunks = list(_date_chunks(date(2019, 1, 1), date(2019, 6, 30), chunk_days=30))

    for (_, prev_end), (next_start, _) in zip(chunks, chunks[1:]):
        assert next_start == prev_end + pd.Timedelta(days=1).to_pytimedelta()

    assert chunks[0][0] == date(2019, 1, 1)
    assert chunks[-1][1] == date(2019, 6, 30)
