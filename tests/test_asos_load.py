"""weather_asos 적재(load_asos_df) 테스트.

DB 없이 통과하는 부분(컬럼 매핑, 빈/결측 DataFrame 처리, upsert SQL 형태)과
실제 DB가 있을 때만 도는 라운드트립 테스트(skipif)로 나뉜다.
"""

from datetime import datetime

import pandas as pd
import pytest
from sqlalchemy.dialects import postgresql

from fetch_data.weather.database import WeatherASOS, load_asos_df


# =========================================================
# 가짜 엔진 (DB 없이 컴파일된 SQL/파라미터만 검사)
# =========================================================

class _FakeConnection:
    def __init__(self):
        self.statement = None

    def execute(self, statement):
        self.statement = statement


class _FakeTransaction:
    def __init__(self, connection):
        self.connection = connection

    def __enter__(self):
        return self.connection

    def __exit__(self, *args):
        pass


class _FakeEngine:
    def __init__(self):
        self.connection = _FakeConnection()

    def begin(self):
        return _FakeTransaction(self.connection)


def _params(engine: _FakeEngine) -> dict:
    compiled = engine.connection.statement.compile(dialect=postgresql.dialect())
    return compiled.construct_params()


def _sql(engine: _FakeEngine) -> str:
    return str(engine.connection.statement.compile(dialect=postgresql.dialect()))


# =========================================================
# 컬럼 매핑 / upsert 형태
# =========================================================

def test_load_asos_df_maps_solar_radiation_column_and_renames_date():
    """CSV 원본 컬럼(date, solar radiation)이 (timestamp, solar_radiation)으로 매핑되는지."""
    engine = _FakeEngine()
    df = pd.DataFrame([{
        "date": "2025-11-01 00:00:00",
        "humidity": 87.0,
        "temperature": 13.1,
        "station_name": "강릉",
        "solar radiation": 0.42,
    }])

    n = load_asos_df(df, engine=engine)

    assert n == 1
    params = _params(engine)
    assert params["timestamp_m0"] == pd.Timestamp("2025-11-01 00:00:00")
    assert params["station_name_m0"] == "강릉"
    assert params["temperature_m0"] == 13.1
    assert params["humidity_m0"] == 87.0
    assert params["solar_radiation_m0"] == 0.42


def test_load_asos_df_upserts_on_timestamp_and_station_name_with_coalesce():
    """(timestamp, station_name) 충돌 시 COALESCE로 갱신 — NULL이 기존 값을 지우지 않는다."""
    engine = _FakeEngine()
    df = pd.DataFrame([{"date": "2025-11-01 00:00:00", "station_name": "강릉", "temperature": 13.1}])

    load_asos_df(df, engine=engine)
    sql = _sql(engine).lower()

    assert "on conflict (timestamp, station_name) do update set" in sql
    for column in ("temperature", "humidity", "solar_radiation"):
        assert f"{column} = coalesce(excluded.{column}, weather_asos.{column})" in sql


def test_load_asos_df_accepts_normalized_daily_frame_without_solar_column():
    """일일 수집 결과(normalize_weather_data 출력)에는 solar radiation이 아예 없다 -> NULL로 채운다."""
    engine = _FakeEngine()
    df = pd.DataFrame([{
        "date": "2026-08-03 00:00:00",
        "humidity": 60.0,
        "temperature": 20.0,
        "station_name": "서울",
    }])

    n = load_asos_df(df, engine=engine)

    assert n == 1
    assert _params(engine)["solar_radiation_m0"] is None


def test_load_asos_df_converts_missing_values_to_null_not_nan():
    """NaN이 DB에 float 리터럴로 들어가면 집계 함수가 오염되므로 실제 SQL NULL(None)이어야 한다."""
    engine = _FakeEngine()
    df = pd.DataFrame([{
        "date": "2025-11-01 00:00:00",
        "station_name": "강화",
        "humidity": None,
        "temperature": 12.0,
        "solar radiation": None,
    }])

    load_asos_df(df, engine=engine)
    params = _params(engine)

    assert params["humidity_m0"] is None
    assert params["solar_radiation_m0"] is None
    assert params["temperature_m0"] == 12.0


# =========================================================
# 결측/빈 DataFrame -> 예외 없이 0건
# =========================================================

def test_load_asos_df_empty_dataframe_returns_zero():
    assert load_asos_df(pd.DataFrame()) == 0


def test_load_asos_df_none_returns_zero():
    assert load_asos_df(None) == 0


def test_load_asos_df_drops_rows_with_missing_timestamp_or_station():
    """timestamp/station_name이 비면 그 행만 버리고 나머지는 정상 적재, 전부 없으면 0건."""
    engine = _FakeEngine()
    df = pd.DataFrame([
        {"date": "2025-11-01 00:00:00", "station_name": "강릉", "temperature": 13.1},
        {"date": None, "station_name": "강화", "temperature": 12.0},
        {"date": "2025-11-01 02:00:00", "station_name": "", "temperature": 11.0},
    ])

    n = load_asos_df(df, engine=engine)

    assert n == 1
    assert _params(engine)["station_name_m0"] == "강릉"


def test_load_asos_df_drops_rows_with_nan_station_name_not_literal_nan_string():
    """station_name이 진짜 NaN(float)이면 문자열 "nan"으로 둔갑해 살아남으면 안 된다."""
    engine = _FakeEngine()
    df = pd.DataFrame([
        {"date": "2025-11-01 00:00:00", "station_name": "강릉", "temperature": 13.1},
        {"date": "2025-11-01 01:00:00", "station_name": float("nan"), "temperature": 12.0},
    ])

    n = load_asos_df(df, engine=engine)

    assert n == 1
    assert _params(engine)["station_name_m0"] == "강릉"


def test_load_asos_df_raises_on_missing_required_columns():
    with pytest.raises(ValueError, match="필수 컬럼"):
        load_asos_df(pd.DataFrame([{"temperature": 1.0}]))


def test_weather_asos_unique_index_is_timestamp_and_station_name():
    unique_indexes = {
        tuple(column.name for column in index.columns)
        for index in WeatherASOS.__table__.indexes
        if index.unique
    }
    assert ("timestamp", "station_name") in unique_indexes


# =========================================================
# 실제 DB 라운드트립 (DB 없으면 skip)
# =========================================================

def _db_available() -> bool:
    try:
        from fetch_data.common.db_base import get_engine
        with get_engine().connect():
            return True
    except Exception:
        return False


@pytest.mark.skipif(not _db_available(), reason="pv DB에 접속할 수 없어 건너뜀")
def test_load_asos_df_roundtrip_is_idempotent_against_real_db():
    """실제 DB에 두 번 적재해도 행 수가 늘지 않는지(멱등) 확인 후 테스트 행을 정리한다."""
    from sqlalchemy import text

    from fetch_data.common.db_base import get_engine
    from fetch_data.weather.database import init_db

    init_db()
    engine = get_engine()
    station = "__PYTEST_ASOS__"
    ts = datetime(2099, 1, 1, 0, 0, 0)

    try:
        df = pd.DataFrame([{
            "date": ts, "station_name": station, "temperature": 1.0,
            "humidity": 2.0, "solar radiation": 3.0,
        }])

        first = load_asos_df(df, engine=engine)
        second = load_asos_df(df, engine=engine)

        with engine.connect() as conn:
            count = conn.execute(
                text("SELECT count(*) FROM weather_asos WHERE station_name = :s"),
                {"s": station},
            ).scalar_one()

        assert first == 1
        assert second == 1
        assert count == 1
    finally:
        with engine.begin() as conn:
            conn.execute(
                text("DELETE FROM weather_asos WHERE station_name = :s"), {"s": station}
            )
