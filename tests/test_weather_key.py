import asyncio

import pandas as pd
import pytest

from fetch_data.common.config import get_service_key
from fetch_data.weather import asos_collect
from prefect_flows.prefect_pipeline import process_missing_values


def test_service_key_takes_precedence(monkeypatch):
    monkeypatch.setenv("SERVICE_KEY", "primary")
    monkeypatch.setenv("NAMDONG_WIND_KEY", "fallback")
    assert get_service_key() == "primary"


def test_namdong_key_is_weather_fallback(monkeypatch):
    monkeypatch.delenv("SERVICE_KEY", raising=False)
    monkeypatch.setenv("NAMDONG_WIND_KEY", "fallback")
    assert get_service_key() == "fallback"


def test_blank_weather_keys_return_blank(monkeypatch):
    monkeypatch.delenv("SERVICE_KEY", raising=False)
    monkeypatch.delenv("NAMDONG_WIND_KEY", raising=False)
    assert get_service_key() == ""


def test_weather_collection_fails_before_http_without_key(monkeypatch):
    monkeypatch.delenv("SERVICE_KEY", raising=False)
    monkeypatch.delenv("NAMDONG_WIND_KEY", raising=False)
    with pytest.raises(RuntimeError, match="SERVICE_KEY.*NAMDONG_WIND_KEY"):
        asyncio.run(asos_collect.select_data_async([], "20260803", "20260803"))


def test_weather_processing_does_not_impute_missing_measurements():
    source = pd.DataFrame(
        {
            "tm": ["2026-08-03 00:00", "2026-08-03 01:00", "2026-08-03 02:00"],
            "stnNm": ["Seoul", "Seoul", "Seoul"],
            "ta": [20.0, None, 22.0],
            "hm": [60.0, None, 62.0],
        }
    )

    result = process_missing_values.fn(source)

    assert pd.isna(result.loc[1, "temperature"])
    assert pd.isna(result.loc[1, "humidity"])


def test_direct_weather_normalization_does_not_impute_missing_measurements():
    source = pd.DataFrame(
        {
            "tm": ["2026-08-03 00:00", "2026-08-03 01:00", "2026-08-03 02:00"],
            "stnNm": ["Seoul", "Seoul", "Seoul"],
            "ta": [20.0, None, 22.0],
            "hm": [60.0, None, 62.0],
        }
    )

    result = asos_collect.normalize_weather_data(source)

    assert pd.isna(result.loc[1, "temperature"])
    assert pd.isna(result.loc[1, "humidity"])
