import asyncio

import pytest

from fetch_data.common.config import get_service_key
from fetch_data.weather import asos_collect


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
