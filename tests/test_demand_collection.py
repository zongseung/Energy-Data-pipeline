from datetime import date, datetime

import asyncio
import pandas as pd
import pytest

from fetch_data.demand.collect import get_collection_start, prepare_records
from fetch_data.demand.database import Demand5Min, DemandWeather1H


def test_gap_collection_starts_on_last_database_day():
    now = datetime(2026, 8, 4, 16, 0)
    assert get_collection_start(datetime(2026, 8, 2, 6, 55), now) == date(2026, 8, 2)


def test_current_collection_uses_recent_window_day():
    now = datetime(2026, 8, 4, 0, 20)
    assert get_collection_start(datetime(2026, 8, 4, 0, 15), now) == date(2026, 8, 3)


def test_prepare_records_maps_kpx_columns():
    rows = prepare_records(pd.DataFrame([{
        "기준일시": "2026-08-04 10:00:00",
        "현재수요(MW)": 70000.0,
        "공급능력(MW)": 90000.0,
        "최대예측수요(MW)": 71000.0,
        "공급예비력(MW)": 20000.0,
        "공급예비율(%)": 28.5,
        "운영예비력(MW)": 9000.0,
    }]))
    assert rows[0]["timestamp"] == datetime(2026, 8, 4, 10, 0)
    assert rows[0]["current_demand"] == 70000.0


def test_database_upsert_identities_are_unique():
    demand_indexes = {tuple(column.name for column in index.columns)
                      for index in Demand5Min.__table__.indexes if index.unique}
    hourly_indexes = {tuple(column.name for column in index.columns)
                      for index in DemandWeather1H.__table__.indexes if index.unique}
    assert ("timestamp",) in demand_indexes
    assert ("timestamp", "station_name") in hourly_indexes


def test_empty_requested_range_fails(monkeypatch):
    from fetch_data.demand import collect

    async def empty_download(*args, **kwargs):
        return pd.DataFrame()

    monkeypatch.setattr(collect, "download_range", empty_download)
    with pytest.raises(RuntimeError, match="수집된 전력수요 데이터가 없습니다"):
        asyncio.run(
            collect.collect_range(object(), date(2026, 8, 3), date(2026, 8, 3))
        )
