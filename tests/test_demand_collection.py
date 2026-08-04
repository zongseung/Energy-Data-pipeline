from datetime import date, datetime

import asyncio
import aiohttp
import pandas as pd
import pytest
from sqlalchemy.dialects import postgresql

from fetch_data.demand.collect import get_collection_start, prepare_records, request_with_retry
from fetch_data.demand.database import Demand5Min, DemandWeather1H, upsert_demand_5min


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


async def _no_sleep(*args, **kwargs):
    pass


def _demand_frame(day, start_slot=0, slots=288):
    return pd.DataFrame({
        "기준일시": [
            datetime.combine(day, datetime.min.time()) + pd.Timedelta(minutes=5 * slot)
            for slot in range(start_slot, start_slot + slots)
        ],
        "현재수요(MW)": [70000.0] * slots,
    })


def test_download_range_includes_first_and_last_dates(monkeypatch):
    from fetch_data.demand import collect

    calls = []

    class Session:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

    async def download_segment(session, start, end):
        calls.append((start, end))
        return _demand_frame(start)

    monkeypatch.setattr(collect.aiohttp, "ClientSession", lambda **kwargs: Session())
    monkeypatch.setattr(collect, "download_segment", download_segment)
    result = asyncio.run(collect.download_range(date(2026, 8, 1), date(2026, 8, 2)))

    assert calls == [
        (date(2026, 8, 1), date(2026, 8, 1)),
        (date(2026, 8, 2), date(2026, 8, 2)),
    ]
    assert len(result) == 576


def test_download_range_merges_partial_historical_attempts(monkeypatch):
    from fetch_data.demand import collect

    historic_day = date.today() - pd.Timedelta(days=1)
    attempts = iter([_demand_frame(historic_day, 0, 144), _demand_frame(historic_day, 144, 144)])

    class Session:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

    async def download_segment(*args):
        return next(attempts)

    monkeypatch.setattr(collect.aiohttp, "ClientSession", lambda **kwargs: Session())
    monkeypatch.setattr(collect, "download_segment", download_segment)
    monkeypatch.setattr(collect.asyncio, "sleep", _no_sleep)

    result = asyncio.run(collect.download_range(historic_day, historic_day, max_retries=2))

    assert len(result) == 288


def test_download_range_rejects_incomplete_historical_day(monkeypatch):
    from fetch_data.demand import collect

    historic_day = date.today() - pd.Timedelta(days=1)

    class Session:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

    async def download_segment(*args):
        return _demand_frame(historic_day, 0, 10)

    monkeypatch.setattr(collect.aiohttp, "ClientSession", lambda **kwargs: Session())
    monkeypatch.setattr(collect, "download_segment", download_segment)
    monkeypatch.setattr(collect.asyncio, "sleep", _no_sleep)

    with pytest.raises(RuntimeError, match="incomplete KPX demand data"):
        asyncio.run(collect.download_range(historic_day, historic_day, max_retries=2))


def test_download_range_rejects_historical_day_with_blank_demand(monkeypatch):
    from fetch_data.demand import collect

    historic_day = date.today() - pd.Timedelta(days=1)
    frame = _demand_frame(historic_day)
    frame.loc[100, "현재수요(MW)"] = None

    class Session:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

    async def download_segment(*args):
        return frame

    monkeypatch.setattr(collect.aiohttp, "ClientSession", lambda **kwargs: Session())
    monkeypatch.setattr(collect, "download_segment", download_segment)
    monkeypatch.setattr(collect.asyncio, "sleep", _no_sleep)

    with pytest.raises(RuntimeError, match="incomplete KPX demand data"):
        asyncio.run(collect.download_range(historic_day, historic_day, max_retries=2))


def test_later_attempt_can_replace_a_blank_historical_demand(monkeypatch):
    from fetch_data.demand import collect

    historic_day = date.today() - pd.Timedelta(days=1)
    incomplete = _demand_frame(historic_day)
    incomplete.loc[100, "현재수요(MW)"] = None
    complete = _demand_frame(historic_day)
    complete.loc[200, "현재수요(MW)"] = None
    attempts = iter([incomplete, complete])

    class Session:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

    async def download_segment(*args):
        return next(attempts)

    monkeypatch.setattr(collect.aiohttp, "ClientSession", lambda **kwargs: Session())
    monkeypatch.setattr(collect, "download_segment", download_segment)
    monkeypatch.setattr(collect.asyncio, "sleep", _no_sleep)

    result = asyncio.run(
        collect.download_range(historic_day, historic_day, max_retries=2)
    )

    assert len(result) == 288
    assert result["현재수요(MW)"].notna().all()


def test_current_day_rejects_rows_from_a_different_date(monkeypatch):
    from fetch_data.demand import collect

    requested_day = date.today()
    wrong_day = requested_day - pd.Timedelta(days=1)

    class Session:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

    async def download_segment(*args):
        return _demand_frame(wrong_day)

    monkeypatch.setattr(collect.aiohttp, "ClientSession", lambda **kwargs: Session())
    monkeypatch.setattr(collect, "download_segment", download_segment)
    monkeypatch.setattr(collect.asyncio, "sleep", _no_sleep)

    with pytest.raises(RuntimeError, match="failed KPX demand data"):
        asyncio.run(collect.download_range(requested_day, requested_day, max_retries=2))


def test_download_range_allows_nonempty_partial_current_day(monkeypatch):
    from fetch_data.demand import collect

    source_day = date.today()
    attempts = iter([_demand_frame(source_day, 0, 10), _demand_frame(source_day, 10, 10)])

    class Session:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

    async def download_segment(*args):
        return next(attempts)

    monkeypatch.setattr(collect.aiohttp, "ClientSession", lambda **kwargs: Session())
    monkeypatch.setattr(collect, "download_segment", download_segment)
    monkeypatch.setattr(collect.asyncio, "sleep", _no_sleep)

    result = asyncio.run(collect.download_range(source_day, source_day, max_retries=2))

    assert len(result) == 20


def test_failed_historical_day_stops_before_later_date_persistence(monkeypatch):
    from fetch_data.demand import collect

    first_day = date.today() - pd.Timedelta(days=2)
    later_day = first_day + pd.Timedelta(days=1)
    downloaded_days = []
    upserted = []

    class Session:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

    async def download_segment(session, start, end):
        downloaded_days.append(start)
        return pd.DataFrame() if start == first_day else _demand_frame(start)

    def upsert(engine, records):
        upserted.append(records)
        return len(records)

    monkeypatch.setattr(collect.aiohttp, "ClientSession", lambda **kwargs: Session())
    monkeypatch.setattr(collect, "download_segment", download_segment)
    monkeypatch.setattr(collect, "upsert_demand_5min", upsert)
    monkeypatch.setattr(collect.asyncio, "sleep", _no_sleep)

    with pytest.raises(RuntimeError, match="failed KPX demand data"):
        asyncio.run(collect.collect_range(object(), first_day, later_day))

    assert downloaded_days == [first_day, first_day, first_day]
    assert upserted == []


def test_completed_day_is_persisted_before_a_later_day_fails(monkeypatch):
    from fetch_data.demand import collect

    first_day = date.today() - pd.Timedelta(days=2)
    second_day = first_day + pd.Timedelta(days=1)
    calls = []
    upserted = []

    async def download(start, end):
        calls.append((start, end))
        if start != end:
            raise RuntimeError("multi-day download is not resumable")
        if start == second_day:
            raise RuntimeError("second day failed")
        return _demand_frame(start)

    monkeypatch.setattr(collect, "download_range", download)
    monkeypatch.setattr(
        collect,
        "upsert_demand_5min",
        lambda engine, records: upserted.append(records) or len(records),
    )

    with pytest.raises(RuntimeError, match="second day failed"):
        asyncio.run(collect.collect_range(object(), first_day, second_day))

    assert calls == [(first_day, first_day), (second_day, second_day)]
    assert len(upserted) == 1
    assert len(upserted[0]) == 288


def test_request_with_retry_returns_after_transient_failure(monkeypatch):
    from fetch_data.demand import collect

    class Response:
        status = 200
        headers = {}

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

        async def read(self):
            return b"ok"

    class Session:
        def __init__(self):
            self.outcomes = [aiohttp.ClientError("temporary"), Response()]
            self.calls = 0

        def request(self, *args, **kwargs):
            self.calls += 1
            outcome = self.outcomes.pop(0)
            if isinstance(outcome, Exception):
                raise outcome
            return outcome

    session = Session()
    monkeypatch.setattr(collect.asyncio, "sleep", _no_sleep)

    assert asyncio.run(request_with_retry(session, "GET", "https://example.test", max_attempts=2)) == b"ok"
    assert session.calls == 2


def test_request_with_retry_raises_after_exhaustion(monkeypatch):
    from fetch_data.demand import collect

    class Session:
        def __init__(self):
            self.calls = 0

        def request(self, *args, **kwargs):
            self.calls += 1
            raise aiohttp.ClientError("offline")

    session = Session()
    monkeypatch.setattr(collect.asyncio, "sleep", _no_sleep)

    with pytest.raises(RuntimeError, match="Request failed after 2 attempts"):
        asyncio.run(request_with_retry(session, "GET", "https://example.test", max_attempts=2))
    assert session.calls == 2


def test_upsert_compiles_timestamp_conflict_with_exact_update_columns():
    class Connection:
        statement = None

        def execute(self, statement):
            self.statement = statement

    class Transaction:
        def __init__(self, connection):
            self.connection = connection

        def __enter__(self):
            return self.connection

        def __exit__(self, *args):
            pass

    class Engine:
        def __init__(self):
            self.connection = Connection()

        def begin(self):
            return Transaction(self.connection)

    engine = Engine()
    upsert_demand_5min(engine, [{"timestamp": datetime(2026, 8, 4, 10, 0)}])

    sql = str(engine.connection.statement.compile(dialect=postgresql.dialect()))
    expected_columns = {
        "current_demand",
        "current_supply",
        "supply_capacity",
        "supply_reserve",
        "reserve_rate",
        "operation_reserve",
        "is_holiday",
        "day_type",
    }
    assert "ON CONFLICT (timestamp) DO UPDATE SET" in sql
    assert all(
        f"{column} = coalesce(excluded.{column}" in sql.lower()
        for column in expected_columns
    )
