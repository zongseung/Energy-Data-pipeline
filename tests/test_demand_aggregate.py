from datetime import datetime
from unittest.mock import MagicMock

import pytest
from sqlalchemy.dialects import postgresql

from fetch_data.demand import aggregate
from fetch_data.demand.aggregate import (
    aggregate_demand_weather,
    get_common_end,
    get_recovery_start,
    remove_repaired_unknowns,
    refresh_demand_views,
)
from fetch_data.demand.database import upsert_demand_weather


def test_recovery_starts_at_earliest_unknown():
    assert get_recovery_start(
        datetime(2026, 8, 2, 6),
        datetime(2026, 1, 6, 0),
        datetime(2026, 8, 2, 7),
    ) == datetime(2026, 1, 6, 0)


def test_common_end_uses_earlier_complete_source():
    assert get_common_end(
        datetime(2026, 8, 4, 15),
        datetime(2026, 8, 3, 23),
    ) == datetime(2026, 8, 4, 0)


def test_common_end_is_none_without_weather():
    assert get_common_end(datetime(2026, 8, 4, 15), None) is None


def test_unknown_cleanup_requires_real_station_rows():
    engine = MagicMock()
    connection = engine.begin.return_value.__enter__.return_value
    connection.execute.return_value.rowcount = 3
    start = datetime(2026, 8, 1)
    end = datetime(2026, 8, 2)

    removed = remove_repaired_unknowns(engine, start, end)

    statement, params = connection.execute.call_args.args
    sql = str(statement.compile(dialect=postgresql.dialect()))
    assert "station_name = 'UNKNOWN'" in sql
    assert "old.timestamp >= %(start)s" in sql
    assert "old.timestamp < %(end)s" in sql
    assert "AND EXISTS" in sql
    assert "WHERE real.timestamp = old.timestamp" in sql
    assert "station_name <> 'UNKNOWN'" in sql
    assert params == {"start": start, "end": end}
    assert removed == 3


def _weather_csv(tmp_path, contents: str):
    path = tmp_path / "asos_all_merged.csv"
    path.write_text(contents, encoding="utf-8-sig")
    return path


def test_aggregate_uses_complete_hours_and_real_deduplicated_weather(tmp_path, monkeypatch):
    engine = MagicMock()
    connection = engine.connect.return_value.__enter__.return_value
    connection.execute.return_value.mappings.return_value.all.return_value = [
        {
            "timestamp": datetime(2026, 8, 4, 8),
            "demand_avg": 80.0,
            "is_holiday": False,
            "day_type": 0,
        },
        {
            "timestamp": datetime(2026, 8, 4, 9),
            "demand_avg": 90.0,
            "is_holiday": True,
            "day_type": 2,
        },
        {
            "timestamp": datetime(2026, 8, 4, 10),
            "demand_avg": 100.0,
            "is_holiday": False,
            "day_type": 0,
        },
    ]
    weather_csv = _weather_csv(
        tmp_path,
        "date,station_name,temperature,humidity\n"
        "2026-08-04 09:00:00,Seoul,20,60\n"
        "2026-08-04 10:00:00,Seoul,21,61\n"
        "2026-08-04 10:30:00,Seoul,22,62\n"
        "2026-08-04 10:00:00,UNKNOWN,30,70\n"
        "2026-08-04 11:00:00,Busan,25,65\n",
    )
    records = []
    cleaned = []
    monkeypatch.setattr(
        aggregate,
        "upsert_demand_weather",
        lambda _engine, rows: records.extend(rows) or len(rows),
    )
    monkeypatch.setattr(
        aggregate,
        "remove_repaired_unknowns",
        lambda _engine, start, end: cleaned.append((start, end)),
    )

    saved = aggregate_demand_weather(engine, weather_csv, now=datetime(2026, 8, 4, 12, 34))

    statement, params = connection.execute.call_args.args
    assert "HAVING COUNT(*) >= 12" in str(statement)
    assert params == {
        "start": datetime(2026, 8, 2, 12),
        "end": datetime(2026, 8, 4, 12),
    }
    assert saved == 2
    assert records == [
        {
            "timestamp": datetime(2026, 8, 4, 9),
            "station_name": "Seoul",
            "temperature": 20,
            "humidity": 60,
            "demand_avg": 90.0,
            "is_holiday": True,
            "day_type": 2,
        },
        {
            "timestamp": datetime(2026, 8, 4, 10),
            "station_name": "Seoul",
            "temperature": 22,
            "humidity": 62,
            "demand_avg": 100.0,
            "is_holiday": False,
            "day_type": 0,
        },
    ]
    assert cleaned == [(datetime(2026, 8, 2, 12), datetime(2026, 8, 4, 11))]


def test_aggregate_empty_recovery_uses_unknown_boundary_without_writes(tmp_path, monkeypatch):
    engine = MagicMock()
    connection = engine.connect.return_value.__enter__.return_value
    connection.execute.return_value.mappings.return_value.all.return_value = []
    weather_csv = _weather_csv(
        tmp_path,
        "date,station_name,temperature,humidity\n2026-08-04 10:00:00,Seoul,20,60\n",
    )
    upsert = MagicMock()
    cleanup = MagicMock()
    monkeypatch.setattr(aggregate, "get_last_demand_weather_timestamp", lambda _engine: datetime(2026, 8, 4, 8))
    monkeypatch.setattr(aggregate, "get_first_unknown_timestamp", lambda _engine: datetime(2026, 8, 1))
    monkeypatch.setattr(aggregate, "upsert_demand_weather", upsert)
    monkeypatch.setattr(aggregate, "remove_repaired_unknowns", cleanup)

    saved = aggregate_demand_weather(
        engine, weather_csv, recover=True, now=datetime(2026, 8, 4, 12, 34)
    )

    assert saved == 0
    assert connection.execute.call_args.args[1] == {
        "start": datetime(2026, 8, 1),
        "end": datetime(2026, 8, 4, 12),
    }
    upsert.assert_not_called()
    cleanup.assert_not_called()


def test_hourly_upsert_compiles_conflict_with_exact_update_columns():
    engine = MagicMock()
    connection = engine.begin.return_value.__enter__.return_value

    upsert_demand_weather(
        engine,
        [{"timestamp": datetime(2026, 8, 4, 10), "station_name": "Seoul"}],
    )

    sql = str(connection.execute.call_args.args[0].compile(dialect=postgresql.dialect()))
    update_columns = {
        column
        for column in ("temperature", "humidity", "demand_avg", "is_holiday", "day_type")
        if f"{column} = excluded.{column}" in sql
    }
    assert "ON CONFLICT (timestamp, station_name) DO UPDATE SET" in sql
    assert update_columns == {"temperature", "humidity", "demand_avg", "is_holiday", "day_type"}


def test_refreshes_views_in_order_and_propagates_errors():
    engine = MagicMock()
    connection = engine.begin.return_value.__enter__.return_value
    connection.execute.side_effect = [None, RuntimeError("refresh failed")]

    with pytest.raises(RuntimeError, match="refresh failed"):
        refresh_demand_views(engine)

    assert [str(call.args[0]) for call in connection.execute.call_args_list] == [
        "REFRESH MATERIALIZED VIEW mv_latest_weather",
        "REFRESH MATERIALIZED VIEW mv_hourly_national",
    ]
