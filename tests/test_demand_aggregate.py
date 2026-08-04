from datetime import datetime
from unittest.mock import MagicMock

from fetch_data.demand.aggregate import (
    get_common_end,
    get_recovery_start,
    remove_repaired_unknowns,
)


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
    removed = remove_repaired_unknowns(
        engine, datetime(2026, 8, 1), datetime(2026, 8, 2)
    )
    sql = str(connection.execute.call_args.args[0])
    assert "station_name = 'UNKNOWN'" in sql
    assert "station_name <> 'UNKNOWN'" in sql
    assert removed == 3
