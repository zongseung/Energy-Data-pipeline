from datetime import date, datetime
from importlib import import_module
from pathlib import Path


def collection_start(last_dt, hours, today):
    return import_module("fetch_data.pv.nambu_state").collection_start(
        last_dt, hours, today
    )


def test_retries_incomplete_latest_day():
    assert collection_start(
        datetime(2026, 8, 2, 23), 23, date(2026, 8, 4)
    ) == datetime(2026, 8, 2)


def test_starts_after_complete_latest_day():
    assert collection_start(
        datetime(2026, 8, 2, 23), 24, date(2026, 8, 4)
    ) == datetime(2026, 8, 3)


def test_skips_inactive_legacy_plant():
    assert collection_start(
        datetime(2023, 10, 20), 24, date(2026, 8, 4)
    ) is None


def test_new_plant_defaults_to_one_year_back():
    assert collection_start(None, 0, date(2026, 8, 4)) == datetime(2025, 8, 4)


def test_collectors_do_not_query_deleted_nambu_table():
    for path in ("fetch_data/pv/nambu_collect.py", "fetch_data/pv/nambu_backfill.py"):
        assert "nambu_generation" not in Path(path).read_text(encoding="utf-8")


def test_collectors_write_using_discovered_plant_name():
    daily = Path("fetch_data/pv/nambu_collect.py").read_text(encoding="utf-8")
    backfill = Path("fetch_data/pv/nambu_backfill.py").read_text(encoding="utf-8")

    assert 'core_df["plant_name"] = target["plant_name"]' in daily
    assert 'core_df["plant_name"] = t["plant_name"]' in backfill
