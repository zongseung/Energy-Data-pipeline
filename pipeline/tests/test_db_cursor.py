"""수집 재개 커서가 로컬 CSV 가 아니라 DB 에서 나오는지."""
from datetime import date
from pathlib import Path

from sqlalchemy import create_engine, text

from pipeline.fetch_data.common.generation_core import latest_generation_date


def _fixture_engine():
    """plants/generation 최소 스키마 + 남동 태양광 2행."""
    engine = create_engine("sqlite://")
    with engine.begin() as c:
        c.execute(text("CREATE TABLE plants (plant_id INTEGER PRIMARY KEY, "
                       "operator TEXT, fuel_type TEXT)"))
        c.execute(text("CREATE TABLE generation (timestamp TIMESTAMP, plant_id INTEGER)"))
        c.execute(text("INSERT INTO plants VALUES (1,'namdong','solar'),(2,'namdong','wind')"))
        c.execute(text("INSERT INTO generation VALUES "
                       "('2026-07-30 23:00:00',1),('2026-07-31 05:00:00',1),"
                       "('2026-08-15 10:00:00',2)"))
    return engine


def test_cursor_is_max_timestamp_for_that_fuel():
    e = _fixture_engine()
    assert latest_generation_date(operator="namdong", fuel_type="solar", engine=e) == date(2026, 7, 31)
    assert latest_generation_date(operator="namdong", fuel_type="wind", engine=e) == date(2026, 8, 15)


def test_cursor_is_none_when_nothing_loaded():
    e = _fixture_engine()
    assert latest_generation_date(operator="nambu", fuel_type="solar", engine=e) is None


def test_collectors_no_longer_glob_csv_for_cursor():
    gen = Path("pipeline/fetch_data/gen/pipeline.py").read_text(encoding="utf-8")
    pv = Path("pipeline/fetch_data/pv/namdong_collect.py").read_text(encoding="utf-8")
    assert 'glob("koen_*.csv")' not in gen
    assert 'glob("south_pv_*.csv")' not in pv
    assert "latest_generation_date" in gen and "latest_generation_date" in pv
