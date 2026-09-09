from datetime import date, datetime
from importlib import import_module
from pathlib import Path


def collection_start(last_dt, hours, today):
    return import_module("pipeline.fetch_data.pv.nambu_state").collection_start(
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
    for path in ("pipeline/fetch_data/pv/nambu_collect.py", "pipeline/fetch_data/pv/nambu_backfill.py"):
        assert "nambu_generation" not in Path(path).read_text(encoding="utf-8")


def test_collectors_write_using_discovered_plant_name():
    daily = Path("pipeline/fetch_data/pv/nambu_collect.py").read_text(encoding="utf-8")
    backfill = Path("pipeline/fetch_data/pv/nambu_backfill.py").read_text(encoding="utf-8")

    assert 'core_df["plant_name"] = target["plant_name"]' in daily
    assert 'core_df["plant_name"] = t["plant_name"]' in backfill


# =========================================================
# 최근 결손일 되메우기 (earliest_start)
# =========================================================
def test_결손일이_없으면_시작일을_그대로_둔다():
    from datetime import datetime as _dt
    from pipeline.fetch_data.pv.nambu_state import earliest_start
    start = _dt(2026, 9, 1)
    assert earliest_start(start, []) == start


def test_최근_결손일이_있으면_그_날부터_다시_받는다():
    """하루치 API 실패가 영구 구멍이 되던 회귀. 가장 이른 결손일로 당겨야 한다."""
    from datetime import date as _d, datetime as _dt
    from pipeline.fetch_data.pv.nambu_state import earliest_start
    gaps = [_d(2026, 8, 30), _d(2026, 8, 22)]  # 정렬돼 있지 않아도 가장 이른 날
    assert earliest_start(_dt(2026, 9, 1), gaps) == _dt(2026, 8, 22)


def test_결손일이_시작일보다_뒤면_시작일이_이긴다():
    from datetime import date as _d, datetime as _dt
    from pipeline.fetch_data.pv.nambu_state import earliest_start
    assert earliest_start(_dt(2026, 8, 1), [_d(2026, 8, 20)]) == _dt(2026, 8, 1)
