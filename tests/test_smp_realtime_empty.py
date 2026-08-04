from datetime import date

import pytest

from fetch_data.smp import smp_realtime
from prefect_flows import smp_flow


def _unconfirmed_grid():
    return [["구분", "구분", "08.04"]] + [
        [
            f"{slot // 4 + 1}h",
            f"{slot % 4 + 1}구간",
            "확정가격은D+1일18시까지공표예정입니다.",
        ]
        for slot in range(96)
    ]


def _numeric_grid():
    return [["구분", "구분", "08.04"]] + [
        [f"{slot // 4 + 1}h", f"{slot % 4 + 1}구간", str(slot + 1)]
        for slot in range(96)
    ]


def test_realtime_smp_unconfirmed_grid_returns_zero_without_upsert(monkeypatch):
    upserts = []
    monkeypatch.setattr(smp_realtime, "make_session", lambda: object())
    monkeypatch.setattr(smp_realtime, "fetch_grid", lambda *args, **kwargs: _unconfirmed_grid())
    monkeypatch.setattr(
        smp_realtime.C,
        "upsert_realtime_jeju",
        lambda *args, **kwargs: upserts.append(args),
    )

    assert smp_realtime.run_realtime_collection() == 0
    assert upserts == []


def test_realtime_smp_rowspan_unconfirmed_grid_returns_zero(monkeypatch):
    grid = _unconfirmed_grid()
    for row in grid[5:]:
        row[-1] = ""
    monkeypatch.setattr(smp_realtime, "make_session", lambda: object())
    monkeypatch.setattr(smp_realtime, "fetch_grid", lambda *args, **kwargs: grid)
    monkeypatch.setattr(smp_realtime.C, "upsert_realtime_jeju", lambda *args, **kwargs: 0)

    assert smp_realtime.run_realtime_collection() == 0


def test_realtime_smp_all_empty_grid_raises_source_format_error(monkeypatch):
    grid = _unconfirmed_grid()
    for row in grid[1:]:
        row[-1] = ""
    monkeypatch.setattr(smp_realtime, "make_session", lambda: object())
    monkeypatch.setattr(smp_realtime, "fetch_grid", lambda *args, **kwargs: grid)

    with pytest.raises(RuntimeError, match="원천 데이터 형식"):
        smp_realtime.run_realtime_collection()


def test_realtime_smp_failed_grid_raises_stale_source_error(monkeypatch):
    monkeypatch.setattr(smp_realtime, "make_session", lambda: object())
    monkeypatch.setattr(smp_realtime, "fetch_grid", lambda *args, **kwargs: None)

    with pytest.raises(RuntimeError, match="제주 실시간 SMP 원천 데이터가 비어 있습니다"):
        smp_realtime.run_realtime_collection()


def test_realtime_smp_truncated_grid_raises_source_format_error(monkeypatch):
    monkeypatch.setattr(smp_realtime, "make_session", lambda: object())
    monkeypatch.setattr(
        smp_realtime,
        "fetch_grid",
        lambda *args, **kwargs: _unconfirmed_grid()[:-1],
    )

    with pytest.raises(RuntimeError, match="원천 데이터 형식"):
        smp_realtime.run_realtime_collection()


def test_realtime_smp_duplicate_slot_grid_raises_source_format_error(monkeypatch):
    grid = _unconfirmed_grid()
    grid[-1] = grid[-2]
    monkeypatch.setattr(smp_realtime, "make_session", lambda: object())
    monkeypatch.setattr(smp_realtime, "fetch_grid", lambda *args, **kwargs: grid)

    with pytest.raises(RuntimeError, match="원천 데이터 형식"):
        smp_realtime.run_realtime_collection()


def test_realtime_smp_unexpected_price_cell_raises_source_format_error(monkeypatch):
    grid = _numeric_grid()
    grid[-1][-1] = "bad cell"
    monkeypatch.setattr(smp_realtime, "make_session", lambda: object())
    monkeypatch.setattr(smp_realtime, "fetch_grid", lambda *args, **kwargs: grid)
    monkeypatch.setattr(smp_realtime.C, "upsert_realtime_jeju", lambda *args, **kwargs: 0)

    with pytest.raises(RuntimeError, match="원천 데이터 형식"):
        smp_realtime.run_realtime_collection()


@pytest.mark.parametrize("value", ["NaN", "inf", "-inf"])
def test_realtime_smp_non_finite_price_raises_source_format_error(monkeypatch, value):
    grid = _numeric_grid()
    grid[-1][-1] = value
    monkeypatch.setattr(smp_realtime, "make_session", lambda: object())
    monkeypatch.setattr(smp_realtime, "fetch_grid", lambda *args, **kwargs: grid)
    monkeypatch.setattr(smp_realtime.C, "upsert_realtime_jeju", lambda *args, **kwargs: 0)

    with pytest.raises(RuntimeError, match="원천 데이터 형식"):
        smp_realtime.run_realtime_collection()


def test_realtime_smp_marker_with_error_prefix_raises_source_format_error(monkeypatch):
    grid = _unconfirmed_grid()
    for row in grid[1:]:
        row[-1] = "ERROR: 확정가격은 D+1일 18시 공표 malformed"
    monkeypatch.setattr(smp_realtime, "make_session", lambda: object())
    monkeypatch.setattr(smp_realtime, "fetch_grid", lambda *args, **kwargs: grid)
    monkeypatch.setattr(smp_realtime.C, "upsert_realtime_jeju", lambda *args, **kwargs: 0)

    with pytest.raises(RuntimeError, match="원천 데이터 형식"):
        smp_realtime.run_realtime_collection()


def test_realtime_smp_duplicate_date_headers_raise_source_format_error(monkeypatch):
    grid = [["구분", "구분", "08.03", "08.03"]] + [
        [f"{slot // 4 + 1}h", f"{slot % 4 + 1}구간", "1", "2"]
        for slot in range(96)
    ]
    monkeypatch.setattr(smp_realtime, "make_session", lambda: object())
    monkeypatch.setattr(smp_realtime, "fetch_grid", lambda *args, **kwargs: grid)
    monkeypatch.setattr(smp_realtime.C, "upsert_realtime_jeju", lambda *args, **kwargs: 0)

    with pytest.raises(RuntimeError, match="원천 데이터 형식"):
        smp_realtime.run_realtime_collection()


def test_realtime_smp_backfill_missing_grid_raises_source_error(monkeypatch):
    monkeypatch.setattr(smp_realtime, "make_session", lambda: object())
    monkeypatch.setattr(smp_realtime, "fetch_grid", lambda *args, **kwargs: None)
    monkeypatch.setattr(smp_realtime.C, "get_engine_for", lambda *args, **kwargs: object())

    with pytest.raises(RuntimeError, match="원천 데이터가 비어 있습니다"):
        smp_realtime.run_realtime_backfill(
            start=date(2026, 8, 3),
            end=date(2026, 8, 3),
        )


def test_realtime_smp_task_returns_zero_for_unconfirmed_grid(monkeypatch):
    monkeypatch.setattr(smp_flow, "run_realtime_collection", lambda: 0)

    assert smp_flow.run_smp_realtime_task.fn() == 0
