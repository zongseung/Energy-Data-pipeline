import pytest

from fetch_data.smp import smp_realtime
from prefect_flows import smp_flow


def _unconfirmed_grid():
    return [["구분", "구분", "08.04"]] + [
        [f"{slot // 4 + 1}h", f"{slot % 4 + 1}구간", "확정가격은 D+1일 18시 공표"]
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


def test_realtime_smp_task_returns_zero_for_unconfirmed_grid(monkeypatch):
    monkeypatch.setattr(smp_flow, "run_realtime_collection", lambda: 0)

    assert smp_flow.run_smp_realtime_task.fn() == 0
