import pytest

from prefect_flows import smp_flow


def test_realtime_smp_zero_rows_is_stale_failure(monkeypatch):
    monkeypatch.setattr(smp_flow, "run_realtime_collection", lambda: 0)

    with pytest.raises(RuntimeError, match="원천 데이터가 비어"):
        smp_flow.run_smp_realtime_task.fn()
