import asyncio
from datetime import datetime

import pytest


def _unified_spec():
    from prefect_flows import deploy

    return next(s for s in deploy.DEPLOYMENTS if s["name"] == "unified-demand-collection")


def test_unified_demand_deployment_registers_complete_contract(monkeypatch):
    from prefect_flows import deploy

    captured = {}
    flow = object()

    class FakeDeployment:
        async def apply(self):
            captured["applied"] = True

    async def build_from_flow(**kwargs):
        captured.update(kwargs)
        return FakeDeployment()

    monkeypatch.setattr(deploy, "import_object", lambda path: flow)
    monkeypatch.setattr(deploy.Deployment, "build_from_flow", build_from_flow)

    asyncio.run(deploy.register(_unified_spec()))

    schedule = captured["schedules"][0]
    assert captured["flow"] is flow
    assert captured["name"] == "unified-demand-collection"
    assert captured["work_pool_name"] == "pv-pool"
    assert captured["entrypoint"] == (
        "prefect_flows/demand_flow.py:unified_demand_collection_flow"
    )
    assert captured["parameters"] == {"force_hourly": False}
    assert schedule.cron == "*/10 * * * *"
    assert schedule.timezone == "Asia/Seoul"
    assert captured["job_variables"] == deploy.get_job_variables()
    assert captured["job_variables"]["image"] == "pv-pipeline:latest"
    assert captured["applied"] is True


def test_all_deployments_register_unified_demand_and_list_it(monkeypatch, capsys):
    from prefect_flows import deploy

    called = []

    async def no_op(*args, **kwargs):
        return None

    async def fake_register(spec):
        called.append(spec["name"])

    monkeypatch.setattr(deploy, "wait_for_api", no_op)
    monkeypatch.setattr(deploy, "ensure_work_pool", no_op)
    monkeypatch.setattr(deploy, "register", fake_register)

    asyncio.run(deploy.create_all_deployments())

    assert "unified-demand-collection" in called
    assert len(called) == len(deploy.DEPLOYMENTS)
    assert "unified-demand-collection" in capsys.readouterr().out


def test_unified_flow_skips_hourly_work_outside_first_ten_minutes(monkeypatch):
    from prefect_flows import demand_flow

    calls = []
    monkeypatch.setattr(demand_flow, "get_demand_engine", lambda: "engine")

    async def collect_latest(engine):
        calls.append(("collect", engine))
        return 12

    monkeypatch.setattr(demand_flow, "run_demand_collection_task", collect_latest)
    monkeypatch.setattr(
        demand_flow,
        "datetime",
        type("FixedDateTime", (), {"now": staticmethod(lambda: datetime(2026, 8, 4, 10, 10))}),
    )
    monkeypatch.setattr(
        demand_flow,
        "run_hourly_aggregation_task",
        lambda engine, recover=False: calls.append(("aggregate", engine, recover)) or 7,
    )

    assert asyncio.run(demand_flow.unified_demand_collection_flow.fn()) == {
        "demand_5min": 12,
        "demand_weather_1h": 0,
    }
    assert calls == [("collect", "engine")]


def test_unified_flow_runs_hourly_work_in_first_ten_minutes(monkeypatch):
    from prefect_flows import demand_flow

    calls = []
    monkeypatch.setattr(demand_flow, "get_demand_engine", lambda: "engine")

    async def collect_latest(engine):
        calls.append(("collect", engine))
        return 12

    monkeypatch.setattr(demand_flow, "run_demand_collection_task", collect_latest)
    monkeypatch.setattr(
        demand_flow,
        "datetime",
        type("FixedDateTime", (), {"now": staticmethod(lambda: datetime(2026, 8, 4, 10, 9))}),
    )
    monkeypatch.setattr(
        demand_flow,
        "run_hourly_aggregation_task",
        lambda engine, recover=False: calls.append(("aggregate", engine, recover)) or 7,
    )
    monkeypatch.setattr(
        demand_flow,
        "refresh_demand_views",
        lambda engine: calls.append(("refresh", engine)),
    )

    assert asyncio.run(demand_flow.unified_demand_collection_flow.fn()) == {
        "demand_5min": 12,
        "demand_weather_1h": 7,
    }
    assert calls == [
        ("collect", "engine"),
        ("aggregate", "engine", False),
        ("refresh", "engine"),
    ]


def test_unified_flow_forces_hourly_work_outside_first_ten_minutes(monkeypatch):
    from prefect_flows import demand_flow

    calls = []
    monkeypatch.setattr(demand_flow, "get_demand_engine", lambda: "engine")

    async def collect_latest(engine):
        calls.append(("collect", engine))
        return 12

    monkeypatch.setattr(demand_flow, "run_demand_collection_task", collect_latest)
    monkeypatch.setattr(
        demand_flow,
        "datetime",
        type("FixedDateTime", (), {"now": staticmethod(lambda: datetime(2026, 8, 4, 10, 10))}),
    )
    monkeypatch.setattr(
        demand_flow,
        "run_hourly_aggregation_task",
        lambda engine, recover=False: calls.append(("aggregate", engine, recover)) or 7,
    )
    monkeypatch.setattr(
        demand_flow,
        "refresh_demand_views",
        lambda engine: calls.append(("refresh", engine)),
    )

    assert asyncio.run(demand_flow.unified_demand_collection_flow.fn(force_hourly=True)) == {
        "demand_5min": 12,
        "demand_weather_1h": 7,
    }
    assert calls == [
        ("collect", "engine"),
        ("aggregate", "engine", True),
        ("refresh", "engine"),
    ]


def test_demand_collection_task_awaits_collect_latest(monkeypatch):
    from prefect_flows import demand_flow

    async def collect_latest(engine):
        assert engine == "engine"
        return 12

    monkeypatch.setattr(demand_flow, "collect_latest", collect_latest)

    assert asyncio.run(demand_flow.run_demand_collection_task.fn("engine")) == 12


def test_hourly_aggregation_task_forwards_recovery_flag(monkeypatch):
    from prefect_flows import demand_flow

    calls = []
    monkeypatch.setattr(
        demand_flow,
        "aggregate_demand_weather",
        lambda engine, weather_csv, recover=False: calls.append(
            (engine, weather_csv, recover)
        ) or 7,
    )

    assert demand_flow.run_hourly_aggregation_task.fn("engine", recover=True) == 7
    assert calls == [("engine", demand_flow.DEFAULT_MERGED_CSV, True)]


@pytest.mark.parametrize(
    ("failing_stage", "expected_calls"),
    [
        ("collector", ["collect"]),
        ("aggregate", ["collect", "aggregate"]),
        ("refresh", ["collect", "aggregate", "refresh"]),
    ],
)
def test_unified_flow_notifies_and_reraises_boundary_failures(
    monkeypatch, failing_stage, expected_calls
):
    from prefect_flows import demand_flow

    calls = []
    notifications = []

    class FailureNotifier:
        def submit(self, flow_name, error_msg):
            notifications.append((flow_name, error_msg))

    async def collect(engine):
        calls.append("collect")
        if failing_stage == "collector":
            raise RuntimeError("collector failure")
        return 12

    def aggregate(engine, recover=False):
        calls.append("aggregate")
        if failing_stage == "aggregate":
            raise RuntimeError("aggregate failure")
        return 7

    def refresh(engine):
        calls.append("refresh")
        if failing_stage == "refresh":
            raise RuntimeError("refresh failure")

    monkeypatch.setattr(demand_flow, "get_demand_engine", lambda: "engine")
    monkeypatch.setattr(demand_flow, "run_demand_collection_task", collect)
    monkeypatch.setattr(demand_flow, "run_hourly_aggregation_task", aggregate)
    monkeypatch.setattr(demand_flow, "refresh_demand_views", refresh)
    monkeypatch.setattr(demand_flow, "notify_slack_failure", FailureNotifier())

    with pytest.raises(RuntimeError, match=f"{failing_stage} failure"):
        asyncio.run(demand_flow.unified_demand_collection_flow.fn(force_hourly=True))

    assert calls == expected_calls
    assert notifications == [
        ("Unified Demand Collection", f"RuntimeError: {failing_stage} failure")
    ]
