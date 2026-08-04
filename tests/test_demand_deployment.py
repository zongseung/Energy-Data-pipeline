import ast
import asyncio
from datetime import datetime
from pathlib import Path


def test_unified_demand_deployment_is_every_ten_minutes():
    tree = ast.parse(Path("prefect_flows/deploy.py").read_text())
    fn = next(
        node
        for node in tree.body
        if isinstance(node, ast.AsyncFunctionDef)
        and node.name == "deploy_unified_demand_flow"
    )
    values = [
        keyword.value.value
        for node in ast.walk(fn)
        if isinstance(node, ast.Call)
        and getattr(node.func, "id", None) == "CronSchedule"
        for keyword in node.keywords
        if keyword.arg == "cron"
    ]
    assert values == ["*/10 * * * *"]


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
        lambda engine: calls.append(("aggregate", engine)) or 7,
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
        lambda engine: calls.append(("aggregate", engine)) or 7,
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
        ("aggregate", "engine"),
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
        lambda engine: calls.append(("aggregate", engine)) or 7,
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
        ("aggregate", "engine"),
        ("refresh", "engine"),
    ]


def test_demand_collection_task_awaits_collect_latest(monkeypatch):
    from prefect_flows import demand_flow

    async def collect_latest(engine):
        assert engine == "engine"
        return 12

    monkeypatch.setattr(demand_flow, "collect_latest", collect_latest)

    assert asyncio.run(demand_flow.run_demand_collection_task.fn("engine")) == 12
