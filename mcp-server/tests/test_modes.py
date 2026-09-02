import asyncio
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock

import httpx
import pytest

from energy_mcp import server


PAST = datetime(2000, 1, 1, tzinfo=timezone.utc)
FUTURE = datetime(9999, 1, 1, tzinfo=timezone.utc)


def tool_names(mcp):
    return {tool.name for tool in asyncio.run(mcp.list_tools())}


def resource_uris(mcp):
    return {str(resource.uri) for resource in asyncio.run(mcp.list_resources())}


def request(app, method, url):
    async def send():
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://testserver"
        ) as client:
            return await client.request(method, url)

    return asyncio.run(send())


class ClaimCollection:
    def __init__(self, doc):
        self.doc = doc

    def find_one_and_update(self, query, update, **kwargs):
        for key, expected in query.items():
            value = self.doc.get(key)
            if isinstance(expected, dict):
                if value is None or value <= expected["$gt"]:
                    return None
            elif value != expected:
                return None
        self.doc.update(update["$set"])
        return dict(self.doc)


def test_workflow_mode_cannot_bypass_approval_with_run_sql():
    assert tool_names(server.server_for_mode("workflow")) == {
        "plan_query",
        "execute_query",
    }
    assert resource_uris(server.server_for_mode("workflow")) == set()


def test_legacy_mode_keeps_run_sql_and_schema_resource_only():
    legacy = server.server_for_mode("legacy")

    assert tool_names(legacy) == {"run_sql"}
    assert resource_uris(legacy) == {server.RESOURCE_URI}


def test_workflow_mode_is_stateless_and_explains_the_confirmation_gate():
    workflow = server.server_for_mode("workflow")

    assert workflow.settings.stateless_http is True
    assert "awaiting_confirmation" in workflow.instructions
    assert "승인했다고 말할 때까지 execute_query를 호출하지 않는다" in workflow.instructions
    assert "legacy run_sql은 이 서버에 없다" in workflow.instructions


def test_unknown_mode_fails_closed():
    with pytest.raises(RuntimeError, match="ENERGY_MCP_MODE"):
        server.server_for_mode("typo")


def test_workflow_collection_is_cached_and_creates_ttl_index(monkeypatch):
    client = MagicMock()
    collection = client.get_default_database.return_value.__getitem__.return_value
    mongo_client = MagicMock(return_value=client)
    monkeypatch.setattr(server, "MongoClient", mongo_client)
    monkeypatch.setenv("ENERGY_MCP_MONGO_URI", "mongodb://example.invalid/workflows")
    server.workflow_collection.cache_clear()

    try:
        assert server.workflow_collection() is collection
        assert server.workflow_collection() is collection
    finally:
        server.workflow_collection.cache_clear()

    mongo_client.assert_called_once_with("mongodb://example.invalid/workflows")
    client.get_default_database.assert_called_once_with()
    collection.create_index.assert_called_once_with("expires_at", expireAfterSeconds=0)


def test_plan_query_creates_and_saves_a_ready_workflow(monkeypatch):
    collection = MagicMock()
    decision = SimpleNamespace(status="ready", sql="SELECT 42")
    saved = {"status": "awaiting_confirmation", "workflow_id": "wf"}
    started = datetime(2026, 9, 2, 10, 0, tzinfo=timezone.utc)
    persisted = datetime(2026, 9, 2, 10, 1, tzinfo=timezone.utc)
    clock = MagicMock(side_effect=[started, persisted])
    monkeypatch.setattr(server, "utcnow", clock)
    monkeypatch.setattr(server, "workflow_collection", lambda: collection)
    new = MagicMock(
        return_value=({"_id": "wf", "question": "상수 조회", "answers": {}}, "wf")
    )
    monkeypatch.setattr(server, "new_workflow", new)
    planner = MagicMock(return_value=decision)
    monkeypatch.setattr(server, "plan_with_openai", planner)
    monkeypatch.setattr(server, "_fetch_schema_markdown", lambda: "schema")
    validate = MagicMock(side_effect=AssertionError("plan_query duplicated SQL validation"))
    monkeypatch.setattr(server, "validate_planned_sql", validate, raising=False)
    save = MagicMock(return_value=saved)
    monkeypatch.setattr(server, "save_decision", save)
    monkeypatch.setenv("ENERGY_MCP_APPROVAL_BASE_URL", "https://mcp/approval")

    assert server.plan_query("상수 조회") == saved

    collection.insert_one.assert_called_once()
    new.assert_called_once_with("상수 조회", started)
    planner.assert_called_once_with("상수 조회", {}, "schema")
    validate.assert_not_called()
    assert save.call_args.args[:4] == (
        collection,
        "wf",
        decision,
        "https://mcp/approval",
    )
    assert save.call_args.args[4] == persisted


def test_plan_query_merges_answers_into_live_clarifying_workflow(monkeypatch):
    collection = MagicMock()
    collection.update_one.return_value.matched_count = 1
    read_at = datetime(2026, 9, 2, 10, 0, tzinfo=timezone.utc)
    updated_at = datetime(2026, 9, 2, 10, 1, tzinfo=timezone.utc)
    persisted_at = datetime(2026, 9, 2, 10, 2, tzinfo=timezone.utc)
    monkeypatch.setattr(server, "utcnow", MagicMock(
        side_effect=[read_at, updated_at, persisted_at]
    ))
    collection.find_one.return_value = {
        "_id": "wf",
        "question": "발전량",
        "answers": {"대상": "태양광"},
    }
    decision = SimpleNamespace(status="needs_clarification", questions=["기간은?"])
    monkeypatch.setattr(server, "workflow_collection", lambda: collection)
    planner = MagicMock(return_value=decision)
    monkeypatch.setattr(server, "plan_with_openai", planner)
    monkeypatch.setattr(server, "_fetch_schema_markdown", lambda: "schema")
    save = MagicMock(return_value={"status": "needs_clarification"})
    monkeypatch.setattr(server, "save_decision", save)
    monkeypatch.setenv("ENERGY_MCP_APPROVAL_BASE_URL", "https://mcp/approval")

    result = server.plan_query("ignored", "wf", {"기간": "2025년"})

    assert result == {"status": "needs_clarification"}
    query = collection.find_one.call_args.args[0]
    assert query["_id"] == "wf"
    assert query["status"] == "clarifying"
    assert query["expires_at"] == {"$gt": read_at}
    update_query = collection.update_one.call_args.args[0]
    collection.update_one.assert_called_once_with(
        update_query,
        {"$set": {"answers": {"대상": "태양광", "기간": "2025년"}}},
    )
    assert update_query == {
        "_id": "wf",
        "status": "clarifying",
        "expires_at": {"$gt": updated_at},
    }
    planner.assert_called_once_with(
        "발전량", {"대상": "태양광", "기간": "2025년"}, "schema"
    )
    assert save.call_args.args[4] == persisted_at


def test_plan_query_stops_if_resumed_answers_lose_the_live_guard(monkeypatch):
    collection = MagicMock()
    collection.find_one.return_value = {
        "_id": "wf",
        "question": "발전량",
        "answers": {"대상": "태양광"},
    }
    collection.update_one.return_value.matched_count = 0
    monkeypatch.setattr(server, "workflow_collection", lambda: collection)
    planner = MagicMock()
    schema = MagicMock()
    monkeypatch.setattr(server, "plan_with_openai", planner)
    monkeypatch.setattr(server, "_fetch_schema_markdown", schema)

    with pytest.raises(RuntimeError, match="만료됐거나 이미 다음 단계"):
        server.plan_query("ignored", "wf", {"기간": "2025년"})

    update_query = collection.update_one.call_args.args[0]
    assert update_query["_id"] == "wf"
    assert update_query["status"] == "clarifying"
    assert "$gt" in update_query["expires_at"]
    planner.assert_not_called()
    schema.assert_not_called()


def test_execute_query_runs_only_claimed_stored_sql(monkeypatch):
    stored = {"_id": "wf", "sql": "SELECT 42", "summary": "상수 조회"}
    collection = MagicMock()
    monkeypatch.setattr(server, "claim_workflow", lambda collection, workflow_id: stored)
    execute = MagicMock(
        return_value={
            "columns": ["answer"],
            "rows": [{"answer": 42}],
            "row_count": 1,
            "truncated": False,
        }
    )
    monkeypatch.setattr(server, "_execute", execute)
    monkeypatch.setattr(server, "workflow_collection", lambda: collection)
    finish = MagicMock()
    monkeypatch.setattr(server, "finish_workflow", finish)

    result = server.execute_query("wf")

    execute.assert_called_once_with("SELECT 42")
    assert result["executed_sql"] == "SELECT 42"
    assert result["request_summary"] == "상수 조회"
    assert result["workflow_id"] == "wf"
    assert finish.call_args.args[:4] == (collection, "wf", execute.return_value, None)


@pytest.mark.parametrize(
    "doc",
    [
        {"_id": "wf", "status": "awaiting_confirmation", "expires_at": FUTURE},
        {"_id": "wf", "status": "confirmed", "expires_at": PAST},
        {"_id": "wf", "status": "declined", "expires_at": FUTURE},
        {"_id": "wf", "status": "executing", "expires_at": FUTURE},
    ],
    ids=["pre-approval", "expired", "declined", "already-claimed"],
)
def test_execute_query_never_touches_postgres_without_a_claim(monkeypatch, doc):
    collection = ClaimCollection(doc)
    monkeypatch.setattr(server, "workflow_collection", lambda: collection)
    execute = MagicMock()
    monkeypatch.setattr(server, "_execute", execute)

    with pytest.raises(RuntimeError, match="실행 가능한 workflow"):
        server.execute_query("wf")

    execute.assert_not_called()


def test_execute_query_records_failure_and_does_not_retry(monkeypatch):
    stored = {"_id": "wf", "sql": "SELECT broken", "summary": "실패 조회"}
    collection = MagicMock()
    monkeypatch.setattr(server, "workflow_collection", lambda: collection)
    monkeypatch.setattr(server, "claim_workflow", lambda collection, workflow_id: stored)
    execute = MagicMock(side_effect=RuntimeError("database secret"))
    monkeypatch.setattr(server, "_execute", execute)
    finish = MagicMock()
    monkeypatch.setattr(server, "finish_workflow", finish)

    with pytest.raises(RuntimeError, match="database secret"):
        server.execute_query("wf")

    execute.assert_called_once_with("SELECT broken")
    assert finish.call_args.args[:4] == (collection, "wf", None, "RuntimeError")


def test_workflow_health_returns_200_only_when_mongo_and_postgres_answer(monkeypatch):
    collection = MagicMock()
    cursor = MagicMock()
    readonly = MagicMock()
    readonly.__enter__.return_value = cursor
    monkeypatch.setenv(server.DSN_ENV, "postgresql://example.invalid/research")
    monkeypatch.setattr(server, "workflow_collection", lambda: collection)
    monkeypatch.setattr(server, "_readonly_cursor", lambda dsn, timeout: readonly)

    response = asyncio.run(server.workflow_health(None))

    assert response.status_code == 200
    assert response.body == b"ok"
    collection.database.client.admin.command.assert_called_once_with("ping")
    cursor.execute.assert_called_once_with("SELECT 1")


@pytest.mark.parametrize("dependency", ["mongo", "postgres"])
def test_workflow_health_fails_closed_without_leaking_details(monkeypatch, dependency):
    collection = MagicMock()
    cursor = MagicMock()
    readonly = MagicMock()
    readonly.__enter__.return_value = cursor
    monkeypatch.setenv(server.DSN_ENV, "postgresql://example.invalid/research")
    monkeypatch.setattr(server, "workflow_collection", lambda: collection)
    monkeypatch.setattr(server, "_readonly_cursor", lambda dsn, timeout: readonly)
    if dependency == "mongo":
        collection.database.client.admin.command.side_effect = RuntimeError(
            "mongodb://user:secret@example.invalid/schema"
        )
    else:
        cursor.execute.side_effect = RuntimeError(
            "postgresql://user:secret@example.invalid/schema"
        )

    response = asyncio.run(server.workflow_health(None))

    assert response.status_code == 503
    assert response.body == b"unavailable"
    assert b"secret" not in response.body
    assert b"schema" not in response.body


def test_health_is_registered_on_workflow_http_server(monkeypatch):
    collection = MagicMock()
    readonly = MagicMock()
    monkeypatch.setenv(server.DSN_ENV, "postgresql://example.invalid/research")
    monkeypatch.setattr(server, "workflow_collection", lambda: collection)
    monkeypatch.setattr(server, "_readonly_cursor", lambda dsn, timeout: readonly)

    response = request(server.workflow_mcp.streamable_http_app(), "GET", "/health")

    assert response.status_code == 200
    assert response.text == "ok"
