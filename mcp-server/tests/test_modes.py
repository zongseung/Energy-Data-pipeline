import asyncio
from types import SimpleNamespace
from unittest.mock import MagicMock

import httpx
import pytest

from energy_mcp import server


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
    monkeypatch.setattr(server, "workflow_collection", lambda: collection)
    monkeypatch.setattr(
        server,
        "new_workflow",
        lambda question, now: ({"_id": "wf", "question": question, "answers": {}}, "wf"),
    )
    planner = MagicMock(return_value=decision)
    monkeypatch.setattr(server, "plan_with_openai", planner)
    monkeypatch.setattr(server, "_fetch_schema_markdown", lambda: "schema")
    validate = MagicMock(return_value="SELECT 42")
    monkeypatch.setattr(server, "validate_planned_sql", validate)
    save = MagicMock(return_value=saved)
    monkeypatch.setattr(server, "save_decision", save)
    monkeypatch.setenv("ENERGY_MCP_APPROVAL_BASE_URL", "https://mcp/approval")

    assert server.plan_query("상수 조회") == saved

    collection.insert_one.assert_called_once()
    planner.assert_called_once_with("상수 조회", {}, "schema")
    validate.assert_called_once_with("SELECT 42")
    assert decision.sql == "SELECT 42"
    assert save.call_args.args[:4] == (
        collection,
        "wf",
        decision,
        "https://mcp/approval",
    )


def test_plan_query_merges_answers_into_live_clarifying_workflow(monkeypatch):
    collection = MagicMock()
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
    monkeypatch.setattr(server, "save_decision", lambda *args: {"status": "needs_clarification"})
    monkeypatch.setenv("ENERGY_MCP_APPROVAL_BASE_URL", "https://mcp/approval")

    result = server.plan_query("ignored", "wf", {"기간": "2025년"})

    assert result == {"status": "needs_clarification"}
    query = collection.find_one.call_args.args[0]
    assert query["_id"] == "wf"
    assert query["status"] == "clarifying"
    assert "$gt" in query["expires_at"]
    collection.update_one.assert_called_once_with(
        {"_id": "wf"},
        {"$set": {"answers": {"대상": "태양광", "기간": "2025년"}}},
    )
    planner.assert_called_once_with(
        "발전량", {"대상": "태양광", "기간": "2025년"}, "schema"
    )


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


@pytest.mark.parametrize("state", ["awaiting_confirmation", "expired", "declined", "executing"])
def test_execute_query_never_touches_postgres_without_a_claim(monkeypatch, state):
    monkeypatch.setattr(server, "workflow_collection", lambda: MagicMock(name=state))
    monkeypatch.setattr(server, "claim_workflow", lambda collection, workflow_id: None)
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
