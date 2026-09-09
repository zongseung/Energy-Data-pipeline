import asyncio
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest

from energy_mcp import server
from energy_mcp.planner import PlannerDecision
from energy_mcp.workflow import _hash


PAST = datetime(2000, 1, 1, tzinfo=timezone.utc)
FUTURE = datetime(9999, 1, 1, tzinfo=timezone.utc)


def tool_names(mcp):
    return {tool.name for tool in asyncio.run(mcp.list_tools())}


def tools_by_name(mcp):
    return {tool.name: tool for tool in asyncio.run(mcp.list_tools())}


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


class RevisionCollection:
    def __init__(self, doc):
        self.doc = dict(doc)

    def _matches(self, query):
        for key, expected in query.items():
            value = self.doc.get(key)
            if isinstance(expected, dict) and "$gt" in expected:
                if value is None or value <= expected["$gt"]:
                    return False
            elif value != expected:
                return False
        return True

    def find_one(self, query):
        return dict(self.doc) if self._matches(query) else None

    def update_one(self, query, update):
        if not self._matches(query):
            return SimpleNamespace(matched_count=0)
        self.doc.update(update.get("$set", {}))
        for key, amount in update.get("$inc", {}).items():
            self.doc[key] = self.doc.get(key, 0) + amount
        return SimpleNamespace(matched_count=1)


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


def test_legacy_tool_explains_forecast_sigungu_discovery():
    description = tools_by_name(server.legacy_mcp)["run_sql"].description

    assert "forecast_regions" in description
    assert "시군구를 `dong` 인자로 넣지 마라" in description
    assert "데이터 존재 여부" in description
    assert "전체 읍면동인지 특정 읍면동인지" in description


def test_workflow_mode_is_stateless_and_explains_the_confirmation_gate():
    workflow = server.server_for_mode("workflow")

    assert workflow.settings.stateless_http is True
    assert "awaiting_confirmation" in workflow.instructions
    assert "승인했다고 말할 때까지 execute_query를 호출하지 않는다" in workflow.instructions
    assert "legacy run_sql은 이 서버에 없다" in workflow.instructions


def test_workflow_tool_descriptions_expose_the_human_approval_protocol():
    tools = tools_by_name(server.workflow_mcp)

    assert tools["plan_query"].description == server.plan_query.__doc__
    assert "SQL을 실행하지 않는다" in tools["plan_query"].description
    assert "채팅으로 돌아와 승인 여부" in tools["plan_query"].description
    assert tools["execute_query"].description == server.execute_query.__doc__
    assert "승인했다고 채팅에 알린 뒤" in tools["execute_query"].description
    assert "model" not in tools["plan_query"].inputSchema["properties"]


def test_workflow_streamable_http_disables_uvicorn_access_logging(monkeypatch):
    app = object()
    config = MagicMock(return_value="config")
    uvicorn_server = MagicMock()
    uvicorn_server.return_value.serve = AsyncMock()
    monkeypatch.setattr(server.workflow_mcp, "streamable_http_app", MagicMock(return_value=app))
    monkeypatch.setattr(server.uvicorn, "Config", config)
    monkeypatch.setattr(server.uvicorn, "Server", uvicorn_server)

    asyncio.run(server._run_workflow_streamable_http())

    config.assert_called_once_with(
        app,
        host=server.workflow_mcp.settings.host,
        port=server.workflow_mcp.settings.port,
        log_level=server.workflow_mcp.settings.log_level.lower(),
        access_log=False,
    )
    uvicorn_server.assert_called_once_with("config")
    uvicorn_server.return_value.serve.assert_awaited_once_with()


def test_unknown_mode_fails_closed():
    with pytest.raises(RuntimeError, match="ENERGY_MCP_MODE"):
        server.server_for_mode("typo")


def test_workflow_collection_is_cached_and_creates_ttl_index(monkeypatch):
    client = MagicMock()
    collection = client.get_default_database.return_value.__getitem__.return_value
    mongo_client = MagicMock(return_value=client)
    recovered_at = datetime(2026, 9, 2, 10, 0, tzinfo=timezone.utc)
    monkeypatch.setattr(server, "MongoClient", mongo_client)
    monkeypatch.setattr(server, "utcnow", lambda: recovered_at)
    monkeypatch.setenv("ENERGY_MCP_MONGO_URI", "mongodb://example.invalid/workflows")
    server.workflow_collection.cache_clear()

    try:
        assert server.workflow_collection() is collection
        assert server.workflow_collection() is collection
    finally:
        server.workflow_collection.cache_clear()

    mongo_client.assert_called_once_with(
        "mongodb://example.invalid/workflows", tz_aware=True
    )
    client.get_default_database.assert_called_once_with()
    collection.create_index.assert_called_once_with("expires_at", expireAfterSeconds=0)
    collection.update_many.assert_called_once_with(
        {"status": "executing"},
        {"$set": {
            "status": "failed",
            "error_code": "execution_interrupted_uncertain",
            "executed_at": recovered_at,
            "duration_ms": None,
        }},
    )


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
        return_value=(
            {"_id": "wf", "question": "상수 조회", "answers": {}, "revision": 0},
            "wf",
        )
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
    assert save.call_args.args[:5] == (
        collection,
        "wf",
        0,
        decision,
        "https://mcp/approval",
    )
    assert save.call_args.args[5] == persisted


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
        "revision": 7,
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
        {
            "$set": {"answers": {"대상": "태양광", "기간": "2025년"}},
            "$inc": {"revision": 1},
        },
    )
    assert update_query == {
        "_id": "wf",
        "status": "clarifying",
        "revision": 7,
        "expires_at": {"$gt": updated_at},
    }
    planner.assert_called_once_with(
        "발전량", {"대상": "태양광", "기간": "2025년"}, "schema"
    )
    assert save.call_args.args[2] == 8
    assert save.call_args.args[5] == persisted_at


def test_plan_query_stops_if_resumed_answers_lose_the_live_guard(monkeypatch):
    collection = MagicMock()
    collection.find_one.return_value = {
        "_id": "wf",
        "question": "발전량",
        "answers": {"대상": "태양광"},
        "revision": 2,
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
    assert update_query["revision"] == 2
    assert "$gt" in update_query["expires_at"]
    planner.assert_not_called()
    schema.assert_not_called()


def test_concurrent_resume_keeps_only_the_newest_claimed_planner_snapshot(monkeypatch):
    collection = RevisionCollection({
        "_id": "wf",
        "question": "발전량",
        "answers": {},
        "conditions": {},
        "revision": 0,
        "status": "clarifying",
        "expires_at": FUTURE,
    })
    nested_result = []
    calls = 0

    def planner(question, answers, schema):
        nonlocal calls
        calls += 1
        if calls == 1:
            nested_result.append(server.plan_query("ignored", "wf", {"지역": "제주"}))
            return PlannerDecision(
                status="needs_clarification",
                questions=["오래된 질문"],
                conditions=[],
            )
        return PlannerDecision(
            status="needs_clarification",
            questions=["최신 질문"],
            conditions=[{"name": "기간", "value": "2025년"}],
        )

    monkeypatch.setattr(server, "utcnow", lambda: datetime(2026, 9, 2, tzinfo=timezone.utc))
    monkeypatch.setattr(server, "workflow_collection", lambda: collection)
    monkeypatch.setattr(server, "plan_with_openai", planner)
    monkeypatch.setattr(server, "_fetch_schema_markdown", lambda: "schema")
    monkeypatch.setenv("ENERGY_MCP_APPROVAL_BASE_URL", "https://mcp/approval")

    with pytest.raises(RuntimeError, match="다른 구체화 요청"):
        server.plan_query("ignored", "wf", {"기간": "2025년"})

    assert nested_result[0]["questions"] == ["최신 질문"]
    assert collection.doc["answers"] == {"기간": "2025년", "지역": "제주"}
    assert collection.doc["questions"] == ["최신 질문"]
    assert collection.doc["revision"] == 2


@pytest.mark.parametrize(
    "failure",
    [RuntimeError("planner refusal"), ValueError("invalid planner SQL")],
    ids=["planner-refusal", "validation"],
)
def test_plan_query_guardedly_marks_planner_failures(monkeypatch, failure):
    collection = MagicMock()
    doc = {"_id": "wf", "question": "질문", "answers": {}, "revision": 0}
    monkeypatch.setattr(server, "workflow_collection", lambda: collection)
    monkeypatch.setattr(server, "new_workflow", lambda question, now: (doc, "wf"))
    monkeypatch.setattr(server, "_fetch_schema_markdown", lambda: "schema")
    monkeypatch.setattr(server, "plan_with_openai", MagicMock(side_effect=failure))
    fail = MagicMock(return_value=True)
    monkeypatch.setattr(server, "fail_planning", fail)

    with pytest.raises(type(failure), match=str(failure)):
        server.plan_query("질문")

    assert fail.call_args.args[:4] == (collection, "wf", 0, type(failure).__name__)


def test_plan_query_guardedly_marks_invalid_planned_sql(monkeypatch):
    collection = MagicMock()
    doc = {"_id": "wf", "question": "질문", "answers": {}, "revision": 0}
    monkeypatch.setattr(server, "workflow_collection", lambda: collection)
    monkeypatch.setattr(server, "new_workflow", lambda question, now: (doc, "wf"))
    monkeypatch.setattr(server, "_fetch_schema_markdown", lambda: "schema")
    monkeypatch.setattr(
        server,
        "plan_with_openai",
        lambda *args: PlannerDecision(
            status="ready",
            conditions=[],
            summary="잘못된 계획",
            sql="UPDATE research.plants SET name = 'x'",
        ),
    )
    fail = MagicMock(return_value=True)
    monkeypatch.setattr(server, "fail_planning", fail)
    monkeypatch.setenv("ENERGY_MCP_APPROVAL_BASE_URL", "https://mcp/approval")

    with pytest.raises(ValueError, match="SELECT 또는 WITH"):
        server.plan_query("질문")

    assert fail.call_args.args[:4] == (collection, "wf", 0, "ValueError")


def test_execute_query_runs_only_claimed_stored_sql(monkeypatch):
    stored = {
        "_id": "wf",
        "sql": "SELECT 42",
        "sql_sha256": _hash("SELECT 42"),
        "approval_sql_sha256": _hash("SELECT 42"),
        "summary": "상수 조회",
    }
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
    monkeypatch.setattr(server.time, "monotonic_ns", MagicMock(
        side_effect=[1_000_000_000, 1_125_000_000]
    ))

    result = server.execute_query("wf")

    execute.assert_called_once_with("SELECT 42")
    assert result["executed_sql"] == "SELECT 42"
    assert result["request_summary"] == "상수 조회"
    assert result["workflow_id"] == "wf"
    assert finish.call_args.args[:5] == (
        collection, "wf", execute.return_value, None, 125
    )


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
    stored = {
        "_id": "wf",
        "sql": "SELECT broken",
        "sql_sha256": _hash("SELECT broken"),
        "approval_sql_sha256": _hash("SELECT broken"),
        "summary": "실패 조회",
    }
    collection = MagicMock()
    monkeypatch.setattr(server, "workflow_collection", lambda: collection)
    monkeypatch.setattr(server, "claim_workflow", lambda collection, workflow_id: stored)
    execute = MagicMock(side_effect=RuntimeError("database secret"))
    monkeypatch.setattr(server, "_execute", execute)
    finish = MagicMock()
    monkeypatch.setattr(server, "finish_workflow", finish)
    monkeypatch.setattr(server.time, "monotonic_ns", MagicMock(
        side_effect=[1_000_000_000, 1_009_000_000]
    ))

    with pytest.raises(RuntimeError, match="database secret"):
        server.execute_query("wf")

    execute.assert_called_once_with("SELECT broken")
    assert finish.call_args.args[:5] == (
        collection, "wf", None, "RuntimeError", 9
    )


def test_execute_query_fails_claimed_workflow_if_sql_hash_mismatches(monkeypatch):
    stored = {
        "_id": "wf",
        "sql": "SELECT changed",
        "sql_sha256": _hash("SELECT approved"),
        "approval_sql_sha256": _hash("SELECT approved"),
        "summary": "조회",
    }
    collection = MagicMock()
    monkeypatch.setattr(server, "workflow_collection", lambda: collection)
    monkeypatch.setattr(server, "claim_workflow", lambda collection, workflow_id: stored)
    execute = MagicMock()
    monkeypatch.setattr(server, "_execute", execute)
    finish = MagicMock()
    monkeypatch.setattr(server, "finish_workflow", finish)
    monkeypatch.setattr(server.time, "monotonic_ns", MagicMock(
        side_effect=[1_000_000_000, 1_001_000_000]
    ))

    with pytest.raises(RuntimeError, match="새 workflow"):
        server.execute_query("wf")

    execute.assert_not_called()
    assert finish.call_args.args[:5] == (
        collection, "wf", None, "sql_integrity_error", 1
    )


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
