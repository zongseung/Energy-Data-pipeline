# 정식 LLM·MCP 질의 승인 워크플로 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 모호한 자연어 질문을 구체화하고, MongoDB에 저장된 조건과 SQL을 사람이 승인한 뒤 정확히 한 번만 실행하는 정식 LLM·MCP 경로를 만든다.

**Architecture:** `energy-mcp` 안에 planner, Mongo workflow, 승인 HTTP route를 추가하되 기존 `_execute()` 읽기전용 경계를 그대로 재사용한다. 정식 Streamable HTTP 서버는 `plan_query`와 `execute_query`만 노출하고, 기존 `run_sql`은 legacy stdio 서버에만 남긴다. 비밀값은 Compose 환경변수에서 제거하고 `/run/secrets` 파일을 시작 래퍼가 읽게 한다.

**Tech Stack:** Python 3.10+, FastMCP 1.28+, OpenAI Python SDK, PyMongo 4, Pydantic, Starlette, PostgreSQL 14, MongoDB 7, Docker Compose, pytest

**Spec:** `docs/superpowers/specs/2026-09-02-production-llm-mcp-workflow-design.md`

## Global Constraints

- Tailscale IP allowlist와 IP→사용자 매핑은 이번 계획에서 구현하지 않는다.
- 정식 HTTP mode에는 `run_sql(query)`를 절대 등록하지 않는다.
- legacy stdio mode의 `run_sql` 입력·출력·읽기전용·timeout·행 제한 동작을 바꾸지 않는다.
- 승인 전, 만료 후, 거절 후, 이미 점유된 workflow에서는 PostgreSQL을 호출하지 않는다.
- 실행할 SQL은 MongoDB에 승인 당시 저장된 문자열뿐이다. `execute_query` 입력에 SQL을 추가하지 않는다.
- SQL이 변경되면 새 hash와 새 승인이 필요하다. 오류 후 자동 수정·재실행하지 않는다.
- 활성 workflow TTL은 30분이며 실행 조건에서도 `expires_at > now`를 검사한다.
- `gpt-4o-mini`를 planner 기본 모델로 사용하고 `ENERGY_MCP_PLANNER_MODEL`로만 교체한다.
- 신규 런타임 의존성은 공식 `openai`와 `pymongo` 두 개뿐이다.
- OpenAI·JWT·암호화·PostgreSQL·MongoDB 비밀값을 Git, Compose environment/env_file, 명령행, 로그에 넣지 않는다.
- 현재 노출된 OpenAI/JWT/암호화 키는 재사용하지 않는다. OpenAI key 폐기·재발급은 계정 소유자가 수행한다.
- Docker daemon 관리자와 host root로부터 secret을 숨긴다고 주장하지 않는다.
- 기존 dirty worktree의 사용자 변경과 staged 삭제를 수정·unstage·커밋하지 않는다.

## File Structure

| 파일 | 책임 |
|---|---|
| `mcp-server/energy_mcp/workflow.py` | workflow 문서 생성, hash/token, Mongo 원자 상태 전이 |
| `mcp-server/energy_mcp/planner.py` | 단일 planner prompt, 구조화 응답 모델, OpenAI 호출 |
| `mcp-server/energy_mcp/approval.py` | 승인 페이지 HTML과 GET/POST route |
| `mcp-server/energy_mcp/server.py` | legacy/workflow FastMCP 등록, 기존 `_execute()` 연결 |
| `mcp-server/tests/test_workflow.py` | 상태 전이와 1회 실행 단위 테스트 |
| `mcp-server/tests/test_planner.py` | prompt 입력과 구조화 응답 검증 |
| `mcp-server/tests/test_approval.py` | token·CSRF·승인/거절 route 테스트 |
| `mcp-server/tests/test_modes.py` | mode별 MCP tool 노출 테스트 |
| `docker/llm-demo/load-secrets.sh` | `/run/secrets`를 런타임 환경으로 로드한 뒤 exec |
| `docker/llm-demo/init-mongo-users.js` | LibreChat·energy_mcp Mongo 사용자 생성 |
| `docker/llm-demo/provision-secrets.sh` | 저장소 밖 secret 디렉터리와 신규 키 파일 생성 |
| `docker/llm-demo/compose.yml` | 정식 workflow mode, Mongo auth, secret mount, healthcheck |
| `docker/llm-demo/nginx.conf` | `/approval/`을 energy-mcp custom route로 전달 |
| `docker/llm-demo/librechat.yaml` | MCP server instructions 활성화 |
| `tests/test_llm_service_config.py` | secret 노출·mode·proxy 설정 정적 검증 |
| `docs/gitbook/03-llm-mcp.md` | 정식 질문→구체화→승인→실행 사용자 절차 |

---

### Task 1: Mongo Workflow 상태 전이

**Files:**
- Create: `mcp-server/energy_mcp/workflow.py`
- Create: `mcp-server/tests/test_workflow.py`
- Modify: `mcp-server/pyproject.toml:7-18`

**Interfaces:**
- Produces: `new_workflow(question: str, now: datetime) -> tuple[dict, str]`
- Produces: `save_decision(collection, workflow_id: str, decision: object, approval_base_url: str, now: datetime) -> dict`
- Produces: `issue_csrf(collection, workflow_id: str, now: datetime) -> str`
- Produces: `approve_workflow(collection, workflow_id: str, approval_token: str, csrf_token: str, now: datetime) -> bool`
- Produces: `decline_workflow(collection, workflow_id: str, approval_token: str, csrf_token: str, now: datetime) -> bool`
- Produces: `claim_workflow(collection, workflow_id: str, now: datetime) -> dict | None`
- Produces: `finish_workflow(collection, workflow_id: str, result: dict | None, error_code: str | None, now: datetime) -> None`

- [ ] **Step 1: Write failing workflow tests**

```python
# mcp-server/tests/test_workflow.py
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock

from energy_mcp.workflow import (
    WORKFLOW_TTL,
    claim_workflow,
    new_workflow,
    save_decision,
)
NOW = datetime(2026, 9, 2, 10, 0, tzinfo=timezone.utc)


def test_clarification_never_stores_sql():
    collection = MagicMock()
    collection.update_one.return_value.matched_count = 1
    doc, _ = new_workflow("효율 좋은 발전소", NOW)
    collection.find_one.return_value = doc
    decision = SimpleNamespace(
        status="needs_clarification",
        questions=["효율을 이용률로 계산할까요?"],
        conditions={},
    )

    result = save_decision(collection, doc["_id"], decision, "https://mcp/approval", NOW)

    assert result["status"] == "needs_clarification"
    update = collection.update_one.call_args.args[1]["$set"]
    assert "sql" not in update
    assert "approval_token_hash" not in update


def test_ready_plan_is_saved_without_execution():
    collection = MagicMock()
    collection.update_one.return_value.matched_count = 1
    doc, _ = new_workflow("2025년 구미태양광 월별 발전량", NOW)
    collection.find_one.return_value = doc
    decision = SimpleNamespace(
        status="ready",
        questions=[],
        conditions={"대상": "구미태양광", "기간": "2025년", "집계": "월별 합계"},
        summary="구미태양광의 2025년 월별 발전량 합계",
        sql="SELECT date_trunc('month', timestamp), sum(gen_kwh) FROM research.generation GROUP BY 1",
    )

    result = save_decision(collection, doc["_id"], decision, "https://mcp/approval", NOW)

    assert result["status"] == "awaiting_confirmation"
    assert result["approval_url"].startswith("https://mcp/approval/")
    update = collection.update_one.call_args.args[1]["$set"]
    assert update["status"] == "awaiting_confirmation"
    assert update["expires_at"] == NOW + WORKFLOW_TTL
    assert update["sql_sha256"]
    assert "approval_token" not in update


def test_only_one_confirmed_execution_can_be_claimed():
    collection = MagicMock()
    collection.find_one_and_update.side_effect = [
        {"_id": "wf", "status": "executing", "sql": "SELECT 1"},
        None,
    ]

    assert claim_workflow(collection, "wf", NOW)["sql"] == "SELECT 1"
    assert claim_workflow(collection, "wf", NOW) is None
    query = collection.find_one_and_update.call_args_list[0].args[0]
    assert query == {"_id": "wf", "status": "confirmed", "expires_at": {"$gt": NOW}}
```

- [ ] **Step 2: Run the focused test and verify RED**

Run: `cd mcp-server && uv run pytest tests/test_workflow.py -q`

Expected: FAIL with `ModuleNotFoundError: No module named 'energy_mcp.workflow'`.

- [ ] **Step 3: Add the two runtime dependencies**

```toml
dependencies = [
    "mcp>=1.28,<2",
    "openai>=1.100,<3",
    "psycopg2-binary>=2.9",
    "pymongo>=4.10,<5",
]
```

Run: `cd mcp-server && uv lock && uv sync --group dev`

Expected: `mcp-server/uv.lock` is created and records both new dependencies.

- [ ] **Step 4: Implement the minimum workflow functions**

```python
# mcp-server/energy_mcp/workflow.py
from __future__ import annotations

import hashlib
import secrets
from datetime import datetime, timedelta, timezone
from urllib.parse import quote

from pymongo import ReturnDocument

WORKFLOW_TTL = timedelta(minutes=30)


def _hash(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def new_workflow(question: str, now: datetime | None = None) -> tuple[dict, str]:
    now = now or utcnow()
    workflow_id = secrets.token_urlsafe(32)
    return ({
        "_id": workflow_id,
        "question": question.strip(),
        "answers": {},
        "conditions": {},
        "status": "clarifying",
        "conversation_id": None,
        "principal_id": None,
        "created_at": now,
        "expires_at": now + WORKFLOW_TTL,
    }, workflow_id)


def save_decision(collection, workflow_id, decision, approval_base_url, now=None):
    now = now or utcnow()
    if decision.status == "needs_clarification":
        changed = collection.update_one(
            {"_id": workflow_id, "status": "clarifying", "expires_at": {"$gt": now}},
            {"$set": {"questions": decision.questions, "conditions": decision.conditions}},
        )
        if changed.matched_count != 1:
            raise RuntimeError("workflow가 만료됐거나 이미 다음 단계로 진행됐습니다.")
        return {"status": "needs_clarification", "workflow_id": workflow_id,
                "questions": decision.questions}

    token = secrets.token_urlsafe(32)
    sql = decision.sql.strip()
    changed = collection.update_one(
        {"_id": workflow_id, "status": "clarifying", "expires_at": {"$gt": now}},
        {"$set": {
            "status": "awaiting_confirmation",
            "conditions": decision.conditions,
            "summary": decision.summary,
            "sql": sql,
            "sql_sha256": _hash(sql),
            "approval_token_hash": _hash(token),
            "expires_at": now + WORKFLOW_TTL,
        }},
    )
    if changed.matched_count != 1:
        raise RuntimeError("workflow가 만료됐거나 이미 다음 단계로 진행됐습니다.")
    url = f"{approval_base_url.rstrip('/')}/{quote(workflow_id)}?token={quote(token)}"
    return {"status": "awaiting_confirmation", "workflow_id": workflow_id,
            "summary": decision.summary, "sql": sql, "approval_url": url,
            "expires_at": (now + WORKFLOW_TTL).isoformat()}


def claim_workflow(collection, workflow_id: str, now: datetime | None = None):
    now = now or utcnow()
    return collection.find_one_and_update(
        {"_id": workflow_id, "status": "confirmed", "expires_at": {"$gt": now}},
        {"$set": {"status": "executing", "claimed_at": now}},
        return_document=ReturnDocument.AFTER,
    )
```

Add `issue_csrf`, `approve_workflow`, and `decline_workflow`. Both transitions
use an atomic filter on `_id`, `status`, `expires_at`, `approval_token_hash`,
and `approval_csrf_hash`; they set `confirmed` or `declined` and unset the CSRF
hash. Add `finish_workflow` to move only `executing` to `done` or `failed` and
record `row_count`/`error_code`, never result rows.

- [ ] **Step 5: Run workflow tests and verify GREEN**

Run: `cd mcp-server && uv run pytest tests/test_workflow.py -q`

Expected: PASS.

- [ ] **Step 6: Commit Task 1 only**

```bash
git add mcp-server/pyproject.toml mcp-server/uv.lock \
  mcp-server/energy_mcp/workflow.py mcp-server/tests/test_workflow.py
git commit -m "feat(mcp): Mongo 질의 workflow 상태 전이 추가"
```

---

### Task 2: 중앙 Planner Prompt와 구조화 출력

**Files:**
- Create: `mcp-server/energy_mcp/planner.py`
- Create: `mcp-server/tests/test_planner.py`
- Modify: `mcp-server/energy_mcp/workflow.py`

**Interfaces:**
- Produces: `PlannerDecision(status, questions, conditions, summary, sql)`
- Produces: `plan_with_openai(question: str, answers: dict[str, str], schema_md: str, model: str | None = None, client=None) -> PlannerDecision`
- Consumes: `server._fetch_schema_markdown() -> str`

- [ ] **Step 1: Write failing planner tests**

```python
# mcp-server/tests/test_planner.py
from types import SimpleNamespace

import pytest

from energy_mcp.planner import PlannerDecision, plan_with_openai


class FakeResponses:
    def __init__(self, parsed):
        self.parsed = parsed
        self.kwargs = None

    def parse(self, **kwargs):
        self.kwargs = kwargs
        return SimpleNamespace(output_parsed=self.parsed)


def test_planner_receives_original_question_answers_and_schema():
    parsed = PlannerDecision(
        status="needs_clarification",
        questions=["기간을 알려주세요."],
        conditions={"대상": "태양광"},
    )
    responses = FakeResponses(parsed)
    client = SimpleNamespace(responses=responses)

    result = plan_with_openai(
        "태양광 비교해줘",
        {"지역": "제주"},
        "research.generation(timestamp, fuel_type, gen_kwh)",
        client=client,
    )

    assert result == parsed
    payload = str(responses.kwargs["input"])
    assert "태양광 비교해줘" in payload
    assert "제주" in payload
    assert "research.generation" in payload
    assert responses.kwargs["model"] == "gpt-4o-mini"


def test_ready_decision_requires_summary_and_sql():
    with pytest.raises(ValueError):
        PlannerDecision(status="ready", questions=[], conditions={})
```

- [ ] **Step 2: Run planner tests and verify RED**

Run: `cd mcp-server && uv run pytest tests/test_planner.py -q`

Expected: FAIL because `energy_mcp.planner` does not exist.

- [ ] **Step 3: Implement the planner schema and one prompt**

```python
# mcp-server/energy_mcp/planner.py
from __future__ import annotations

import json
import os
from typing import Literal

from openai import OpenAI
from pydantic import BaseModel, Field, model_validator

PLANNER_MODEL_ENV = "ENERGY_MCP_PLANNER_MODEL"
DEFAULT_PLANNER_MODEL = "gpt-4o-mini"

PLANNER_PROMPT = """당신은 PostgreSQL research 스키마 전용 질의 계획기다.
사용자 의도를 임의로 채우지 마라. 대상, 지표, 기간, 집계 단위, 중요한 필터 중
SQL 결과를 바꿀 정보가 빠졌으면 needs_clarification과 짧은 질문만 반환한다.
정보가 충분하면 ready, 한국어 조건 요약, 단일 읽기전용 PostgreSQL SELECT 또는
WITH...SELECT를 반환한다. 제공된 스키마 밖 이름을 만들지 마라. 세미콜론을 쓰지 마라.
연료를 지정한 질문은 fuel_type을 필터하고, 집계 질문은 GROUP BY와 집계함수를 쓴다.
"""


class PlannerDecision(BaseModel):
    status: Literal["needs_clarification", "ready"]
    questions: list[str] = Field(default_factory=list)
    conditions: dict[str, str] = Field(default_factory=dict)
    summary: str | None = None
    sql: str | None = None

    @model_validator(mode="after")
    def validate_state(self):
        if self.status == "ready" and (not self.summary or not self.sql):
            raise ValueError("ready 응답에는 summary와 sql이 필요합니다.")
        if self.status == "needs_clarification" and not self.questions:
            raise ValueError("needs_clarification 응답에는 질문이 필요합니다.")
        return self


def plan_with_openai(question, answers, schema_md, model=None, client=None):
    client = client or OpenAI()
    payload = json.dumps(
        {"question": question, "answers": answers, "schema": schema_md},
        ensure_ascii=False,
    )
    response = client.responses.parse(
        model=model or os.environ.get(PLANNER_MODEL_ENV, DEFAULT_PLANNER_MODEL),
        input=[
            {"role": "system", "content": PLANNER_PROMPT},
            {"role": "user", "content": payload},
        ],
        text_format=PlannerDecision,
    )
    if response.output_parsed is None:
        raise RuntimeError("planner가 구조화된 응답을 반환하지 않았습니다.")
    return response.output_parsed
```

Move `_reject_multi_statement` from `server.py` to `workflow.py` and import it
back into `server.py`, so legacy and workflow mode use the same check. Add this
strict planner-only wrapper and focused tests asserting that `UPDATE`, an empty
query, and `SELECT 1; SELECT 2` are rejected:

```python
def validate_planned_sql(query: str) -> str:
    _reject_multi_statement(query)
    query = query.strip().removesuffix(";").strip()
    first = query.split(None, 1)[0].upper()
    if first not in {"SELECT", "WITH"}:
        raise ValueError("planner SQL은 SELECT 또는 WITH로 시작해야 합니다.")
    return query
```

- [ ] **Step 4: Run planner and workflow tests**

Run: `cd mcp-server && uv run pytest tests/test_planner.py tests/test_workflow.py -q`

Expected: PASS without network or a live MongoDB.

- [ ] **Step 5: Commit Task 2 only**

```bash
git add mcp-server/energy_mcp/planner.py mcp-server/energy_mcp/workflow.py \
  mcp-server/tests/test_planner.py mcp-server/tests/test_workflow.py
git commit -m "feat(mcp): 모호성을 판정하는 중앙 planner 추가"
```

---

### Task 3: 사람 전용 승인 페이지

**Files:**
- Create: `mcp-server/energy_mcp/approval.py`
- Create: `mcp-server/tests/test_approval.py`
- Modify: `mcp-server/energy_mcp/workflow.py`

**Interfaces:**
- Produces: `register_approval_routes(mcp: FastMCP, collection_factory: Callable) -> None`
- Produces routes: `GET /approval/{workflow_id}`, `POST /approval/{workflow_id}`
- Consumes: `approve_workflow(...) -> bool`, `_hash(value: str) -> str`

- [ ] **Step 1: Write failing approval route tests**

```python
# mcp-server/tests/test_approval.py
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

from starlette.applications import Starlette
from starlette.testclient import TestClient

from energy_mcp.approval import approval_get, approval_post
from energy_mcp.workflow import _hash

NOW = datetime.now(timezone.utc)


def app_for(collection):
    app = Starlette()
    async def get_approval(request):
        return await approval_get(request, collection, now=lambda: NOW)
    async def post_approval(request):
        return await approval_post(request, collection, now=lambda: NOW)
    app.add_route(
        "/approval/{workflow_id}",
        get_approval,
        methods=["GET"],
    )
    app.add_route(
        "/approval/{workflow_id}",
        post_approval,
        methods=["POST"],
    )
    return app


def test_get_rejects_bad_token_without_showing_sql():
    collection = MagicMock()
    collection.find_one.return_value = None
    response = TestClient(app_for(collection)).get("/approval/wf?token=bad")
    assert response.status_code == 404
    assert "SELECT" not in response.text


def test_get_escapes_summary_and_issues_one_time_csrf():
    collection = MagicMock()
    collection.find_one.return_value = {
        "_id": "wf",
        "status": "awaiting_confirmation",
        "summary": "<script>alert(1)</script>",
        "sql": "SELECT 1",
        "expires_at": NOW + timedelta(minutes=10),
        "approval_token_hash": _hash("secret"),
    }
    response = TestClient(app_for(collection)).get("/approval/wf?token=secret")
    assert response.status_code == 200
    assert "<script>" not in response.text
    assert "&lt;script&gt;" in response.text
    assert "SELECT 1" in response.text
    assert collection.update_one.called


def test_post_confirm_uses_atomic_workflow_transition():
    collection = MagicMock()
    collection.find_one_and_update.return_value = {"_id": "wf", "status": "confirmed"}
    response = TestClient(app_for(collection)).post(
        "/approval/wf",
        data={"action": "confirm", "token": "secret", "csrf": "nonce"},
    )
    assert response.status_code == 200
    query = collection.find_one_and_update.call_args.args[0]
    assert query["status"] == "awaiting_confirmation"
    assert query["approval_token_hash"] == _hash("secret")
    assert query["approval_csrf_hash"] == _hash("nonce")
```

- [ ] **Step 2: Run approval tests and verify RED**

Run: `cd mcp-server && uv run pytest tests/test_approval.py -q`

Expected: FAIL because `energy_mcp.approval` does not exist.

- [ ] **Step 3: Implement escaped GET and atomic POST**

Use only `html.escape`, `secrets.token_urlsafe`, and
`urllib.parse.parse_qs`; do not add a template engine or multipart parser.

```python
# mcp-server/energy_mcp/approval.py
from html import escape
from urllib.parse import parse_qs

from starlette.requests import Request
from starlette.responses import HTMLResponse, PlainTextResponse

from energy_mcp.workflow import (
    _hash,
    approve_workflow,
    decline_workflow,
    issue_csrf,
    utcnow,
)


async def approval_get(request: Request, collection, now=utcnow):
    workflow_id = request.path_params["workflow_id"]
    token = request.query_params.get("token", "")
    doc = collection.find_one({
        "_id": workflow_id,
        "status": "awaiting_confirmation",
        "expires_at": {"$gt": now()},
        "approval_token_hash": _hash(token),
    })
    if doc is None:
        return PlainTextResponse("승인 요청이 없거나 만료됐습니다.", status_code=404)
    csrf = issue_csrf(collection, workflow_id, now())
    body = APPROVAL_HTML.format(
        summary=escape(doc["summary"]),
        sql=escape(doc["sql"]),
        token=escape(token, quote=True),
        csrf=escape(csrf, quote=True),
    )
    return HTMLResponse(body, headers={
        "Cache-Control": "no-store",
        "Content-Security-Policy": "default-src 'none'; style-src 'unsafe-inline'; form-action 'self'; base-uri 'none'; frame-ancestors 'none'",
        "Referrer-Policy": "no-referrer",
        "X-Content-Type-Options": "nosniff",
    })


async def approval_post(request: Request, collection, now=utcnow):
    form = parse_qs((await request.body()).decode())
    action = form.get("action", [""])[0]
    token = form.get("token", [""])[0]
    csrf = form.get("csrf", [""])[0]
    transition = approve_workflow if action == "confirm" else decline_workflow
    if action not in {"confirm", "decline"}:
        return PlainTextResponse("알 수 없는 처리입니다.", status_code=400)
    ok = transition(collection, request.path_params["workflow_id"],
                    token, csrf, now())
    if not ok:
        return PlainTextResponse("승인 요청이 없거나 이미 처리됐습니다.", status_code=409)
    return PlainTextResponse("조회 조건을 처리했습니다. 채팅으로 돌아가세요.")
```

`APPROVAL_HTML` must contain no external scripts, stylesheets, images, or links.
It must show two submit buttons with `action=confirm` and `action=decline`.
`issue_csrf` stores only `_hash(csrf)` and replaces any previous nonce. The
POST transition must unset the CSRF hash so refresh cannot approve twice.

- [ ] **Step 4: Run approval and workflow tests**

Run: `cd mcp-server && uv run pytest tests/test_approval.py tests/test_workflow.py -q`

Expected: PASS.

- [ ] **Step 5: Commit Task 3 only**

```bash
git add mcp-server/energy_mcp/approval.py mcp-server/energy_mcp/workflow.py \
  mcp-server/tests/test_approval.py mcp-server/tests/test_workflow.py
git commit -m "feat(mcp): 일회용 SQL 승인 페이지 추가"
```

---

### Task 4: 정식 MCP mode와 SQL 실행 연결

**Files:**
- Modify: `mcp-server/energy_mcp/server.py:84,308-526`
- Create: `mcp-server/tests/test_modes.py`
- Modify: `mcp-server/tests/test_server.py`

**Interfaces:**
- Produces: `legacy_mcp: FastMCP` with `run_sql` and `energy://schema`
- Produces: `workflow_mcp: FastMCP` with `plan_query` and `execute_query`
- Produces: `server_for_mode(mode: str) -> FastMCP`
- Produces: `GET /health` returning 200 only when MongoDB and PostgreSQL answer
- Consumes: `plan_with_openai`, Mongo collection, approval route registration, `_execute`

- [ ] **Step 1: Write failing mode and execution tests**

```python
# mcp-server/tests/test_modes.py
import asyncio
from unittest.mock import MagicMock

import pytest

from energy_mcp import server


def tool_names(mcp):
    return {tool.name for tool in asyncio.run(mcp.list_tools())}


def test_workflow_mode_cannot_bypass_approval_with_run_sql():
    assert tool_names(server.server_for_mode("workflow")) == {"plan_query", "execute_query"}


def test_legacy_mode_keeps_run_sql_only():
    assert tool_names(server.server_for_mode("legacy")) == {"run_sql"}


def test_unknown_mode_fails_closed():
    with pytest.raises(RuntimeError, match="ENERGY_MCP_MODE"):
        server.server_for_mode("typo")


def test_workflow_health_fails_when_a_dependency_is_down(monkeypatch):
    def mongo_down():
        raise RuntimeError("mongo down")
    monkeypatch.setattr(server, "workflow_collection", mongo_down)
    response = asyncio.run(server.workflow_health(None))
    assert response.status_code == 503


def test_execute_query_runs_only_claimed_stored_sql(monkeypatch):
    stored = {"_id": "wf", "sql": "SELECT 42", "summary": "상수 조회"}
    monkeypatch.setattr(server, "claim_workflow", lambda collection, workflow_id: stored)
    execute = MagicMock(return_value={"columns": ["answer"], "rows": [{"answer": 42}],
                                      "row_count": 1, "truncated": False})
    monkeypatch.setattr(server, "_execute", execute)
    monkeypatch.setattr(server, "workflow_collection", lambda: MagicMock())

    result = server.execute_query("wf")

    execute.assert_called_once_with("SELECT 42")
    assert result["executed_sql"] == "SELECT 42"
    assert result["request_summary"] == "상수 조회"
```

- [ ] **Step 2: Run mode tests and verify RED**

Run: `cd mcp-server && uv run pytest tests/test_modes.py -q`

Expected: FAIL because `server_for_mode` and workflow tools do not exist.

- [ ] **Step 3: Register separate FastMCP instances**

```python
WORKFLOW_INSTRUCTIONS = """모호한 질문은 plan_query가 반환한 질문으로 구체화한다.
awaiting_confirmation이면 조건, SQL, 승인 링크를 보여주고 사용자가 승인했다고
말할 때까지 execute_query를 호출하지 않는다. 실행 결과에는 확정 조건과 실제 SQL을
표시한다. legacy run_sql은 이 서버에 없다.
"""

legacy_mcp = FastMCP("energy-mcp-legacy")
workflow_mcp = FastMCP(
    "energy-mcp",
    instructions=WORKFLOW_INSTRUCTIONS,
    stateless_http=True,
)
mcp = legacy_mcp  # 기존 import 호환


def server_for_mode(mode: str) -> FastMCP:
    if mode == "legacy":
        return legacy_mcp
    if mode == "workflow":
        return workflow_mcp
    raise RuntimeError("ENERGY_MCP_MODE는 legacy 또는 workflow여야 합니다.")
```

Change existing decorators to `@legacy_mcp.tool()` and
`@legacy_mcp.resource(RESOURCE_URI)`. Register workflow functions with
`@workflow_mcp.tool()` and call `register_approval_routes(workflow_mcp,
workflow_collection)` once at import.

Register `/health` as a custom route. It must call Mongo `ping` and run
`SELECT 1` through a readonly cursor; return 200 only when both succeed and
503 without exception text otherwise. This route contains no secret or schema
data.

`plan_query` must create/load a Mongo document, merge supplied answers, call
`_fetch_schema_markdown()`, call `plan_with_openai`, validate the SQL, and save
the decision. `execute_query` must claim first, call `_execute(stored["sql"])
once, finish the workflow, and return `request_summary`, `executed_sql`, and
the existing result. On `_execute` error, mark `failed` and re-raise without
changing SQL.

```python
@workflow_mcp.tool()
def plan_query(
    question: str,
    workflow_id: str | None = None,
    answers: dict[str, str] | None = None,
) -> dict[str, Any]:
    collection = workflow_collection()
    now = utcnow()
    if workflow_id is None:
        doc, workflow_id = new_workflow(question, now)
        collection.insert_one(doc)
    else:
        doc = collection.find_one({
            "_id": workflow_id,
            "status": "clarifying",
            "expires_at": {"$gt": now},
        })
        if doc is None:
            raise RuntimeError("구체화할 workflow가 없거나 만료됐습니다.")
        merged = {**doc.get("answers", {}), **(answers or {})}
        collection.update_one({"_id": workflow_id}, {"$set": {"answers": merged}})
        doc["answers"] = merged
    decision = plan_with_openai(
        doc["question"], doc.get("answers", {}), _fetch_schema_markdown()
    )
    if decision.status == "ready":
        decision.sql = validate_planned_sql(decision.sql)
    return save_decision(
        collection,
        workflow_id,
        decision,
        os.environ["ENERGY_MCP_APPROVAL_BASE_URL"],
        now,
    )


@workflow_mcp.tool()
def execute_query(workflow_id: str) -> dict[str, Any]:
    collection = workflow_collection()
    stored = claim_workflow(collection, workflow_id)
    if stored is None:
        raise RuntimeError("승인됐고 실행 가능한 workflow가 아닙니다.")
    try:
        result = _execute(stored["sql"])
    except Exception as exc:
        finish_workflow(collection, workflow_id, None, type(exc).__name__, utcnow())
        raise
    finish_workflow(collection, workflow_id, result, None, utcnow())
    return {**result, "request_summary": stored["summary"],
            "executed_sql": stored["sql"], "workflow_id": workflow_id}
```

Use one cached Mongo client:

```python
@functools.lru_cache(maxsize=1)
def workflow_collection():
    uri = os.environ.get("ENERGY_MCP_MONGO_URI")
    if not uri:
        raise RuntimeError("ENERGY_MCP_MONGO_URI가 설정되지 않았습니다.")
    collection = MongoClient(uri).get_default_database()["query_workflows"]
    collection.create_index("expires_at", expireAfterSeconds=0)
    return collection
```

Update `main()` to choose
`server_for_mode(os.environ.get("ENERGY_MCP_MODE", "legacy"))`, apply host and
port settings to that instance, and run it with the existing transport.

- [ ] **Step 4: Run all MCP package tests**

Run: `cd mcp-server && uv run pytest -q`

Expected: all tests PASS; no network, PostgreSQL, MongoDB, or OpenAI call.

- [ ] **Step 5: Commit Task 4 only**

```bash
git add mcp-server/energy_mcp/server.py mcp-server/tests/test_modes.py \
  mcp-server/tests/test_server.py
git commit -m "feat(mcp): 승인 workflow를 정식 mode로 분리"
```

---

### Task 5: 비밀값 파일 주입과 Mongo 인증

**Files:**
- Create: `docker/llm-demo/load-secrets.sh`
- Create: `docker/llm-demo/provision-secrets.sh`
- Create: `docker/llm-demo/init-mongo-users.js`
- Create: `tests/test_llm_service_config.py`
- Modify: `docker/llm-demo/mcp.Dockerfile`
- Modify: `docker/llm-demo/compose.yml`
- Modify: `.gitignore:135-139`

**Interfaces:**
- Produces: `/usr/local/bin/load-secrets <profile> <command...>`
- Produces profiles: `librechat`, `energy-mcp`, `pgbouncer`
- Consumes secret files under `/run/secrets`
- Produces Mongo users: `librechat_app@LibreChat`, `energy_mcp_app@energy_mcp`

- [ ] **Step 1: Write failing secret/config tests**

```python
# tests/test_llm_service_config.py
from pathlib import Path
import re

COMPOSE = Path("docker/llm-demo/compose.yml").read_text()
LOADER = Path("docker/llm-demo/load-secrets.sh")


def test_sensitive_values_are_not_compose_environment_or_env_file():
    assert "env_file:" not in COMPOSE
    for name in (
        "OPENAI_API_KEY", "JWT_SECRET", "JWT_REFRESH_SECRET",
        "CREDS_KEY", "CREDS_IV", "ENERGY_MCP_DSN", "DB_PASSWORD",
    ):
        assert not re.search(rf"^\s+{name}:\s+", COMPOSE, re.M)


def test_services_mount_secrets_and_use_loader():
    assert LOADER.exists()
    assert COMPOSE.count("/usr/local/bin/load-secrets:ro") >= 2
    assert "ENERGY_MCP_MODE: workflow" in COMPOSE
    assert "mongod --auth" in COMPOSE


def test_secret_loader_never_traces_or_prints_values():
    text = LOADER.read_text()
    assert "set -x" not in text
    assert "echo $" not in text
    assert "/run/secrets/" in text
```

- [ ] **Step 2: Run config tests and verify RED**

Run: `uv run pytest tests/test_llm_service_config.py -q`

Expected: FAIL because the loader and secret mounts do not exist.

- [ ] **Step 3: Implement one secret loader**

```bash
#!/usr/bin/env bash
set -eu

read_secret() {
    variable=$1
    file=/run/secrets/$2
    [ -r "$file" ] || { echo "필수 secret 파일이 없습니다: $2" >&2; exit 1; }
    value=$(sed -e 's/[[:space:]]*$//' "$file")
    export "$variable=$value"
    unset value
}

profile=${1:?profile이 필요합니다}
shift

case "$profile" in
    librechat)
        read_secret OPENAI_API_KEY openai_api_key
        read_secret JWT_SECRET jwt_secret
        read_secret JWT_REFRESH_SECRET jwt_refresh_secret
        read_secret CREDS_KEY creds_key
        read_secret CREDS_IV creds_iv
        read_secret MONGO_PASSWORD librechat_mongo_password
        export MONGO_URI="mongodb://librechat_app:${MONGO_PASSWORD}@mongodb:27017/LibreChat?authSource=LibreChat"
        unset MONGO_PASSWORD
        ;;
    energy-mcp)
        read_secret OPENAI_API_KEY openai_api_key
        read_secret ENERGY_MCP_DSN energy_mcp_dsn
        read_secret MONGO_PASSWORD energy_mcp_mongo_password
        export ENERGY_MCP_MONGO_URI="mongodb://energy_mcp_app:${MONGO_PASSWORD}@mongodb:27017/energy_mcp?authSource=energy_mcp"
        unset MONGO_PASSWORD
        ;;
    pgbouncer)
        read_secret DB_PASSWORD postgres_readonly_password
        ;;
    *) echo "알 수 없는 secret profile: $profile" >&2; exit 1 ;;
esac

exec "$@"
```

Generate Mongo passwords as hex so URI encoding is not needed. Set executable
mode on both shell scripts. The loader may name a missing secret but must never
print its value or length.

- [ ] **Step 4: Implement secret provisioning without terminal output**

`provision-secrets.sh` accepts exactly one absolute directory argument, creates
it with mode `0700`, uses `umask 077`, writes generated hex values without
printing them, and reads the replacement OpenAI key with `read -r -s`.

```sh
#!/bin/sh
set -eu
umask 077
secret_dir=${1:?사용법: provision-secrets.sh /absolute/secret/dir}
case "$secret_dir" in /*) ;; *) echo "절대경로가 필요합니다." >&2; exit 1 ;; esac
install -d -m 700 "$secret_dir"
openssl rand -hex 32 > "$secret_dir/jwt_secret"
openssl rand -hex 32 > "$secret_dir/jwt_refresh_secret"
openssl rand -hex 32 > "$secret_dir/creds_key"
openssl rand -hex 16 > "$secret_dir/creds_iv"
openssl rand -hex 32 > "$secret_dir/mongo_root_password"
openssl rand -hex 32 > "$secret_dir/librechat_mongo_password"
openssl rand -hex 32 > "$secret_dir/energy_mcp_mongo_password"
printf "새 OpenAI API key: " >&2
IFS= read -r -s openai_key
printf '\n' >&2
printf '%s' "$openai_key" > "$secret_dir/openai_api_key"
unset openai_key
printf "읽기전용 PostgreSQL 비밀번호: " >&2
IFS= read -r -s postgres_password
printf '\n' >&2
printf '%s' "$postgres_password" > "$secret_dir/postgres_readonly_password"
encoded_password=$(printf '%s' "$postgres_password" | python3 -c \
  'import sys, urllib.parse; print(urllib.parse.quote(sys.stdin.read(), safe=""), end="")')
printf 'postgresql://demo_ro:%s@pgbouncer:5432/pv' "$encoded_password" \
  > "$secret_dir/energy_mcp_dsn"
unset postgres_password encoded_password
```

Do not add a command that prints or validates secret contents. Add
`docker/llm-demo/secrets/` to `.gitignore` as a last-resort guard even though
the operational directory is outside the repository.

- [ ] **Step 5: Add one-time Mongo user bootstrap**

`init-mongo-users.js` runs once through MongoDB's localhost exception before
application services start. It reads secret files with `fs.readFileSync(...,
'utf8').trim()`. For each application DB, call `getUser`; create the user only
when absent with `readWrite` on its own DB. Create the root user in `admin`
only when absent. Never print passwords. After auth is active, do not rerun it
without authenticating as root.

```javascript
const fs = require('fs');
const secret = (name) => fs.readFileSync(`/run/secrets/${name}`, 'utf8').trim();

const ensureUser = (database, user, password, roles) => {
  const target = db.getSiblingDB(database);
  if (target.getUser(user) == null) target.createUser({ user, pwd: password, roles });
};

const admin = db.getSiblingDB('admin');
const rootPassword = secret('mongo_root_password');
if (admin.getUser('root') == null) {
  admin.createUser({ user: 'root', pwd: rootPassword, roles: [{ role: 'root', db: 'admin' }] });
}
if (!admin.auth('root', rootPassword)) throw new Error('Mongo root 인증 실패');
ensureUser('LibreChat', 'librechat_app', secret('librechat_mongo_password'),
  [{ role: 'readWrite', db: 'LibreChat' }]);
ensureUser('energy_mcp', 'energy_mcp_app', secret('energy_mcp_mongo_password'),
  [{ role: 'readWrite', db: 'energy_mcp' }]);
```

- [ ] **Step 6: Update images and Compose**

In `mcp.Dockerfile`, copy `uv` from its official image, sync the locked package,
copy `load-secrets.sh`, and keep the existing CSV export process behind the
loader:

```dockerfile
FROM ghcr.io/astral-sh/uv:0.8.17 AS uv
FROM python:3.11-slim
COPY --from=uv /uv /uvx /bin/
COPY mcp-server /opt/mcp-server
WORKDIR /opt/mcp-server
RUN uv sync --frozen --no-dev
ENV PATH="/opt/mcp-server/.venv/bin:$PATH"
COPY docker/llm-demo/serve_exports.py /serve_exports.py
COPY docker/llm-demo/load-secrets.sh /usr/local/bin/load-secrets
RUN chmod 0555 /usr/local/bin/load-secrets
EXPOSE 8000 8098
CMD ["/usr/local/bin/load-secrets", "energy-mcp", "sh", "-c", "mkdir -p /exports && python /serve_exports.py & exec energy-mcp"]
```

In Compose:

- mount the loader read-only into LibreChat and PgBouncer;
- set their entrypoint to the loader profile followed by the original command
  (`npm run backend` for LibreChat, `/entrypoint.sh /usr/bin/pgbouncer
  /etc/pgbouncer/pgbouncer.ini` for PgBouncer);
- set `ENERGY_MCP_MODE: workflow`, transport, host, row limits, approval base
  URL, and non-secret model name only;
- remove `env_file: librechat.env` and all sensitive environment entries;
- mount Mongo secret files and `init-mongo-users.js`, and run `mongod --auth
  --bind_ip_all`;
- use `${LLM_SECRET_DIR:?LLM_SECRET_DIR를 설정하세요}` for every Compose secret
  `file:` source;
- add healthchecks for Mongo (`mongosh` authenticated ping), energy-mcp HTTP,
  and LibreChat HTTP;
- use `depends_on.condition: service_healthy` where startup order matters.

The security-relevant Compose shape is:

```yaml
services:
  pgbouncer:
    entrypoint: ["/usr/local/bin/load-secrets", "pgbouncer", "/entrypoint.sh"]
    command: ["/usr/bin/pgbouncer", "/etc/pgbouncer/pgbouncer.ini"]
    volumes:
      - ./load-secrets.sh:/usr/local/bin/load-secrets:ro
    secrets: [postgres_readonly_password]

  energy-mcp:
    environment:
      ENERGY_MCP_MODE: workflow
      ENERGY_MCP_TRANSPORT: streamable-http
      ENERGY_MCP_HOST: 0.0.0.0
      ENERGY_MCP_APPROVAL_BASE_URL: ${LLM_PUBLIC_BASE_URL:?LLM_PUBLIC_BASE_URL을 설정하세요}/approval
      ENERGY_MCP_PLANNER_MODEL: gpt-4o-mini
      ENERGY_MCP_ROW_LIMIT: "10"
    secrets: [openai_api_key, energy_mcp_dsn, energy_mcp_mongo_password]
    depends_on:
      mongodb: {condition: service_healthy}
      pgbouncer: {condition: service_started}
    healthcheck:
      test: ["CMD", "python", "-c", "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8000/health', timeout=5)"]
      interval: 30s
      timeout: 10s
      retries: 3

  mongodb:
    command: ["mongod", "--auth", "--bind_ip_all"]
    volumes:
      - mongo-data:/data/db
      - ./init-mongo-users.js:/docker-entrypoint-initdb.d/init-mongo-users.js:ro
    secrets: [mongo_root_password, librechat_mongo_password, energy_mcp_mongo_password]
    healthcheck:
      test: ["CMD-SHELL", "mongosh --quiet --username root --password \"$$(cat /run/secrets/mongo_root_password)\" --authenticationDatabase admin --eval 'quit(db.runCommand({ping:1}).ok ? 0 : 2)'"]
      interval: 30s
      timeout: 10s
      retries: 5

  librechat:
    entrypoint: ["/usr/local/bin/load-secrets", "librechat"]
    command: ["npm", "run", "backend"]
    environment:
      HOST: 0.0.0.0
      ENDPOINTS: openAI,agents
      OPENAI_MODELS: gpt-4o-mini
      SEARCH: "false"
      ALLOW_REGISTRATION: "false"
    volumes:
      - ./load-secrets.sh:/usr/local/bin/load-secrets:ro
      - ./librechat.yaml:/app/librechat.yaml:ro
    secrets:
      - openai_api_key
      - jwt_secret
      - jwt_refresh_secret
      - creds_key
      - creds_iv
      - librechat_mongo_password
    depends_on:
      mongodb: {condition: service_healthy}
      energy-mcp: {condition: service_healthy}
    healthcheck:
      test: ["CMD", "node", "-e", "fetch('http://127.0.0.1:3080/api/health').then(r=>process.exit(r.ok?0:1)).catch(()=>process.exit(1))"]
      interval: 30s
      timeout: 10s
      retries: 5

secrets:
  openai_api_key: {file: "${LLM_SECRET_DIR:?LLM_SECRET_DIR를 설정하세요}/openai_api_key"}
  jwt_secret: {file: "${LLM_SECRET_DIR:?LLM_SECRET_DIR를 설정하세요}/jwt_secret"}
  jwt_refresh_secret: {file: "${LLM_SECRET_DIR:?LLM_SECRET_DIR를 설정하세요}/jwt_refresh_secret"}
  creds_key: {file: "${LLM_SECRET_DIR:?LLM_SECRET_DIR를 설정하세요}/creds_key"}
  creds_iv: {file: "${LLM_SECRET_DIR:?LLM_SECRET_DIR를 설정하세요}/creds_iv"}
  postgres_readonly_password: {file: "${LLM_SECRET_DIR:?LLM_SECRET_DIR를 설정하세요}/postgres_readonly_password"}
  energy_mcp_dsn: {file: "${LLM_SECRET_DIR:?LLM_SECRET_DIR를 설정하세요}/energy_mcp_dsn"}
  mongo_root_password: {file: "${LLM_SECRET_DIR:?LLM_SECRET_DIR를 설정하세요}/mongo_root_password"}
  librechat_mongo_password: {file: "${LLM_SECRET_DIR:?LLM_SECRET_DIR를 설정하세요}/librechat_mongo_password"}
  energy_mcp_mongo_password: {file: "${LLM_SECRET_DIR:?LLM_SECRET_DIR를 설정하세요}/energy_mcp_mongo_password"}
```

- [ ] **Step 7: Verify config tests and rendered Compose contain no value**

Run: `uv run pytest tests/test_llm_service_config.py -q`

Expected: PASS.

Run with a disposable directory containing dummy values:

```bash
LLM_SECRET_DIR=/tmp/energy-llm-plan-secrets \
  docker compose -f docker/llm-demo/compose.yml config > /tmp/energy-llm-compose.txt
```

Expected: config succeeds; `rg -n 'sk-|JWT_SECRET:|CREDS_KEY:|ENERGY_MCP_DSN:|DB_PASSWORD:' /tmp/energy-llm-compose.txt` prints nothing. Delete only these two exact `/tmp/energy-llm-*` paths after inspection.

- [ ] **Step 8: Commit Task 5 only**

```bash
git add .gitignore tests/test_llm_service_config.py docker/llm-demo/load-secrets.sh \
  docker/llm-demo/provision-secrets.sh docker/llm-demo/init-mongo-users.js \
  docker/llm-demo/mcp.Dockerfile docker/llm-demo/compose.yml
git commit -m "fix(llm): 런타임 비밀값을 Docker secret으로 이동"
```

---

### Task 6: LibreChat 연결, 승인 proxy, 정식 사용자 문서

**Files:**
- Modify: `docker/llm-demo/librechat.yaml:11-36`
- Modify: `docker/llm-demo/nginx.conf:12-36`
- Modify: `docs/gitbook/03-llm-mcp.md:1-133`
- Modify: `tests/test_gitbook_docs.py:47-60`
- Modify: `tests/test_llm_service_config.py`

**Interfaces:**
- Produces: LibreChat `energy-db` connection with `serverInstructions: true`
- Produces: nginx `/approval/` proxy to `energy-mcp:8000`
- Produces: user-visible clarification and confirmation instructions

- [ ] **Step 1: Extend failing configuration/document tests**

```python
def test_librechat_uses_server_instructions_and_approval_proxy():
    librechat = Path("docker/llm-demo/librechat.yaml").read_text()
    nginx = Path("docker/llm-demo/nginx.conf").read_text()
    assert "serverInstructions: true" in librechat
    assert "location /approval/" in nginx
    assert "proxy_pass http://energy-mcp:8000" in nginx
    assert "access_log off" in nginx
```

Add these requirements to `test_mcp_guide_uses_the_same_personal_database_role`:

```python
for required in ("구체화", "조건 요약", "승인", "실제 실행 SQL"):
    assert required in text
```

- [ ] **Step 2: Run focused tests and verify RED**

Run: `uv run pytest tests/test_llm_service_config.py tests/test_gitbook_docs.py::test_mcp_guide_uses_the_same_personal_database_role -q`

Expected: FAIL because server instructions, approval proxy, and formal workflow copy are absent.

- [ ] **Step 3: Enable server instructions and proxy only the approval path**

Add `serverInstructions: true` under `mcpServers.energy-db`. In nginx, put the
more specific location before `/`:

```nginx
location /approval/ {
    proxy_pass http://energy-mcp:8000;
    proxy_set_header Host $host;
    proxy_set_header X-Forwarded-Proto $scheme;
    proxy_buffering off;
    access_log off; # URL bearer token을 access log에 남기지 않는다
}
```

Do not trust `X-Forwarded-For` for identity in this task. IP identity remains
deferred.

- [ ] **Step 4: Rewrite the user flow as a formal service**

Update `03-llm-mcp.md` so the first path is the hosted formal LibreChat service,
not a demo. Explain exactly:

1. an abstract question may produce one or more clarification questions;
2. the assistant shows normalized conditions, SQL, and a 30-minute approval link;
3. the user reviews and clicks approve or decline;
4. only after approval does `execute_query` run;
5. any changed SQL requires a new approval;
6. the final answer includes conditions and actual SQL;
7. local stdio `run_sql` remains an advanced legacy path and has no approval
   workflow, so it is not the recommended formal route.

Keep the existing warnings about external OpenAI processing, read-only roles,
time conventions, quality filters, truncation, and direct SQL verification.

- [ ] **Step 5: Run documentation/config tests**

Run: `uv run pytest tests/test_llm_service_config.py tests/test_gitbook_docs.py -q`

Expected: PASS.

- [ ] **Step 6: Commit Task 6 only**

```bash
git add docker/llm-demo/librechat.yaml docker/llm-demo/nginx.conf \
  docs/gitbook/03-llm-mcp.md tests/test_gitbook_docs.py tests/test_llm_service_config.py
git commit -m "docs(llm): 승인 기반 정식 MCP 조회 절차 안내"
```

---

### Task 7: 전체 검증과 운영 전환 준비

**Files:**
- Inspect: files changed in Tasks 1-6
- No production data mutation in automated verification

**Interfaces:**
- Consumes: complete workflow mode and secret-enabled Compose
- Produces: reproducible test/build evidence and explicit manual rotation handoff

- [ ] **Step 1: Run the package and repository suites**

Run: `cd mcp-server && uv run pytest -q`

Expected: PASS.

Run: `uv run pytest -q`

Expected: PASS. If unrelated dirty-worktree tests fail, record the exact existing
failure and run every MCP, config, docs, and security-focused test explicitly;
do not alter unrelated collector code.

- [ ] **Step 2: Build the affected MCP image without starting services**

Run: `LLM_SECRET_DIR=/tmp/energy-llm-plan-secrets docker compose -f docker/llm-demo/compose.yml build energy-mcp`

Expected: image builds from the locked MCP package.

Run: `LLM_SECRET_DIR=/tmp/energy-llm-plan-secrets docker compose -f docker/llm-demo/compose.yml config --quiet`

Expected: exit 0.

- [ ] **Step 3: Verify secrets are absent from tracked files and image config**

Run:

```bash
git grep -n -E 'sk-[A-Za-z0-9_-]{20,}|JWT_SECRET=[^$]|CREDS_KEY=[^$]|postgresql://[^<[:space:]]+:[^<[:space:]@]+@' -- . ':!uv.lock'
```

Expected: no live credential match.

Run after building but before production restart:

```bash
docker image inspect llm-demo-energy-mcp --format '{{json .Config.Env}}' |
  rg 'sk-|postgresql://|mongodb://'
```

Expected: no output.

- [ ] **Step 4: Prepare the manual rotation checklist without exposing values**

The operator performs these actions in this order:

1. Revoke the exposed OpenAI API key in the OpenAI account.
2. Create `/etc/energy-llm/secrets` using `provision-secrets.sh`; enter only the
   newly issued OpenAI key and the existing `demo_ro` PostgreSQL password.
3. Set `LLM_SECRET_DIR=/etc/energy-llm/secrets` in the operator shell, not a
   tracked file.
4. Start only MongoDB, then run
   `docker exec librechat-mongo mongosh --quiet /docker-entrypoint-initdb.d/init-mongo-users.js`.
5. Restart MongoDB with auth and verify authenticated ping.
6. Recreate PgBouncer, energy-mcp, LibreChat, and nginx.
7. Existing LibreChat sessions and encrypted provider credentials may become
   invalid after JWT/CREDS rotation; sign in again and re-enter provider
   credentials when prompted.
8. Run
   `docker inspect librechat energy-mcp llm-demo-pgbouncer --format '{{json .Config.Env}}'`
   and confirm it contains secret variable names at most, never values, DSNs,
   `sk-` prefixes, or Mongo credentials. Run `docker logs --tail 200` for the
   same three containers and apply the same check.

Do not execute this production rotation until the operator confirms the new
OpenAI key is available. Never retrieve the old key from `docker inspect`.

- [ ] **Step 5: Run a manual acceptance workflow after rotation**

Use these exact prompts:

1. `발전 효율이 좋은 발전소를 알려줘.`
   Expected: clarification question; no SQL execution.
2. Answer: `2025년 태양광 이용률로 계산하고 월별로 보여줘.`
   Expected: condition summary, SQL, approval link; no DB result yet.
3. Open the link and decline.
   Expected: `execute_query` refuses.
4. Repeat, approve, then tell the chat `승인했어.`
   Expected: one execution; final response shows the approved summary and exact
   SQL.
5. Call `execute_query` again with the same workflow ID.
   Expected: refusal; PostgreSQL audit log has only one query for that ID.

- [ ] **Step 6: Commit any verification-only correction and stop before IP work**

If verification required a correction, commit only the affected Task 1-6 files
with a message describing that correction. Do not add Tailscale binds, grants,
IP allowlists, real-IP headers, or principal matching in this plan.
