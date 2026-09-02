from __future__ import annotations

import hashlib
import secrets
from datetime import datetime, timedelta, timezone
from urllib.parse import quote

from pymongo import ReturnDocument

WORKFLOW_TTL = timedelta(minutes=30)


def _reject_multi_statement(query: str) -> None:
    """세미콜론으로 이어진 여러 문장을 거부한다."""
    body = query.strip()
    if not body:
        raise ValueError("빈 쿼리입니다.")
    if body.endswith(";"):
        body = body[:-1]
    if ";" in body:
        raise ValueError(
            "한 번에 하나의 SQL 문장만 실행할 수 있습니다. "
            "세미콜론으로 여러 문장을 연결하지 마세요."
        )


def validate_planned_sql(query: str) -> str:
    _reject_multi_statement(query)
    query = query.strip().removesuffix(";").strip()
    first = query.split(None, 1)[0].upper()
    if first not in {"SELECT", "WITH"}:
        raise ValueError("planner SQL은 SELECT 또는 WITH로 시작해야 합니다.")
    return query


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
    sql = validate_planned_sql(decision.sql)
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


def issue_csrf(collection, workflow_id: str, now: datetime | None = None) -> str:
    now = now or utcnow()
    token = secrets.token_urlsafe(32)
    changed = collection.update_one(
        {"_id": workflow_id, "status": "awaiting_confirmation", "expires_at": {"$gt": now}},
        {"$set": {"approval_csrf_hash": _hash(token)}},
    )
    if changed.matched_count != 1:
        raise RuntimeError("workflow가 만료됐거나 이미 처리됐습니다.")
    return token


def _decide_workflow(collection, workflow_id, approval_token, csrf_token, status, now):
    changed = collection.update_one(
        {
            "_id": workflow_id,
            "status": "awaiting_confirmation",
            "expires_at": {"$gt": now},
            "approval_token_hash": _hash(approval_token),
            "approval_csrf_hash": _hash(csrf_token),
        },
        {"$set": {"status": status, f"{status}_at": now},
         "$unset": {"approval_csrf_hash": ""}},
    )
    return changed.matched_count == 1


def approve_workflow(collection, workflow_id: str, approval_token: str, csrf_token: str,
                     now: datetime | None = None) -> bool:
    return _decide_workflow(
        collection, workflow_id, approval_token, csrf_token, "confirmed", now or utcnow()
    )


def decline_workflow(collection, workflow_id: str, approval_token: str, csrf_token: str,
                     now: datetime | None = None) -> bool:
    return _decide_workflow(
        collection, workflow_id, approval_token, csrf_token, "declined", now or utcnow()
    )


def claim_workflow(collection, workflow_id: str, now: datetime | None = None):
    now = now or utcnow()
    return collection.find_one_and_update(
        {"_id": workflow_id, "status": "confirmed", "expires_at": {"$gt": now}},
        {"$set": {"status": "executing", "claimed_at": now}},
        return_document=ReturnDocument.AFTER,
    )


def finish_workflow(collection, workflow_id: str, result: dict | None,
                    error_code: str | None, now: datetime | None = None) -> None:
    now = now or utcnow()
    collection.update_one(
        {"_id": workflow_id, "status": "executing"},
        {"$set": {
            "status": "failed" if error_code is not None else "done",
            "finished_at": now,
            "row_count": result.get("row_count") if result else None,
            "error_code": error_code,
        }},
    )
