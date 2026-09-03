from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from energy_mcp import workflow
from energy_mcp.planner import PlannerDecision
from energy_mcp.workflow import (
    WORKFLOW_TTL,
    _hash,
    approve_workflow,
    claim_workflow,
    decline_workflow,
    finish_workflow,
    issue_csrf,
    new_workflow,
    save_decision,
    validate_planned_sql,
)

NOW = datetime(2026, 9, 2, 10, 0, tzinfo=timezone.utc)


def decision(status, *, questions=None, conditions=None, summary=None, sql=None):
    return PlannerDecision(
        status=status,
        questions=questions or [],
        conditions=[
            {"name": name, "value": value}
            for name, value in (conditions or {}).items()
        ],
        summary=summary,
        sql=sql,
    )


def test_clarification_never_stores_sql():
    collection = MagicMock()
    collection.update_one.return_value.matched_count = 1
    doc, _ = new_workflow("효율 좋은 발전소", NOW)
    collection.find_one.return_value = doc
    planned = decision(
        "needs_clarification",
        questions=["효율을 이용률로 계산할까요?"],
    )

    result = save_decision(
        collection, doc["_id"], 0, planned, "https://mcp/approval", NOW
    )

    assert result["status"] == "needs_clarification"
    query, update_doc = collection.update_one.call_args.args
    assert query["revision"] == 0
    update = update_doc["$set"]
    assert "sql" not in update
    assert "approval_token_hash" not in update
    assert result["conditions"] == {}


def test_ready_plan_is_saved_without_execution():
    collection = MagicMock()
    collection.update_one.return_value.matched_count = 1
    doc, _ = new_workflow("2025년 구미태양광 월별 발전량", NOW)
    collection.find_one.return_value = doc
    planned = decision(
        "ready",
        conditions={"대상": "구미태양광", "기간": "2025년", "집계": "월별 합계"},
        summary="구미태양광의 2025년 월별 발전량 합계",
        sql="SELECT date_trunc('month', timestamp), sum(gen_kwh) FROM research.generation GROUP BY 1",
    )

    result = save_decision(
        collection, doc["_id"], 0, planned, "https://mcp/approval", NOW
    )

    assert result["status"] == "awaiting_confirmation"
    assert result["approval_url"].startswith("https://mcp/approval/")
    query, update_doc = collection.update_one.call_args.args
    assert query["revision"] == 0
    update = update_doc["$set"]
    assert update["status"] == "awaiting_confirmation"
    assert update["expires_at"] == NOW + WORKFLOW_TTL
    assert update["sql_sha256"] == _hash(update["sql"])
    assert update["conditions"] == {
        "대상": "구미태양광",
        "기간": "2025년",
        "집계": "월별 합계",
    }
    assert result["conditions"] == update["conditions"]
    assert "approval_token" not in update


def test_new_workflow_starts_at_revision_zero():
    doc, _ = new_workflow("발전량", NOW)

    assert doc["revision"] == 0


def test_stale_planner_decision_cannot_overwrite_a_newer_revision():
    collection = MagicMock()
    collection.update_one.return_value.matched_count = 0

    with pytest.raises(RuntimeError, match="다른 구체화 요청"):
        save_decision(
            collection,
            "wf",
            3,
            decision("needs_clarification", questions=["기간은?"]),
            "https://mcp/approval",
            NOW,
        )

    assert collection.update_one.call_args.args[0]["revision"] == 3


def test_planning_failure_transition_is_guarded_by_revision_and_status():
    collection = MagicMock()
    collection.update_one.return_value.matched_count = 1

    assert workflow.fail_planning(collection, "wf", 4, "ValidationError", NOW) is True

    query, update = collection.update_one.call_args.args
    assert query == {
        "_id": "wf",
        "status": "clarifying",
        "revision": 4,
        "expires_at": {"$gt": NOW},
    }
    assert update == {
        "$set": {"status": "failed", "error_code": "ValidationError"}
    }


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


def test_approval_consumes_matching_one_time_csrf_token():
    collection = MagicMock()
    collection.update_one.return_value.matched_count = 1

    assert approve_workflow(collection, "wf", "approval", "csrf", NOW) is True
    query, update = collection.update_one.call_args.args
    assert query["_id"] == "wf"
    assert query["status"] == "awaiting_confirmation"
    assert query["expires_at"] == {"$gt": NOW}
    assert query["approval_token_hash"]
    assert query["approval_csrf_hash"]
    assert query["$expr"] == {"$eq": ["$sql_sha256", "$approval_sql_sha256"]}
    assert update["$set"]["status"] == "confirmed"
    assert update["$unset"] == {"approval_csrf_hash": ""}


def test_decline_uses_same_atomic_confirmation_guard():
    collection = MagicMock()
    collection.update_one.return_value.matched_count = 0

    assert decline_workflow(collection, "wf", "approval", "csrf", NOW) is False
    query, update = collection.update_one.call_args.args
    assert set(query) == {
        "$expr",
        "_id",
        "status",
        "expires_at",
        "approval_token_hash",
        "approval_csrf_hash",
    }
    assert query["$expr"] == {"$eq": ["$sql_sha256", "$approval_sql_sha256"]}
    assert update["$set"]["status"] == "declined"
    assert update["$unset"] == {"approval_csrf_hash": ""}


def test_issue_csrf_binds_nonce_to_the_exact_displayed_sql_hash():
    collection = MagicMock()
    collection.update_one.return_value.matched_count = 1
    sql = "SELECT 1"
    sql_sha256 = _hash(sql)

    csrf = issue_csrf(collection, "wf", sql, sql_sha256, NOW)

    query, update = collection.update_one.call_args.args
    assert csrf
    assert query == {
        "_id": "wf",
        "status": "awaiting_confirmation",
        "expires_at": {"$gt": NOW},
        "sql": sql,
        "sql_sha256": sql_sha256,
    }
    assert update["$set"] == {
        "approval_csrf_hash": _hash(csrf),
        "approval_sql_sha256": sql_sha256,
    }


def test_finish_records_summary_not_result_rows():
    collection = MagicMock()

    finish_workflow(
        collection,
        "wf",
        {"row_count": 2, "rows": [{"secret": "not persisted"}]},
        None,
        125,
        NOW,
    )

    query, update = collection.update_one.call_args.args
    assert query == {"_id": "wf", "status": "executing"}
    assert update["$set"] == {
        "status": "done",
        "executed_at": NOW,
        "duration_ms": 125,
        "row_count": 2,
        "error_code": None,
    }


def test_finish_marks_execution_failed_without_storing_result_rows():
    collection = MagicMock()

    finish_workflow(
        collection,
        "wf",
        {"rows": [{"secret": "not persisted"}]},
        "db_error",
        9,
        NOW,
    )

    assert collection.update_one.call_args.args[1]["$set"] == {
        "status": "failed",
        "executed_at": NOW,
        "duration_ms": 9,
        "row_count": None,
        "error_code": "db_error",
    }


def test_finish_treats_any_error_code_as_failure():
    collection = MagicMock()

    finish_workflow(collection, "wf", None, "", 0, NOW)

    assert collection.update_one.call_args.args[1]["$set"]["status"] == "failed"


@pytest.mark.parametrize("query", ["UPDATE research.plants SET name = 'x'", "", "SELECT 1; SELECT 2"])
def test_planned_sql_rejects_non_single_select_or_with(query):
    with pytest.raises(ValueError):
        validate_planned_sql(query)
