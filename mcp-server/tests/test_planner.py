import json

import httpx
import pytest
from openai import OpenAI
from pydantic import ValidationError

from energy_mcp.planner import PLANNER_PROMPT, PlannerDecision, plan_with_openai


EXPECTED_SCHEMA = {
    "$defs": {
        "Condition": {
            "additionalProperties": False,
            "properties": {
                "name": {"title": "Name", "type": "string"},
                "value": {"title": "Value", "type": "string"},
            },
            "required": ["name", "value"],
            "title": "Condition",
            "type": "object",
        }
    },
    "additionalProperties": False,
    "properties": {
        "status": {
            "enum": ["needs_clarification", "ready"],
            "title": "Status",
            "type": "string",
        },
        "questions": {
            "items": {"type": "string"},
            "title": "Questions",
            "type": "array",
        },
        "conditions": {
            "items": {"$ref": "#/$defs/Condition"},
            "title": "Conditions",
            "type": "array",
        },
        "summary": {
            "anyOf": [{"type": "string"}, {"type": "null"}],
            "title": "Summary",
        },
        "sql": {
            "anyOf": [{"type": "string"}, {"type": "null"}],
            "title": "Sql",
        },
    },
    "required": ["status", "questions", "conditions", "summary", "sql"],
    "title": "PlannerDecision",
    "type": "object",
}


def _sdk_client(answer: dict, captured: dict) -> OpenAI:
    def respond(request: httpx.Request) -> httpx.Response:
        captured.update(json.loads(request.content))
        return httpx.Response(
            200,
            json={
                "id": "resp_offline",
                "object": "response",
                "created_at": 0,
                "model": "gpt-4o-mini",
                "output": [
                    {
                        "id": "msg_offline",
                        "type": "message",
                        "role": "assistant",
                        "status": "completed",
                        "content": [
                            {
                                "type": "output_text",
                                "text": json.dumps(answer, ensure_ascii=False),
                                "annotations": [],
                            }
                        ],
                    }
                ],
                "parallel_tool_calls": True,
                "tool_choice": "auto",
                "tools": [],
            },
        )

    return OpenAI(
        api_key="offline-test-key",
        http_client=httpx.Client(transport=httpx.MockTransport(respond)),
    )


def test_real_sdk_serializes_strict_planner_schema_and_exact_request():
    captured = {}
    client = _sdk_client(
        {
            "status": "needs_clarification",
            "questions": ["기간을 알려주세요."],
            "conditions": [{"name": "대상", "value": "태양광"}],
            "summary": None,
            "sql": None,
        },
        captured,
    )

    result = plan_with_openai(
        "태양광 비교해줘",
        {"지역": "제주"},
        "research.generation(timestamp, fuel_type, gen_kwh)",
        client=client,
    )

    assert result.condition_mapping() == {"대상": "태양광"}
    assert set(captured) == {"input", "model", "store", "text"}
    assert captured["model"] == "gpt-4o-mini"
    assert captured["store"] is False
    assert captured["input"] == [
        {"role": "system", "content": PLANNER_PROMPT},
        {
            "role": "user",
            "content": json.dumps(
                {
                    "question": "태양광 비교해줘",
                    "answers": {"지역": "제주"},
                    "schema": "research.generation(timestamp, fuel_type, gen_kwh)",
                },
                ensure_ascii=False,
            ),
        },
    ]
    assert captured["text"]["format"] == {
        "type": "json_schema",
        "strict": True,
        "name": "PlannerDecision",
        "schema": EXPECTED_SCHEMA,
    }

    def assert_strict_objects(node):
        if isinstance(node, dict):
            if node.get("type") == "object":
                assert node.get("additionalProperties") is False
            for value in node.values():
                assert_strict_objects(value)
        elif isinstance(node, list):
            for value in node:
                assert_strict_objects(value)

    assert_strict_objects(captured["text"]["format"]["schema"])


def test_planner_rejects_duplicate_condition_names():
    with pytest.raises(ValidationError, match="조건 이름은 중복될 수 없습니다"):
        PlannerDecision(
            status="needs_clarification",
            questions=["기간은?"],
            conditions=[
                {"name": "대상", "value": "태양광"},
                {"name": "대상", "value": "풍력"},
            ],
        )


def test_ready_decision_requires_summary_and_sql():
    with pytest.raises(ValidationError):
        PlannerDecision(status="ready", questions=[], conditions=[])
