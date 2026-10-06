import json

import httpx
import pytest
from openai import OpenAI
from pydantic import ValidationError

from energy_mcp.planner import PLANNER_PROMPT, ForecastRequest, PlannerDecision, plan_with_openai


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
    answers = iter(answer) if isinstance(answer, list) else None
    def respond(request: httpx.Request) -> httpx.Response:
        current = next(answers) if answers is not None else answer
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
                                "text": json.dumps(current, ensure_ascii=False),
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
        {"role": "developer", "content": "research.generation(timestamp, fuel_type, gen_kwh)"},
        {
            "role": "user",
            "content": '태양광 비교해줘\n추가 답변: {"지역": "제주"}',
        },
    ]
    schema = captured["text"]["format"]["schema"]
    assert captured["text"]["format"]["strict"] is True
    assert captured["text"]["format"]["name"] == "PlannerDecision"
    assert schema["properties"]["forecast"] == {"anyOf": [{"$ref": "#/$defs/ForecastRequest"}, {"type": "null"}]}
    assert "forecast" in schema["required"]
    assert schema["$defs"]["ForecastRequest"]["required"] == [
        "forecast_type", "sido", "sigungu", "dong", "element", "from_ym", "to_ym"
    ]
    assert schema["$defs"]["ForecastRequest"]["properties"]["forecast_type"]["enum"] == [
        "단기예보", "초단기예보", "초단기실황"
    ]
    # 기존 일반 SQL 계획 계약도 유지한다.
    for name, field in EXPECTED_SCHEMA["properties"].items():
        assert schema["properties"][name] == field

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


def test_invalid_forecast_sql_is_replanned_once_before_it_can_be_approved():
    forecast = dict(forecast_type="단기예보", sido="서울특별시", sigungu="중구", dong="필동",
                    element="1시간기온", from_ym="202301", to_ym="202301")
    wrong = dict(status="ready", questions=[], conditions=[], summary="기온 예보", forecast=forecast,
                 sql="SELECT * FROM research.forecast('단기예보','서울특별시','필동')")
    correct = wrong | {"sql": None}
    captured = {}
    client = _sdk_client([wrong, correct], captured)
    result = plan_with_openai("필동 2023년 1월 예보", {}, "schema", client=client)
    assert result.forecast.dong == "필동"
    assert result.sql is None
    assert len(captured["input"]) == 4
    assert "예보 값 조회는 sql 대신 forecast" in captured["input"][-1]["content"]
