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
