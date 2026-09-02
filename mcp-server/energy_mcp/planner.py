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


def plan_with_openai(
    question: str,
    answers: dict[str, str],
    schema_md: str,
    model: str | None = None,
    client=None,
) -> PlannerDecision:
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
