from __future__ import annotations

import json
import os
import re
from datetime import datetime
from typing import Literal

from openai import OpenAI
from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator

PLANNER_MODEL_ENV = "ENERGY_MCP_PLANNER_MODEL"
DEFAULT_PLANNER_MODEL = "gpt-4o-mini"

PLANNER_PROMPT = """당신은 PostgreSQL research 스키마 전용 질의 계획기다.
기상예보 요청의 우선 규칙: 질문과 answers를 함께 읽고 예보종·시도·시군구·읍면동·요소·기간이
모두 있으면 반드시 ready로 답한다. 추가 질문은 누락된 조건에만 허용한다.
지역 적합성, 요소나 월별 자료의 존재는 서버가 검증하므로 사용자에게 확인을 요구하지 않는다.
사용자 의도를 임의로 채우지 마라. 대상, 지표, 기간, 집계 단위, 중요한 필터 중
SQL 결과를 바꿀 정보가 빠졌으면 needs_clarification과 짧은 질문만 반환한다.
정보가 충분하면 ready, 한국어 조건 요약, 단일 읽기전용 PostgreSQL SELECT 또는
WITH...SELECT를 반환한다. 제공된 스키마 밖 이름을 만들지 마라. 세미콜론을 쓰지 마라.
연료를 지정한 질문은 fuel_type을 필터하고, 집계 질문은 GROUP BY와 집계함수를 쓴다.
실제 기상예보 값/CSV 요청은 forecast 필드를 채운다. 원시 추출이면 sql=null이다.
예보 집계 SQL은 forecast_data를 참조하는 SELECT로 작성한다. research.forecast()를 직접 호출하지 않는다.
forecast_data는 서버가 만드는 임시 CTE 이름이다. research.forecast_data라는 테이블은 없다.
forecast에는 예보종, 시도, 시군구, 읍면동, 기상요소, 시작월, 종료월을 모두 넣는다.
지역이나 요소, 기간이 빠졌으면 질문한다. 시도·시군구를 읍면동으로 대신 넣지 마라.
연간 요청은 시작 YYYY01, 종료 YYYY12다. 지역 목록은 기상 값 데이터가 아니다.
answers는 앞선 추가 질문에 대한 답변이다. 이미 답한 조건을 다시 묻지 않는다.
자연어 질문에서도 조건을 추출한다. 추가 답변이 비어 있어도 질문에 있는 조건은 제공된 것이다.
예보종·시도·시군구·읍면동·요소·시작월·종료월이 모두 주어졌으면 ready를 반환한다.
행정구역과 요소의 실제 존재, 제공 가능한 월은 서버가 NAS에서 검증한다.
이 검증을 사용자에게 요구하지 않는다. 지역이 맞는지 확인하는 질문을 반복하지 않는다.
예시: question="서울 예보 CSV", answers={"시도":"서울특별시","시군구":"강남구",
"읍면동":"개포1동","예보종":"단기예보","요소":"1시간기온","기간":"2023년 1월 한 달",
"출력":"원시 전체 CSV"}이면 아래와 같이 답한다:
{"status":"ready","questions":[],"conditions":[],"summary":"개포1동 2023년 1월 기온 예보",
"sql":null,"forecast":{"forecast_type":"단기예보","sido":"서울특별시","sigungu":"강남구",
"dong":"개포1동","element":"1시간기온","from_ym":"202301","to_ym":"202301"}}
열수요 값 요청은 research.heat_demand를 조회한다. heat_demand_location은 지사 정보뿐이다.
"""


class Condition(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    value: str


class ForecastRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    forecast_type: Literal["단기예보", "초단기예보", "초단기실황"]
    sido: str = Field(min_length=1)
    sigungu: str = Field(min_length=1)
    dong: str = Field(min_length=1)
    element: str = Field(min_length=1)
    from_ym: str
    to_ym: str

    @model_validator(mode="after")
    def validate_period(self):
        for month in (self.from_ym, self.to_ym):
            if not re.fullmatch(r"[0-9]{6}", month):
                raise ValueError("예보 기간은 YYYYMM 형식이어야 합니다.")
            datetime.strptime(month, "%Y%m")
        if self.from_ym > self.to_ym:
            raise ValueError("시작월은 종료월보다 늦을 수 없습니다.")
        return self


class PlannerDecision(BaseModel):
    model_config = ConfigDict(extra="forbid")

    status: Literal["needs_clarification", "ready"]
    questions: list[str] = Field(default_factory=list)
    conditions: list[Condition] = Field(default_factory=list)
    summary: str | None = None
    sql: str | None = None
    forecast: ForecastRequest | None = None

    @model_validator(mode="after")
    def validate_state(self):
        names = [condition.name for condition in self.conditions]
        if len(names) != len(set(names)):
            raise ValueError("조건 이름은 중복될 수 없습니다.")
        if self.status == "ready":
            if not self.summary or not (self.sql or self.forecast):
                raise ValueError("ready 응답에는 summary와 sql 또는 forecast가 필요합니다.")
            if self.sql and re.search(r"research\s*\.\s*forecast\s*\(", self.sql, re.I):
                raise ValueError("예보 값 조회는 sql 대신 forecast 조건을 지정해야 합니다.")
            if self.sql and self.forecast and (not self.sql.lstrip().upper().startswith("SELECT ")
                                               or not re.search(r"\bforecast_data\b", self.sql, re.I)
                                               or re.search(r"\.\s*\"?forecast_data\b", self.sql, re.I)):
                raise ValueError("예보 집계 SQL은 forecast_data를 참조하는 SELECT여야 합니다.")
        if self.status == "needs_clarification" and not self.questions:
            raise ValueError("needs_clarification 응답에는 질문이 필요합니다.")
        return self

    def condition_mapping(self) -> dict[str, str]:
        conditions = {condition.name: condition.value for condition in self.conditions}
        if self.forecast:
            names = ("예보종", "시도", "시군구", "읍면동", "요소", "시작월", "종료월")
            conditions.update(zip(names, self.forecast.model_dump().values()))
        return conditions


def plan_with_openai(
    question: str,
    answers: dict[str, str],
    schema_md: str,
    model: str | None = None,
    client=None,
) -> PlannerDecision:
    client = client or OpenAI()
    messages = [
        {"role": "system", "content": PLANNER_PROMPT},
        {"role": "developer", "content": schema_md},
        {"role": "user", "content": question + "\n추가 답변: " + json.dumps(answers, ensure_ascii=False)},
    ]
    for attempt in range(2):
        try:
            response = client.responses.parse(
                model=model or os.environ.get(PLANNER_MODEL_ENV, DEFAULT_PLANNER_MODEL),
                store=False, input=messages, text_format=PlannerDecision,
            )
            break
        except ValidationError as exc:
            if attempt:
                raise
            error_messages = "; ".join(error["msg"] for error in exc.errors())
            messages.append({"role": "developer", "content":
                f"계획 검증 오류: {error_messages}. 필수 조건이 있으면 ready로 답하세요. "
                "예보 값 요청은 forecast에 일곱 조건을 넣고 원시 CSV라면 sql=null로 하세요. "
                "집계 SQL은 SELECT ... FROM forecast_data만 쓰고 research.forecast()는 호출하지 마세요. "
                "forecast_data는 임시 CTE이므로 research. 같은 스키마를 붙이지 마세요. "
                "다른 요청은 summary와 단일 읽기전용 SQL을 정확하게 작성하세요."})
    if response.output_parsed is None:
        raise RuntimeError("planner가 구조화된 응답을 반환하지 않았습니다.")
    return response.output_parsed
