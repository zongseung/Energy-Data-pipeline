# energy-mcp

호스팅 LibreChat에서 PostgreSQL `research` 스키마를 계획하고 읽기전용으로
조회하는 MCP 서버입니다. 운영 `workflow` 모드는 `plan_query`와 `execute_query`를
제공하며, 승인 없이 즉시 실행하는 레거시 `run_sql` 도구는 제공하지 않습니다.

## 호스팅 workflow

LibreChat은 Streamable HTTP로 MCP 서버에 연결합니다. 질문, 구체화된 조건, SQL,
승인 상태는 MongoDB workflow에 저장합니다. 승인 페이지는 상태만 바꾸며 조회를
실행하지 않습니다. 사용자가 승인 후 채팅으로 돌아와 알리면 `execute_query`가
저장된 승인 SQL을 한 번 실행합니다.

계획 응답은 정보가 부족하면 `needs_clarification`과 추가 질문을 반환합니다. 조건이
충분하면 정규화된 조건, SQL, 승인 링크를 돌려줍니다. workflow는 30분이 지나면
사용할 수 없고, PostgreSQL 결과 행과 CSV 내용은 workflow 컬렉션에 저장하지
않습니다.

운영자는 Git에서 제외된 `llm/librechat/mcp.env`에 planner용 `OPENAI_API_KEY`와
workflow MongoDB 연결용 `ENERGY_MCP_MONGO_URI`를 설정합니다. Compose는
`ENERGY_MCP_MODE=workflow`로 서버를 실행합니다. 실제 값은 Git이나 문서에 기록하지
않으며, LibreChat 사용자는 이 값들을 입력하지 않습니다.

## NAS 예보 함수

`forecast_regions`는 NAS의 실제 지역 폴더 목록을 확인하고, `forecast_months`는 예보종·지역·요소에
대해 실제 시계열 CSV가 있는 월 목록을 반환하며, `forecast`는 시계열 파일을 읽습니다.
계획기는 승인 전에 요청한 각 월을 `forecast_months` 결과와 비교합니다. 없는 월이
있으면 부분 범위 조회를 진행하지 않고 기간을 다시 묻습니다. 평균 등 집계 질의는
서버가 검증한 원본을 `forecast_data` CTE로 넣어 실행합니다.

## 레거시 stdio 모드

패키지에는 호환과 개발을 위한 별도 `legacy` 모드가 남아 있습니다. 이 모드는
읽기전용 `run_sql`을 즉시 실행하므로 호스팅 승인 workflow와 다릅니다.

## 개발

```bash
cd llm/mcp-server
uv sync
uv run pytest
```

테스트는 PostgreSQL과 MongoDB 연결을 대체해 쿼리 검증, workflow 상태 전이, MCP 도구
등록을 확인합니다.
