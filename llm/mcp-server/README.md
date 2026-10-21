# energy-mcp

호스팅 LibreChat에서 PostgreSQL `research` 스키마를 읽기전용으로 조회하는 MCP 서버입니다.
`ENERGY_MCP_MODE`로 두 모드를 고릅니다.

| 모드 | 도구 | 용도 |
| --- | --- | --- |
| `legacy` (운영, 2026-10-21~) | `run_sql`, `describe_data`, `collect_forecast`, `forecast_collection_status` | AI가 SQL을 직접 작성해 즉시 실행 |
| `workflow` (보존, 현재 꺼짐) | `plan_query`, `execute_query`, `collect_forecast`, `forecast_collection_status` | 별도 계획기가 SQL을 만들고 브라우저 승인 뒤 1회 실행 |

`workflow` 코드는 남겨 두었으니 필요하면 `llm/librechat/compose.yml`의 `ENERGY_MCP_MODE`를
되돌린 뒤 이미지를 재빌드·재시작합니다(계획기용 `OPENAI_API_KEY`는 `mcp.env`에 필요).

## 운영 모드(legacy)

LibreChat은 Streamable HTTP로 연결합니다. 서버 안내문(`LEGACY_INSTRUCTIONS`)은 일하는 방식과
데이터 지도(뷰 이름·내용)를 담고, 기간처럼 변하는 값은 넣지 않습니다. 데이터 자체에 대한 질문은
`describe_data`가 `docs/gitbook`의 카탈로그 문서(컨테이너에는 `/docs`로 읽기전용 마운트,
`ENERGY_MCP_DOCS_DIR`)와 DB에서 실측한 뷰별 기간(10분 캐시)을 돌려줍니다. 문서를 고치면
재빌드 없이 반영됩니다. `run_sql`은 단일 문장을 읽기전용 세션·`demo_ro`·실행 시간 제한으로
실행하고 미리보기와 CSV 링크를 돌려줍니다.

`hints.KNOWN_VIEWS`, 안내문의 데이터 지도, `RANGE_SPECS`는 DB와 어긋나기 쉬우므로
`ENERGY_MCP_TEST_DSN`을 주고 `tests/test_guide_vs_db.py`를 돌려 점검합니다.

## 워크플로 모드(보존)

질문, 구체화된 조건, SQL, 승인 상태를 MongoDB `query_workflows`에 저장하고, 승인 페이지는
상태만 바꿉니다. 승인 후 `execute_query`가 저장된 SQL을 한 번 실행합니다. 계획 응답은
정보가 부족하면 `needs_clarification`을, 충분하면 조건·SQL·승인 링크를 돌려주며 30분이
지나면 사용할 수 없습니다. 운영자는 `llm/librechat/mcp.env`에 계획기용 `OPENAI_API_KEY`를,
`weather.env`에 `ENERGY_MCP_MONGO_URI`를 설정합니다. 실제 값은 Git이나 문서에 기록하지 않습니다.

## NAS 예보 함수

`forecast_regions`는 NAS의 실제 지역 폴더 목록을 확인하고, `forecast_months`는 예보종·지역·요소에
대해 실제 시계열 CSV가 있는 월 목록을 반환하며, `forecast`는 시계열 파일을 읽습니다.
`workflow` 모드의 계획기는 승인 전에 요청한 각 월을 `forecast_months` 결과와 비교하고, 없는 월이
있으면 부분 범위 조회를 진행하지 않고 기간을 다시 묻습니다(평균 등 집계는 검증한 원본을
`forecast_data` CTE로 넣어 실행). `legacy` 모드에서는 AI가 `forecast_months`로 직접 확인합니다.

## NAS 예보 수집

LibreChat의 `collect-weather-nas` 배포 Skill은 `collect_forecast`를 사용합니다.
기본 `confirmed=false`는 실제 파일 검증만 합니다. 사용자의 수집 요청 후 true로
호출하면 기존 `/mnt/nvme/weather-data` 수집기를 별도 프로세스에서 실행합니다.
한 번에 한 읍면동·한 요소·최대 12개월 중 없는 월만 수집하며, 두 배포가 공통
파일 잠금을 사용합니다. 작업 상태는 `forecast_collection_status(job_id)`로 확인합니다.

운영자는 git에서 제외된 `llm/librechat/weather.env`에 `KMA_ID`, `KMA_PW`를
등록합니다. 사용자별 계정 등록은 아직 지원하지 않습니다. CIFS 마운트를 검증하고
다운로드한 CSV를 검증한 뒤 NAS에 원자적으로 반영합니다. 수집은 20분 제한이며
완료 후 다시 조회합니다. 수집 작업 메타데이터는 MongoDB에서
7일 뒤 만료됩니다. DB의 공용 읽기전용 권한은 그대로 유지합니다.

첫 배포 전 `docker volume create energy-weather-state`로 공통 작업 잠금 볼륨을
만듭니다. LibreChat 시작 후 호스트에서
`docker exec -i librechat node < llm/librechat/provision_weather_agent.cjs`를 실행하면
배포 Skill과 네 도구(`run_sql`·`describe_data`·수집 도구 둘)를 사용하는 공용 `NAS 기상 조회` Agent를 등록합니다.

## stdio 사용

`ENERGY_MCP_TRANSPORT`를 지정하지 않으면 stdio로 실행됩니다(Claude Desktop 등 로컬 클라이언트용, `legacy` 모드).

## 개발

```bash
cd llm/mcp-server
uv sync
uv run pytest
```

테스트는 PostgreSQL과 MongoDB 연결을 대체해 쿼리 검증, workflow 상태 전이, MCP 도구
등록을 확인합니다.
