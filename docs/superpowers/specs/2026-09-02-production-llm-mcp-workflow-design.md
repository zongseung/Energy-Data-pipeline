# 정식 LLM·MCP 질의 승인 워크플로 설계

## 목표

연구원이 추상적인 자연어 질문을 보내면 필요한 조건을 구체화하고, 확정된 조건과
SQL을 사람이 한 번 검토·승인한 뒤에만 조회하는 정식 LLM·MCP 경로를 제공한다.
대화 상태와 승인 상태는 MongoDB에 보존해 서버 재시작이나 긴 대화에서도 잃지
않는다.

이 설계는 Tailscale IP 허용 목록을 제외한다. IP 기반 접근 제어는 별도 설계에서
추가한다. 그 전까지 기존 LibreChat 계정 로그인을 유지한다.

## 현재 문제

- `energy-mcp`의 `run_sql(query)`는 호출 즉시 SQL을 실행하며 사용자, 대화,
  승인 상태를 모른다.
- `confirmed=true` 같은 인자를 추가해도 그 값은 LLM이 만들기 때문에 실제 사용자
  승인의 증거가 아니다.
- MCP transport session은 채팅 conversation과 동일하지 않으며 재연결과 서버
  재시작에 걸친 업무 상태 저장소로 사용할 수 없다.
- 현재 LibreChat v0.8.7 이미지에는 최신 main 브랜치 문서의 Mongo 기반 도구 승인
  기능이 없다. 정식 서비스에 RC 이미지를 도입하거나 LibreChat을 포크하지 않는다.
- 현재 OpenAI·JWT·자격증명 암호화 키는 컨테이너 환경변수로 전달돼
  `docker inspect`에 나타난다.
- 현재 nginx의 8098·8099는 `0.0.0.0`에 바인딩돼 있다. Tailscale 전용 바인딩은
  IP 접근 제어 설계와 함께 후속 변경한다.

## 검토한 접근

### 1. 프롬프트와 `confirmed` 인자만 추가

가장 작지만 기각한다. LLM이 확인 없이 `confirmed=true`를 만들거나, 승인 후 SQL을
바꿔 재시도해도 서버가 구분할 수 없다.

### 2. LibreChat 최신 개발 기능 사용

LibreChat main에는 도구 승인과 Mongo checkpointer가 있지만 현재 안정 이미지
v0.8.7에는 없다. v0.8.8-rc1 또는 미출시 코드를 정식 서비스의 핵심 승인 경계로
사용하지 않는다.

### 3. energy-mcp가 Mongo workflow와 승인 페이지 소유

선택한 방식이다. 기존 읽기전용 SQL 실행기를 재사용하고, SQL보다 앞에 작은 상태
기계를 둔다. 승인 페이지는 MCP tool이 아니므로 모델이 승인 API를 tool call로
호출할 수 없다. 동일한 Streamable HTTP endpoint를 쓰는 클라이언트가 같은 절차를
사용한다.

## 외부 인터페이스

정식 HTTP endpoint는 아래 두 도구만 광고한다.

### `plan_query`

입력:

- `question: str`: 사용자의 원문 질문
- `workflow_id: str | null`: 후속 구체화 응답이면 기존 workflow ID
- `answers: dict[str, str] | null`: 사용자가 답한 구체화 항목

출력은 둘 중 하나다.

1. `needs_clarification`: 빠진 조건을 묻는 질문과 `workflow_id`
2. `awaiting_confirmation`: 정규화된 조건 요약, 실행 예정 SQL, 승인 URL,
   `workflow_id`, 만료 시각

서버가 소유한 단일 planner prompt가 대상, 지표, 기간, 집계 단위, 필터, 출력 형태를
판정한다. 질문에 따라 필요하지 않은 항목은 요구하지 않는다. planner는 실제
`research` 스키마와 기존 함정 규칙만 사용하며 JSON 구조화 출력으로 SQL과 조건을
반환한다. 구현은 공식 OpenAI Python SDK의 구조화 출력을 사용한다. 모델은 현재
서비스와 같은 `gpt-4o-mini`를 기본값으로 하고 `ENERGY_MCP_PLANNER_MODEL`로만
교체할 수 있게 한다.

### `execute_query`

입력:

- `workflow_id: str`

호출자가 SQL이나 승인 값을 전달할 수 없다. 서버는 MongoDB에 저장된 workflow가
같은 주체의 `confirmed` 상태이고 만료되지 않았을 때만 저장된 SQL을 한 번 점유해
기존 `_execute()`로 실행한다.

기존 `run_sql(query)`는 로컬 stdio·관리자 호환을 위해 코드에 남기되 정식 HTTP
endpoint에서는 광고하지 않는다. 정식 endpoint에 같이 노출하면 승인 절차를 우회할
수 있기 때문이다. 이를 위해 프로세스 시작 시 `ENERGY_MCP_MODE`를 읽어 서로 다른
FastMCP 인스턴스 중 하나만 실행한다. `workflow` 모드는 `plan_query`와
`execute_query`만, `legacy` 모드는 기존 `run_sql`과 schema resource만 등록한다.
정식 Compose는 `workflow`, 로컬 stdio 기본값은 호환성을 위해 `legacy`다.

## 처리 흐름

1. 클라이언트 LLM이 사용자의 원문을 `plan_query`에 전달한다.
2. planner가 중요한 조건이 빠졌으면 SQL을 만들지 않고 질문만 반환한다.
3. 클라이언트가 사용자의 답을 같은 `workflow_id`와 함께 다시 전달한다.
4. 조건이 충분하면 서버가 조건 요약과 SQL을 MongoDB에 `awaiting_confirmation`으로
   저장하고 승인 URL을 반환한다.
5. 사용자가 브라우저에서 조건 요약과 SQL을 확인하고 승인 또는 거절한다.
6. 승인 페이지의 POST만 workflow를 `confirmed`로 바꿀 수 있다. 승인용 MCP tool은
   만들지 않는다.
7. `execute_query`가 `confirmed → executing` 원자 전이에 성공하면 저장된 SQL을
   실행한다.
8. 결과와 함께 확정 조건, 실제 실행 SQL, workflow ID를 반환한다.
9. SQL 오류로 SQL을 수정해야 하면 새 초안을 저장하고 다시 승인받는다. 기존 승인을
   재사용하지 않는다.

## MongoDB 모델

기존 Mongo 컨테이너를 재사용하되 LibreChat 컬렉션과 섞지 않고 `energy_mcp` DB와
전용 DB 사용자를 둔다. Python 패키지에는 공식 `openai` SDK와 `pymongo`만 추가한다.

### `query_workflows`

- `_id`: 임의 생성 workflow ID
- `question`, `answers`: 원문과 구체화 답변
- `conditions`: planner가 정규화한 조건
- `summary`: 사람에게 표시할 조건 요약
- `sql`, `sql_sha256`: 실행 예정 SQL과 무결성 해시
- `status`: `clarifying`, `awaiting_confirmation`, `confirmed`, `executing`,
  `done`, `declined`, `failed`, `expired`
- `conversation_id`, `principal_id`: 가능한 클라이언트에서 전달한 소유 정보
- `created_at`, `expires_at`, `confirmed_at`, `executed_at`
- `approval_token_hash`, `approval_csrf_hash`: 원문 토큰은 저장하지 않는다.
- `error_code`, `row_count`, `duration_ms`: 결과 데이터가 아닌 실행 감사 정보

활성 workflow 유효기간은 30분이다. TTL index로 만료 문서를 정리하되 실행 시에는
`expires_at > now`를 코드에서 다시 검사한다. 실행 점유는 조건부 `findOneAndUpdate`로
한 번만 성공하게 한다. 조회 결과와 CSV는 MongoDB에 저장하지 않는다.

## 승인 페이지

FastMCP의 `custom_route`로 최소 HTML GET/POST를 제공한다. GET은 조건 요약, SQL,
만료 시각만 표시하고 실행하지 않는다. POST는 승인·거절만 처리한다.

- URL에는 256-bit 임의 bearer token을 사용하고 MongoDB에는 SHA-256 hash만 저장한다.
- 승인 폼에는 별도 one-time CSRF nonce를 사용한다.
- 승인과 거절은 한 번만 가능하다.
- 응답에 DB 결과, DSN, API key를 포함하지 않는다.
- Tailscale IP/사용자 검증은 후속 범위다. IP gate가 추가되면 workflow의
  `principal_id`와 승인 주체를 반드시 일치시킨다.

불특정 MCP 클라이언트가 사람인지 프로토콜만으로 증명할 수는 없다. 이 설계가
강제하는 것은 모델이 호출할 수 없는 별도 HTTP POST, 저장 SQL의 불변성, 1회 실행,
만료다. IP/사용자 신원 결합이 완료되기 전에는 승인 링크를 가진 사람이 승인
주체다.

## Prompt 규칙

FastMCP 전역 `instructions`와 `plan_query` tool 설명에 같은 핵심 절차를 넣는다.

1. 원문을 임의로 구체화하지 말고 `plan_query` 결과의 질문을 사용자에게 전달한다.
2. 사용자의 답을 요약하거나 바꾸지 말고 같은 workflow에 전달한다.
3. `awaiting_confirmation`이면 조건과 SQL, 승인 링크를 보여주고 승인을 기다린다.
4. 승인 전에는 `execute_query`를 호출하지 않는다.
5. 결과에는 확정 조건과 실제 실행 SQL을 표시한다.

프롬프트는 UX를 일관되게 하지만 보안 경계는 아니다. 실행 권한은 Mongo 상태 전이와
저장 SQL로 강제한다.

## 비밀값 처리

OpenAI API key, LibreChat `JWT_SECRET`, `JWT_REFRESH_SECRET`, `CREDS_KEY`,
`CREDS_IV`, PostgreSQL 비밀번호, MongoDB 비밀번호는 아래 규칙을 따른다.

- Git 추적 파일, Compose `environment`, `env_file`, 명령행 인자에 값을 넣지 않는다.
- Compose secret으로 `/run/secrets/<name>`에 파일 마운트한다.
- LibreChat가 `_FILE` 변수를 직접 지원하지 않으므로 작은 POSIX shell 시작 래퍼가
  파일을 읽어 프로세스 환경에 넣고 즉시 `exec npm run backend`를 실행한다.
- 래퍼는 `set -x`를 쓰지 않고 값이나 길이를 로그에 남기지 않는다.
- energy-mcp도 같은 방식으로 DSN과 planner OpenAI key를 런타임에 조립한다.
- `docker inspect`의 container config에는 secret 값이 없어야 한다.
- Docker daemon 관리자와 host root는 실행 프로세스나 secret 파일을 읽을 수 있다.
  이 권한까지 비밀값을 숨긴다고 주장하지 않는다.
- 이번 점검 중 노출된 OpenAI/JWT/자격증명 암호화 키는 모두 새 값으로 교체한다.
  OpenAI key 폐기·재발급은 OpenAI 계정 소유자가 수행해야 한다.

Secret 원본 파일은 저장소 밖 운영 경로에 두고 소유자 읽기만 허용한다. MongoDB에는
secret 값이나 DSN을 저장하지 않고 secret 이름만 기록한다.

## 오류 처리

- planner 응답이 JSON schema를 만족하지 않으면 workflow를 `failed`로 두고 실행하지
  않는다.
- MongoDB 장애 시 plan, 승인, 실행을 모두 fail closed 한다.
- 만료·거절·이미 실행된 workflow는 새 workflow 생성을 안내한다.
- 실행 중 프로세스가 종료되면 자동 재실행하지 않는다. `executing` 상태를
  `failed`로 정리하고 새 승인을 요구한다.
- SQL 실행 오류는 기존 교정 힌트를 보존하되 SQL을 자동 변경·재실행하지 않는다.

## 검증

- 추상 질문은 `needs_clarification`이며 DB를 호출하지 않는다.
- 충분한 답변은 SQL을 실행하지 않고 `awaiting_confirmation`을 만든다.
- 미승인·만료·거절·다른 workflow는 실행되지 않는다.
- 승인된 저장 SQL만 정확히 한 번 실행된다.
- SQL이 바뀌면 hash가 달라지고 재승인이 필요하다.
- 서버 재시작 후 pending/confirmed 상태가 보존된다.
- 동시 `execute_query` 호출 중 하나만 실행 점유에 성공한다.
- `docker inspect`와 컨테이너 로그에 secret 값이 없다.
- 저장소 secret scan과 기존 MCP 읽기전용·행 제한 테스트가 모두 통과한다.

## 범위에서 제외

- Tailscale IP allowlist와 IP→사용자 매핑
- LibreChat 포크 또는 RC 이미지 도입
- 승인 없는 자동 SQL 실행
- MongoDB에 조회 결과 저장
- 자체 캐시, 큐, Redis, 다중 worker
- 기존 ETL·research 뷰 변경
- `docker/llm-demo` 디렉터리의 대규모 이름 변경. 사용자-facing 문구만 정식 서비스로
  고치고 경로명은 별도 정리 작업에서 바꾼다.
