# energy-mcp

에너지 연구 데이터(`research` 스키마)를 Claude Desktop / VS Code / Continue 같은
MCP 클라이언트에서 자연어로 조회할 수 있게 해주는 얇은 읽기전용 stdio MCP 서버.

**이 서버는 선택 층이다.** 데이터 배포의 코어는 Tailscale 폐쇄망 안에서의
읽기전용 Postgres 직접 접속이다 — psql이나 pandas로도 얼마든지 데이터를 쓸 수
있다. 이 서버가 없어도 연구에는 지장이 없다.

**연구원 본인의 role 자격증명**을 그대로 쓰기 때문에 누가 어떤 쿼리를 날렸는지
DB 쪽에서 개인별로 감사 추적이 남는다. 이 서버가 대신 인증하지 않는다.

## 무엇을 제공하는가

- 툴 `run_sql(query: str)` 하나. `research` 스키마에 대해 **읽기전용** SQL을
  실행한다.
- 리소스 `energy://schema`. `research` 스키마의 뷰·컬럼·설명을 DB에서 직접 읽어
  마크다운으로 낸다 — 시간 규약(모두 KST 구간시작), `gen_kwh` 단위(kWh, 원천
  헤더가 MWh로 적혀 있어도 실제 값은 kWh), 발전소 데이터 품질 등급 등 쿼리를
  잘못 짜기 쉬운 함정을 여기서 먼저 확인해야 한다.

## 읽기전용이 강제되는 방식

두 겹이다.

1. **DB 권한** — 연구원에게 발급되는 role은 애초에 `research` 스키마 `SELECT`
   권한만 가진다 (이 서버가 부여하는 게 아니라 DB 쪽에서 이미 그렇게 설정돼
   있다).
2. **이 서버** — 커넥션마다 `set_session(readonly=True)`로 세션 전체를
   읽기전용으로 고정한다(`SET SESSION CHARACTERISTICS AS TRANSACTION READ
   ONLY`와 동일). SQL 문자열에서 `INSERT`/`UPDATE`/`DROP` 같은 키워드를
   정규식으로 걸러내는 방식은 우회가 쉬워서 쓰지 않는다 — PostgreSQL 엔진
   자체가 쓰기 문장을 거부하게 만든다.

그 외에:

- `statement_timeout`을 세션에 설정한다 (기본 60초, `ENERGY_MCP_STATEMENT_TIMEOUT_S`
  로 조정).
- 세미콜론으로 여러 문장을 이어 붙인 요청은 거부한다 — 그렇지 않으면 뒤 문장에서
  `SET statement_timeout = 0` 같은 걸로 위 타임아웃을 무력화할 수 있다.
- 결과 행 수는 기본 10,000행으로 제한한다 (`ENERGY_MCP_ROW_LIMIT`로 조정).
  잘렸으면 응답의 `truncated: true`와 `note`에 명시된다 — 조용히 자르지 않는다.
- 에러는 사람이 읽을 수 있는 메시지로만 돌려준다. 스택트레이스나 DSN은
  응답에 담기지 않는다.

## 설치

```bash
uvx energy-mcp
```

패키지를 아직 PyPI에 올리지 않았다면 로컬 체크아웃에서 바로 실행할 수 있다.

```bash
uvx --from /path/to/Energy-Data-pipeline/mcp-server energy-mcp
```

## 설정: `ENERGY_MCP_DSN`

연구원 본인에게 발급된 읽기전용 role의 DSN을 환경변수로 넘긴다. **비밀번호를
설정 파일이나 커밋에 평문으로 남기지 않도록 주의하라.** 아래 예시의 값은
전부 자리표시자다 — 실제 호스트/비밀번호로 바꿔서 쓰되 저장소나 채팅에는
남기지 마라.

```bash
export ENERGY_MCP_DSN="postgresql://<본인_role>:<비밀번호>@<tailscale_호스트>:5436/<db>"  # pv DB 예시. demand DB는 5433
```

pv DB와 demand DB는 서로 다른 Postgres 인스턴스다. 하나의 서버 프로세스는 DSN
하나, 즉 DB 하나에만 붙는다. 두 DB를 다 조회하고 싶으면 아래처럼 MCP 클라이언트
설정에 서버 항목을 두 개(예: `energy-mcp-pv`, `energy-mcp-demand`) 등록하고
`ENERGY_MCP_DSN`만 다르게 주면 된다.

선택 환경변수:

| 변수 | 기본값 | 의미 |
|---|---|---|
| `ENERGY_MCP_DSN` | (필수) | 읽기전용 role DSN |
| `ENERGY_MCP_STATEMENT_TIMEOUT_S` | `60` | 쿼리당 최대 실행 시간(초) |
| `ENERGY_MCP_ROW_LIMIT` | `10000` | 응답에 담을 최대 행 수 |

## Claude Desktop 설정

`claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "energy-pv": {
      "command": "uvx",
      "args": ["energy-mcp"],
      "env": {
        "ENERGY_MCP_DSN": "postgresql://<본인_role>:<비밀번호>@<tailscale_호스트>:5436/pv"
      }
    },
    "energy-demand": {
      "command": "uvx",
      "args": ["energy-mcp"],
      "env": {
        "ENERGY_MCP_DSN": "postgresql://<본인_role>:<비밀번호>@<tailscale_호스트>:5433/demand"
      }
    }
  }
}
```

## VS Code (MCP 확장) 설정

`.vscode/mcp.json` (워크스페이스) 또는 사용자 설정:

```json
{
  "servers": {
    "energy-pv": {
      "type": "stdio",
      "command": "uvx",
      "args": ["energy-mcp"],
      "env": {
        "ENERGY_MCP_DSN": "postgresql://<본인_role>:<비밀번호>@<tailscale_호스트>:5436/pv"
      }
    }
  }
}
```

Continue도 동일한 stdio 서버 등록 방식(`command`/`args`/`env`)을 쓰는 MCP
클라이언트라면 위 설정을 그대로 옮기면 된다 — 정확한 설정 파일 경로만 Continue
문서에서 확인하라.

## 개발 · 테스트

```bash
cd mcp-server
uv sync
uv run pytest
```

테스트는 DB 없이 통과한다(커넥션을 모킹한다). 실제 DB 접속 동작은 이 저장소를
운영하는 쪽에서 `ENERGY_MCP_DSN`을 실제 role로 채워 수동으로 확인했다.
