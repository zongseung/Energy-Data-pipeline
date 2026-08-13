# LLM·MCP로 조회

`energy-mcp`는 인터넷에 공개된 서버가 아니라 연구원 PC에서 실행되는 로컬 stdio 프로그램입니다. LLM·MCP 방식도 Tailscale에 먼저 연결해야 하며, 직접 SQL 방식과 같은 개인 DB 계정을 씁니다.

## 이 방법이 적합한 경우

- 스키마를 아직 잘 몰라 자연어로 탐색하고 싶을 때
- 간단한 집계·조회를 빠르게 확인하고 싶을 때

논문·보고서에 들어갈 최종 분석은 이 방법만으로 끝내지 말고, 생성된 SQL을
직접 검증하거나 [직접 SQL](02-direct-sql.md)로 재현하세요.

## 요청이 처리되는 순서

1. 연구원이 LLM 클라이언트(Claude Desktop 등)에 자연어로 질문
2. LLM이 SQL을 생성해 로컬 `energy-mcp`의 `run_sql` 툴 호출을 요청
3. `energy-mcp`가 Tailscale 폐쇄망을 거쳐 **연구원 본인의 개인 role**로 DB에 접속해 실행
4. LLM이 결과를 받아 답변으로 정리

이 서버가 대신 인증하지 않습니다 — 본인 role의 권한과 감사 로그를 그대로 씁니다.

## 시작하기 전에

1. [이용조건](05-terms.md) 서약 완료
2. Tailscale 연결 (`tailscale status`로 확인 — [직접 SQL 가이드](02-direct-sql.md) 1절 참고)
3. 개인별 DB 계정(role·비밀번호)
4. `uv` 설치 ([docs.astral.sh/uv](https://docs.astral.sh/uv/))

## 1. 로컬 energy-mcp 실행

이 패키지는 **아직 PyPI에 올라가 있지 않습니다** — 지금 `uvx energy-mcp`를
실행하면 package not found로 실패합니다. 현재는 저장소 로컬 체크아웃에서
실행합니다 (경로는 관리자에게 문의):

```bash
uvx --from /path/to/Energy-Data-pipeline/mcp-server energy-mcp
```

기능은 두 가지입니다.

- 툴 `run_sql` — `research` 스키마를 **읽기전용**으로 조회하는 SQL 실행. 기본
  시간 제한 60초, 행 제한 10,000행이며 결과가 잘리면 응답에 `truncated: true`가
  붙습니다.
- 리소스 `energy://schema` — 뷰·컬럼·설명을 DB에서 직접 읽어 보여줍니다.

## 2. pv와 demand 서버 등록

Claude Desktop 기준 `claude_desktop_config.json`에 pv와 demand 서버를 각각
등록합니다. 아래 DSN(접속 문자열) 값은 전부 플레이스홀더입니다 — 본인 발급
정보로 바꿔 쓰되 실제 값을 저장소나 채팅에 남기지 마세요.

```json
{
  "mcpServers": {
    "energy-pv": {
      "command": "uvx",
      "args": ["--from", "/path/to/Energy-Data-pipeline/mcp-server", "energy-mcp"],
      "env": {
        "ENERGY_MCP_DSN": "postgresql://<발급받은_ID>:<발급받은_비밀번호>@<tailnet-host>:5436/pv"
      }
    },
    "energy-demand": {
      "command": "uvx",
      "args": ["--from", "/path/to/Energy-Data-pipeline/mcp-server", "energy-mcp"],
      "env": {
        "ENERGY_MCP_DSN": "postgresql://<발급받은_ID>:<발급받은_비밀번호>@<tailnet-host>:5433/demand"
      }
    }
  }
}
```

서버 프로세스 하나는 DB 하나에만 붙습니다. 두 DB를 다 쓰려면 위처럼 두 항목을
등록하고 `ENERGY_MCP_DSN`만 다르게 줍니다.

## 3. 첫 질문 보내기

클라이언트를 재시작하고 다음 문장으로 시작해 보세요.

```text
2025년 구미태양광의 월별 발전량 합계를 조회해 줘. 실행한 SQL도 함께 보여줘.
```

"실행한 SQL도 함께 보여줘"를 습관처럼 붙이면 검증이 쉬워집니다.

## 4. 실행 SQL과 결과 검증

LLM 답변은 분석의 최종 근거가 아닙니다. 다음을 연구원이 직접 확인하세요.

1. **실행 SQL** — 의도한 발전소·기간·조건이 맞는지
2. **연료 필터** — `research.generation`에는 태양광·풍력·수력·화력·연료전지가
   모두 섞여 있습니다. 태양광을 물었는데 `fuel_type = 'solar'`가 없으면 화력이
   섞인 답입니다
3. **단위** — `gen_kwh`는 kWh ([데이터 카탈로그](04-data-catalog.md) 참고)
4. **시간 규약** — 모두 KST 구간시작
5. **과잉 필터** — `data_quality = '정상'`이나 `is_aggregate = false`를
   **덧붙이지 않았는지**. 뷰가 이미 걸러 놨기 때문에 다시 걸면 멀쩡한 데이터가
   사라집니다 ([이유](catalog/generation.md#함정--is_aggregate-지금은-빼면-안-됩니다))
6. **잘림 여부** — 응답에 `truncated: true`가 있으면 집계가 불완전할 수 있음

## 좋은 질문과 피해야 할 질문

| 좋은 질문 | 이유 |
|---|---|
| "구미태양광의 2025년 6월 일별 발전량 합계, SQL도 보여줘" | 대상·기간이 명확, 검증 가능 |
| "plants 뷰에 어떤 컬럼이 있어?" | 스키마 탐색에 적합 |

| 피해야 할 질문 | 이유 |
|---|---|
| "전체 데이터 다 보여줘" | 60초 제한·10,000행 제한에 걸림 |
| "발전 효율이 제일 좋은 발전소는?" | '효율'의 정의가 모호 — 모델이 임의로 해석한 SQL을 만들 수 있음 |

## 문제 해결

| 증상 | 먼저 확인할 것 | 해결 |
|---|---|---|
| MCP 서버가 시작 안 됨 | 로컬 경로, `ENERGY_MCP_DSN` | 경로·환경변수 재확인, 클라이언트 로그 확인 |
| 연결 시간 초과 | `tailscale status` | Tailscale 재연결 후 관리자에게 호스트 확인 |
| 인증 실패 | 개인 role·비밀번호 | 재발급 요청 (비밀번호를 채팅에 남기지 마세요) |
| 답변이 이상함 | 실행 SQL | SQL을 [데이터 카탈로그](04-data-catalog.md)의 단위·시간·품질 규칙과 대조 |

조회 결과를 외부 LLM 서버로 보내기 꺼려지면
[부록: 로컬 LLM](appendix-local-llm.md)의 완전 로컬 경로를 쓰세요.

## 직접 SQL로 전환

MCP가 계속 실패하거나 복잡한 쿼리가 필요하면 [직접 SQL](02-direct-sql.md)로
전환하세요. Tailscale 연결과 개인 계정을 그대로 쓰므로 추가 발급 절차가
없습니다. LLM이 만든 SQL을 psql에 붙여 넣어 재현해 보는 것부터 시작하면
됩니다.
