# LLM·MCP로 조회

## 설치 없이 쓰기 — 정식 LibreChat 서비스

브라우저에서 공식 LibreChat 서비스에 로그인하고 채팅 입력창의 MCP(도구) 메뉴에서 `energy-db`를 켜세요. 주소와 계정은 관리자에게 받습니다. 계정은 이 서비스 전용이므로 다른 곳에서 쓰는 비밀번호를 재사용하지 마세요.

이 경로는 읽기전용 조회를 **승인 뒤에만** 실행하는 정식 서비스입니다. 질문과 조회 결과는 OpenAI 서버로 전송되므로, 외부로 보내면 안 되는 분석은 직접 SQL 또는 아래의 고급 로컬 경로를 쓰세요.

{% hint style="warning" %}
논문·보고서의 최종 분석은 LLM 답변만으로 끝내지 마세요. 아래의 조건과 실제 실행 SQL을 직접 SQL로 재현·검증해야 합니다.
{% endhint %}

### 요청·승인 절차

1. 질문이 모호하면 도우미가 발전소·기간·집계·단위처럼 빠진 조건을 **구체화**하도록 하나 이상 물어봅니다.
2. 조건이 정해지면 도우미가 **조건 요약**, 실행 예정 SQL, 30분 동안 유효한 승인 URL을 보여 줍니다. 아직 조회는 실행되지 않습니다.
3. 조건과 SQL을 검토한 뒤 URL에서 **승인** 또는 거절을 누릅니다.
4. 승인된 경우에만 `execute_query`가 실행됩니다. 거절하면 조회하지 않습니다.
5. SQL이 한 글자라도 바뀌면 기존 승인은 쓸 수 없으므로 새 조건 요약과 새 승인을 받아야 합니다.
6. 최종 답변에는 조건 요약과 **실제 실행 SQL**이 함께 표시됩니다.

결과가 많으면 미리보기·컬럼 요약과 전체 CSV 다운로드 링크가 제공될 수 있습니다. 조회는 읽기전용 role로만 실행되고 기본 시간 제한은 60초, 행 제한은 10,000행입니다. 응답에 `truncated: true`가 있으면 집계가 불완전할 수 있습니다.

### 결과 검증

1. **실제 실행 SQL** — 의도한 발전소·기간·조건과 같은지 확인합니다.
2. **연료 필터** — `research.generation`에는 태양광·풍력·수력·화력·연료전지가 섞여 있습니다. 태양광 조회에 `fuel_type = 'solar'`가 없으면 화력이 섞일 수 있습니다.
3. **단위·시간** — `gen_kwh`는 kWh이고 기본 시간 규약은 KST 구간시작입니다 (09:00 값 = 09~10시 구간). 단, `research.weather_asos.solar_radiation`의 시간 라벨은 구간시작/구간종료 중 무엇인지 아직 미확정이라 보정하지 않았고, 풍력 발전량은 실제 값과 ±1시간 어긋날 수 있습니다.
4. **품질 필터** — 뷰가 이미 적용한 품질 규칙에 `data_quality = '정상'` 또는 `is_aggregate = false`를 다시 덧붙여 정상 데이터를 빼지 않았는지 확인합니다.
5. **잘림 여부** — `truncated: true`이면 기간을 나누거나 직접 SQL로 재현합니다.

## 운영자: 기존 Mongo 볼륨 전환

기존 `mongo-data` 볼륨으로 이 서비스를 처음 전환하는 경우에만, 전체 스택을 올리기 전에 `docker/llm-demo/bootstrap-mongo.sh`를 한 번 실행해 Mongo 사용자 초기화를 마칩니다. 새 볼륨과 이후 재시작에는 필요하지 않습니다.

## 고급 경로 — 로컬 stdio `run_sql`

로컬 `energy-mcp`는 연구원 PC에서 실행하는 stdio 프로그램입니다. Tailscale에 먼저 연결하고 직접 SQL과 같은 개인 읽기전용 DB 계정을 사용합니다. 이 `run_sql` 경로는 **승인 절차를 거치지 않는 고급·비권장 경로**이므로, 정식 조회에는 위 LibreChat 서비스를 사용하세요.

### 시작하기 전에

1. 이용조건 서약 완료
2. Tailscale 연결 (`tailscale status`로 확인 — 직접 SQL 가이드 1절 참고)
3. 개인별 DB 계정(role·비밀번호)
4. `uv` 설치 ([docs.astral.sh/uv](https://docs.astral.sh/uv/))

### 1. 로컬 energy-mcp 실행

이 패키지는 아직 PyPI에 올라가 있지 않습니다. 현재는 저장소 로컬 체크아웃에서 실행합니다 (경로는 관리자에게 문의):

```bash
uvx --from /path/to/Energy-Data-pipeline/mcp-server energy-mcp
```

`run_sql`은 `research` 스키마를 읽기전용으로 조회합니다. `energy://schema` 리소스는 뷰·컬럼·설명을 DB에서 읽어 보여 줍니다.

### 2. 서버 등록

Claude Desktop 기준 `claude_desktop_config.json`에 DB별 서버를 등록합니다. 아래 `ENERGY_MCP_DSN`은 플레이스홀더이며, 실제 값을 저장소나 채팅에 남기지 마세요.

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

서버 프로세스 하나는 DB 하나에만 붙습니다. 두 DB를 모두 쓰려면 항목을 두 개 등록하고 `ENERGY_MCP_DSN`만 다르게 둡니다.

## 좋은 질문과 피해야 할 질문

| 좋은 질문 | 이유 |
| --- | --- |
| "구미태양광의 2025년 6월 일별 발전량 합계" | 대상·기간이 명확하고 조건을 검토할 수 있음 |
| "plants 뷰에 어떤 컬럼이 있어?" | 스키마 탐색에 적합 |

| 피해야 할 질문 | 이유 |
| --- | --- |
| "전체 데이터 다 보여줘" | 60초 제한·10,000행 제한에 걸릴 수 있음 |
| "발전 효율이 제일 좋은 발전소는?" | '효율' 정의가 모호해 임의 조건이 생길 수 있음 |

## 직접 SQL로 전환

정식 서비스의 SQL을 재현·검증하거나 복잡한 분석이 필요하면 직접 SQL로 전환하세요. Tailscale 연결과 개인 계정을 그대로 사용합니다. 로컬 stdio `run_sql`이 계속 실패해도 직접 SQL로 재현할 수 있습니다.
