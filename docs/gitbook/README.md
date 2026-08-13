# 에너지 연구 데이터 이용 안내

발전량·SMP·기상·전력수요 데이터를 연구 목적으로 안전하게 조회하는 방법을 안내합니다.

{% hint style="info" %}
이 GitBook은 누구나 읽을 수 있는 공개 안내서입니다. 실제 데이터는 Tailscale
폐쇄망과 연구원별(개인별) 읽기전용 PostgreSQL 계정으로 보호됩니다.
{% endhint %}

## 처음 한 번만 준비하세요

{% stepper %}
{% step %}
### 이용조건 서약

[이용조건](05-terms.md)을 읽고 서약합니다.
{% endstep %}

{% step %}
### Tailscale 가입

관리자가 보낸 초대로 폐쇄망 VPN에 합류합니다.
{% endstep %}

{% step %}
### DB 계정 수령

개인별 계정을 별도 채널로 받습니다. 문서나 채팅에 붙여넣지 마세요.
{% endstep %}

{% step %}
### 조회 방법 선택

직접 SQL과 LLM·MCP 중에서 고릅니다. 아래 표를 참고하세요.
{% endstep %}
{% endstepper %}

## 어떤 방법을 사용할까요?

| 구분 | 직접 SQL | LLM·MCP |
|---|---|---|
| 추천 대상 | 재현 가능한 분석·통계·그래프 | 스키마 탐색·간단한 자연어 조회 |
| 사용하는 도구 | psql, pandas, R, DBeaver | Claude Desktop 등 LLM 클라이언트와 로컬 `energy-mcp` |
| 공통 조건 | Tailscale + 개인 DB 계정 | Tailscale + 같은 개인 DB 계정 |
| 결과 확인 | SQL과 원자료를 직접 확인 | 생성된 SQL과 데이터 규칙을 반드시 재확인 |

두 방법 모두 같은 Tailscale 폐쇄망과 같은 개인 계정을 사용합니다. 어느 쪽을
선택해도 보안 경계는 달라지지 않습니다 — [데이터 제공 구조](01-architecture.md)에서
전체 흐름을 확인하세요.

## 다음으로 읽을 페이지

- [데이터 제공 구조](01-architecture.md) — 두 조회 방법이 공유하는 보안 경계
- [직접 SQL로 조회](02-direct-sql.md) — Tailscale 연결부터 첫 쿼리까지
- [LLM·MCP로 조회](03-llm-mcp.md) — 로컬 MCP 설치부터 자연어 질문까지
- [데이터 카탈로그 · 스키마 사전](04-data-catalog.md) — 뷰별 기간·단위·품질·시간 규약
- [이용조건 · 보안 서약](05-terms.md) — 허용·금지 행위, 감사 로그, 계정 회수
