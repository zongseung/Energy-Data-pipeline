# 데모 LLM 쿼리 견고화 — 실증 배터리 + 에러 교정 힌트

2026-08-12. 배경: 시연 중 관찰된 SQL 실패 모드를 그때그때 docstring으로 패치해 왔다
(테이블 환각 → round 캐스트 → GROUP BY 누락 → 식별 컬럼 누락). 이번에 관찰된
추가 실패: **태양광만 요청했는데 소수력 포함**(fuel_type 필터 누락), **기간 불일치**
(날짜 경계 오류). 사후 패치를 멈추고 체계적으로 잡는다.

## 목표

연구원이 던질 법한 대표 질문들이 (a) 정답이거나 (b) 실패해도 모델이 에러 힌트를
보고 스스로 교정해 성공하는 상태. 측정 가능: 배터리 통과율 전/후 비교.

## 구성요소

### 1. 질문 배터리 — `scripts/demo_query_battery.py`

실제 데모 API 루프(GET /tools → chat(tools) → POST /tools → 최종 답)를 질문별로
실행하고 SQL·에러·답변을 기록하는 진단 도구. 재실행 가능하게 repo에 남긴다.

카테고리 (~18문항):
- 집계: 발전소별/월별/지역별 합산 (GROUP BY 검사)
- **필터 충실도**: "태양광만" → SQL에 `fuel_type = 'solar'` 존재 + 결과에 타 연료 없음,
  "2026년 7월" → 날짜 경계가 [7/1, 8/1) 인지
- 조인: 기상↔발전량, SMP↔수요
- 함정: 영암 합계계열(is_aggregate) 이중계상, 전면무효 발전소, 풍력 시간규약
- 추출: CSV 요청 시 SELECT * + download_url
- 날짜 표현: 연도만/월만/절대 날짜
- 빈 결과: 미래 날짜 질문에 환각 없이 "없음" 답변

판정: 질문마다 SQL 정규식 검사(필터·GROUP BY) + 가능한 경우 결과 값 검사.
출력: 통과/실패 카탈로그 markdown.

### 2. 에러 교정 힌트 — `mcp-server/energy_mcp/server.py`

`_execute`의 SQL 오류 반환에 패턴 매칭 힌트를 덧붙이는 순수 함수 `_hint_for(msg)`:

| 패턴 | 힌트 |
|---|---|
| function round(double precision | round(값::numeric, n) 캐스트 |
| relation ... does not exist | 존재하는 뷰 11개 나열 |
| column ... does not exist | SELECT * LIMIT 1 로 실제 컬럼 확인 |
| statement timeout | 기간 축소 또는 집계 전환 |
| syntax error | 단일 SELECT 문만 |
| division by zero | NULLIF(분모, 0) |

모델의 기존 에러-재시도 루프를 활용 → 목록에 없는 실패도 회복 가능.
`_hint_for`는 pytest 단위 테스트.

### 3. 검증 절차

배터리 1차(현재) → 힌트 + 배터리가 드러낸 최소 docstring 보강 → 재빌드 →
배터리 2차 → 통과율 비교. 실패였다가 회복된 항목을 카탈로그에 기록.

## 범위 제외 (YAGNI)

- SQL 파서/재작성기 — 과잉. 힌트-재시도로 충분한지 먼저 본다.
- 모델 교체(CUDA 전환 후 별건), 프롬프트 전면 개편.
- docstring 보강은 배터리가 실제 구멍을 보여준 항목만.
