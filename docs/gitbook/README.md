# 에너지 연구 데이터 안내

이 문서는 남부발전·남동발전 태양광, KOEN 비태양광(화력·연료전지·해양소수력),
풍력, 전국/제주 전력수급, 지역난방 열수요, SMP(계통한계가격), ASOS 기상
데이터를 연구 목적으로 조회하는 방법을 안내한다.

데이터는 **Tailscale 폐쇄망 안에서 읽기전용 PostgreSQL 직접 접속**으로
제공된다. 별도 API 서버나 대시보드를 거치지 않는다 — psql, pandas, R 등
익숙한 도구로 SQL을 바로 실행하면 된다.

## 이 문서가 다루는 것

| 파일 | 내용 |
|---|---|
| [01-data.md](01-data.md) | 데이터 카탈로그(뷰별 행수·기간·갱신주기)와 스키마 사전(컬럼 의미·단위·함정)을 합친 문서. **가장 먼저 읽어야 할 문서** |
| [02-access.md](02-access.md) | Tailscale 연결부터 psql/pandas/R/MCP 접속, 예제 쿼리까지 |
| [03-terms.md](03-terms.md) | 이용약관·서약서 (법적 검토 전 초안) |
| [appendix-local-llm.md](appendix-local-llm.md) | 외부 LLM에 조회 결과를 보내고 싶지 않은 경우를 위한 로컬 LLM 경로 (선택) |

## 데이터베이스 2개

운영 테이블은 감춰져 있고 `research` 스키마의 뷰만 노출된다. 뷰 컬럼에는
한국어 설명이 DB 레벨(`COMMENT`)로 붙어 있어 `psql`의 `\d+`나
`col_description()`으로도 바로 확인할 수 있다.

| DB | 접속 대상(포트/DB명은 [02-access.md](02-access.md) 참고) | 뷰 6개 |
|---|---|---|
| **pv** | 발전량·발전소·SMP·기상 | `generation`, `plants`, `smp_hourly`, `smp_realtime_jeju`, `smp_weighted_avg`, `weather_asos` |
| **demand** | 전국/제주 수급·열수요 | `demand_5min`, `jeju_supply_demand`, `heat_demand`, `heat_demand_location`, `demand_weather_1h` |

## 시작하는 순서

1. [03-terms.md](03-terms.md)를 읽고 서약한다 (전체 쿼리가 감사 로그에 남는다는 점 포함).
2. [02-access.md](02-access.md)로 Tailscale에 연결하고 발급받은 role로 접속을 확인한다.
3. [01-data.md](01-data.md)에서 쓰려는 뷰의 함정(시간 규약, 단위, 품질 등급)을 먼저 읽는다.
   특히 `research.plants.data_quality`와 `research.generation.gen_kwh` 단위는 결과를
   완전히 뒤집을 수 있는 함정이니 반드시 확인한다.
4. [02-access.md](02-access.md)의 예제 쿼리를 자신의 분석에 맞게 고쳐 쓴다.

## 참고 — 수집 방법에 대하여

이 문서는 데이터의 **의미·단위·품질·시간 규약**을 다룬다. 어떤 사이트에서
어떤 방식으로 데이터를 긁어오는지는 이 문서의 범위가 아니며, 보안상 다루지
않는다. 원천은 대체로 공공데이터포털·발전사 공개 자료 기반이라고만
알아두면 된다.
