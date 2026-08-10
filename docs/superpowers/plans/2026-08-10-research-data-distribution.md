# 연구원 데이터 배포 (Phase 1)

## 배경

과제 참여 연구원 ~5명(동일 기관)에게 현재 수집 중인 전력·기상 데이터를 제공한다.
공인 IP에 아무것도 노출하지 않고, Tailscale 폐쇄망 안에서 **읽기전용 Postgres 직접 접속**을
코어 인터페이스로 삼는다. 그 위에 **로컬 stdio MCP 서버(`run_sql` 하나)**를 선택 층으로 얹는다.

일반 외부 공개(REST API + nginx + Redis + API key)는 **Phase 2**이며 이번 범위가 아니다.
Phase 1의 `research` 스키마 뷰를 Phase 2 REST가 그대로 덮는 것을 전제로 설계한다.

### 확정된 설계 결정

| 항목 | 결정 |
|---|---|
| 노출 | Tailscale 폐쇄망. 공인 IP 미노출 |
| 인터페이스 | 읽기전용 Postgres 직접 접속(코어) + 로컬 stdio MCP(선택) |
| 계정 | 연구원 1인당 Postgres role 1개 (감사 추적) |
| 공개 면 | `research` 스키마 뷰만 GRANT, `public` 테이블은 REVOKE |
| 범위 | pv DB(발전량·SMP·기상) + demand DB(수요·제주수급·열수요) 전부 |
| 보안 | 수집 방법 은폐(`source` 컬럼 제외, 파이프라인 코드 비공개) + 감사로그·서약서 |
| 워터마크 | 하지 않음 (연구 데이터 무결성 우선) |
| 시간 규약 | `research` 뷰에서 **구간시작 KST로 통일** |
| MCP 배포 | 연구원 PC 로컬 stdio. 별도 공개 GitHub repo |
| 문서 | GitBook — 카탈로그 / 스키마 사전 / 접속 가이드 / 약관·서약서 |

### 만들지 않는 것 (명시적 제외)

nginx, Redis, Kafka, REST API, IP allowlist, canary 워터마크, 전량 덤프 차단, 복제 DB.
현재 아키텍처에 HTTP 계층이 없으므로 걸 자리가 없다. Phase 2에서 재검토한다.

## Global Constraints

1. **한국어**가 기본이다. 코드 주석·문서·커밋 메시지 모두 한국어로 쓴다.
2. **운영 테이블의 기존 스키마를 변경하지 않는다.** 신규 테이블(`weather_asos`) 추가는 허용.
   기존 컬럼 삭제/이름변경/타입변경/데이터 시프트는 금지 — Grafana와 Energy-hub가 붙어 있다.
3. **`research` 스키마의 모든 시계열 뷰는 구간시작(interval-start) KST로 통일한다.**
   원본 테이블은 건드리지 않고 뷰에서만 보정한다.
4. **`generation.source` 컬럼은 어떤 뷰에도 노출하지 않는다.** (수집 방법 은폐)
5. **비밀번호·DSN·API 키를 저장소에 커밋하지 않는다.** SQL은 플레이스홀더를 쓰고,
   실제 값은 `.env` 또는 별도 전달 채널로 처리한다.
6. DB 접속은 기존 `fetch_data/common/db_base.py`의 `get_engine()`/`get_session()`을 쓴다.
   새 엔진 팩토리를 만들지 않는다.
7. 테스트는 `pytest`, `tests/` 아래에 둔다. DB가 필요한 테스트는 DB 없이도 수집·통과하도록
   스킵 조건을 건다(`pytest.mark.skipif`).
8. 새 서드파티 의존성을 추가하지 않는다. 기존 `pyproject.toml` 의존성으로 해결한다.
   (예외: Task 4의 MCP 서버는 별도 패키지이므로 자체 의존성을 가진다.)

---

## Task 1: 수집기 시간규약 전수 검증

### 목적

`generation` 테이블 안에서 수집기마다 시간 라벨 처리가 다르다는 정황이 있다.
Task 3의 뷰 보정식을 쓰려면 **operator × fuel_type 조합별로 정확히 어떤 규약인지**
확정해야 한다. 추측으로 뷰를 만들면 전체 연구 결과가 1시간 밀린다.

### 확인된 정황 (검증 대상)

| 파일:라인 | 코드가 하는 일 | 추정 규약 |
|---|---|---|
| `fetch_data/smp/smp_collect.py:84` | 라벨 N → (N-1)시 | 구간시작 |
| `fetch_data/pv/ekr_collect.py:9` | 라벨 N → (N-1)시 | 구간시작 |
| `fetch_data/pv/namdong_collect.py:310` | `date+(hour-1)` | 구간시작 |
| `fetch_data/gen/transform_gen.py:93` | `base + hour_num % 24`, 24→익일 00시 | **구간종료(미보정)** |
| `fetch_data/pv/nambu_transform.py:88` | `ymd + timedelta(hours=hour)`, qhorgen01→01:00 | 미확정 |

`generation` 테이블 컬럼 주석은 "원천 테이블의 시간 규약을 변환 없이 보존
(남부=구간시작, KOEN계열=hour-ending→N시 표기)"라고 되어 있어 어긋남을 인정하고 있다.

### 해야 할 일

1. `fetch_data/` 아래 **`generation` 테이블에 쓰는 모든 경로**를 전수 조사한다.
   최소한 다음을 포함한다:
   - `fetch_data/pv/nambu_transform.py`, `nambu_collect.py`, `nambu_backfill.py`, `nambu_bulk_sync.py`
   - `fetch_data/pv/namdong_collect.py`, `namdong_transform.py`
   - `fetch_data/pv/ekr_collect.py`
   - `fetch_data/gen/transform_gen.py`, `pipeline.py`, `load_gen.py`
   - `fetch_data/wind/namdong_collect.py`, `seobu_backfill.py`, `hangyoung_backfill.py`
2. 각 경로에 대해 **원천의 시간 라벨이 무엇을 뜻하는지**(구간시작/구간종료)와
   **코드가 그것을 timestamp로 어떻게 바꾸는지**를 코드 근거(파일:라인)와 함께 기록한다.
   원천 규약이 코드만으로 불분명하면 "불명"으로 남기고 근거를 적는다 — 추측으로 단정하지 않는다.
3. 가능하면 **데이터로 교차검증**한다. 태양광은 일출/일몰 시각과 발전 개시/종료 시각을
   비교하면 1시간 시프트가 드러난다. DB 접속은 `docker exec pv-data-postgres psql -U pv -d pv`로 한다.
   (예: 여름철 남부 태양광의 첫 발전 시각 vs 남동 태양광의 첫 발전 시각 비교)
4. 결과를 `docs/time-convention-audit.md`에 쓴다. 반드시 다음 표를 포함한다:

   | operator | fuel_type | 원천 라벨 의미 | 코드 처리 | DB 저장 규약 | 근거(파일:라인) | 뷰 보정 필요 |
   |---|---|---|---|---|---|---|

   "뷰 보정 필요" 열은 `없음` 또는 `-1시간` 중 하나로 확정한다(불명이면 `불명`).
5. **Task 3이 그대로 쓸 수 있는 보정 규칙**을 문서 말미에 SQL 조각으로 제시한다. 예:
   ```sql
   -- operator IN ('namdong','seobu','hangyoung') AND fuel_type <> 'solar' 인 행은 timestamp - 1h
   ```
   실제 조건은 검증 결과에 따른다.
6. 회귀 방지 테스트 `tests/test_time_convention.py`를 만든다.
   각 transform 함수에 **최소 입력을 직접 넣어** 라벨 → timestamp 매핑을 assert한다.
   DB 없이 순수 함수 단위로 통과해야 한다.
   최소한 `transform_gen.py`와 `nambu_transform.py`의 매핑을 고정한다.

### 완료 기준

- `docs/time-convention-audit.md`에 위 표가 채워져 있고, 모든 행의 "뷰 보정 필요"가 확정됨
- 각 판정에 코드 근거(파일:라인)가 붙어 있음
- `tests/test_time_convention.py`가 DB 없이 통과
- **코드 수정은 하지 않는다.** 이 태스크는 조사와 테스트 추가만 한다.

---

## Task 2: ASOS 기상 DB 적재

### 목적

일사량(solar radiation)은 태양광 연구의 핵심 변수인데 지금 어떤 DB에도 없고
`data/asos_all_merged.csv`(548k행, 21MB)로만 존재한다. 이걸 pv DB 테이블로 올려
모든 데이터가 SQL 하나로 접근되게 한다.

### 현재 상태

- 매일 09:00 `daily-weather-collection-flow`(`prefect_flows/prefect_pipeline.py:97`)가
  ASOS를 수집해 `data/asos_YYYYMMDD_YYYYMMDD.csv`로 저장하고
  `merge_weather_to_all()`(`:72`)로 `data/asos_all_merged.csv`에 병합한다. **DB 적재 단계가 없다.**
- CSV 컬럼: `date, humidity, temperature, station_name, solar radiation`
- 수집기: `fetch_data/weather/asos_collect.py` — `normalize_weather_data(df)`(`:148`)가 정규화

### 해야 할 일

1. `fetch_data/weather/database.py`에 SQLAlchemy 모델 `weather_asos`를 만든다.
   - 컬럼: `timestamp`(DateTime, KST, not null), `station_name`(String(50), not null),
     `temperature`(Float), `humidity`(Float), `solar_radiation`(Float)
   - `UNIQUE(timestamp, station_name)` 제약, `timestamp` 인덱스
   - 기존 `fetch_data/*/database.py` 파일들의 스타일(컬럼 `comment=` 한국어)을 따른다
   - **테이블 생성 함수도 함께 제공한다** (기존 `database.py`들의 패턴을 그대로 따를 것)
2. 적재 함수 `load_asos_df(df)`를 만든다.
   - CSV/DataFrame → 위 스키마로 매핑 (`solar radiation` → `solar_radiation`)
   - **UPSERT**: `(timestamp, station_name)` 충돌 시 갱신 (`ON CONFLICT DO UPDATE`)
   - 기존 수집기들의 UPSERT 패턴을 재사용한다 — 새로 발명하지 않는다
3. 백필 스크립트 `fetch_data/weather/asos_backfill.py`를 만든다.
   - `data/asos_all_merged.csv` 전체를 읽어 `load_asos_df`로 적재
   - 멱등해야 한다(두 번 돌려도 행 수가 안 늘어남)
   - 진행 상황을 로그로 출력 (기존 `fetch_data/common/logger.py` 사용)
4. `prefect_flows/prefect_pipeline.py`의 `daily_weather_collection_flow`에
   **적재 태스크를 한 단계 추가**한다. `merge_weather_to_all` 이후 또는 병렬로,
   그날 수집한 DataFrame을 `load_asos_df`로 적재한다.
   - 기존 태스크 스타일(`@task`, `.submit()`)을 따른다
   - 적재 실패가 CSV 저장을 되돌리지 않게 한다
5. 테스트 `tests/test_asos_load.py`:
   - 컬럼 매핑(`solar radiation` → `solar_radiation`)이 맞는지
   - 결측/빈 DataFrame에서 예외 없이 0건 처리되는지
   - DB 필요 테스트는 `skipif`로 감싼다

### 완료 기준

- `weather_asos` 테이블 모델과 생성 함수가 있음
- `asos_backfill.py`가 멱등하게 전체 CSV를 적재함
- 일일 flow가 적재 단계를 포함함
- `tests/test_asos_load.py`가 DB 없이 통과
- **실제 백필 실행은 하지 않는다** — 스크립트만 만들고, 실행은 컨트롤러가 검토 후 결정한다

---

## Task 3: research 스키마 · 뷰 · 읽기전용 role

**선행: Task 1(시간규약 확정), Task 2(`weather_asos` 존재)**

### 목적

연구원이 보는 유일한 면을 만든다. 운영 테이블은 감추고, `research` 스키마의 뷰만 노출한다.

### 해야 할 일

1. `sql/research/pv_research_schema.sql` (pv DB 대상):
   - `CREATE SCHEMA IF NOT EXISTS research;`
   - 뷰 생성:
     | 뷰 | 원본 | 비고 |
     |---|---|---|
     | `research.plants` | `public.plants` | 마스터. 전 컬럼 노출 가능 |
     | `research.generation` | `public.generation` + `plants` 조인 | **`source` 컬럼 제외**, 시간 구간시작 통일 |
     | `research.smp_hourly` | `public.smp_hourly` | `id` 제외 |
     | `research.smp_realtime_jeju` | `public.smp_realtime_jeju` | `id` 제외 |
     | `research.smp_weighted_avg` | `public.smp_weighted_avg` | `id` 제외 |
     | `research.weather_asos` | `public.weather_asos` | Task 2 산출 |
   - `research.generation`은 **Task 1의 `docs/time-convention-audit.md`가 제시한 보정 규칙**을
     그대로 적용한다. 보정이 필요한 조합만 `timestamp - INTERVAL '1 hour'` 한다.
   - `research.generation`은 `plant_id`뿐 아니라 `plant_name`·`operator`·`fuel_type`을 함께
     내보내 연구원이 조인 없이 쓸 수 있게 한다.
2. `sql/research/demand_research_schema.sql` (demand DB 대상):
   - `research` 스키마 + 뷰: `demand_5min`, `jeju_supply_demand`, `heat_demand`,
     `heat_demand_location`, `demand_weather_1h` (각각 `id`·`created_at` 제외)
   - 이 DB의 테이블들도 시간 규약을 Task 1 문서 기준으로 확인해 필요하면 보정한다
3. `sql/research/create_research_role.sql` — role 발급 템플릿:
   - `CREATE ROLE :role_name LOGIN PASSWORD :'password';` (psql 변수 사용, 값 하드코딩 금지)
   - `REVOKE ALL ON SCHEMA public FROM :role_name;`
   - `GRANT USAGE ON SCHEMA research TO :role_name;`
   - `GRANT SELECT ON ALL TABLES IN SCHEMA research TO :role_name;`
   - `ALTER ROLE :role_name SET statement_timeout = '60s';`
   - `ALTER ROLE :role_name CONNECTION LIMIT 5;`
   - `ALTER ROLE :role_name SET log_statement = 'all';` (개인별 감사 추적)
   - `ALTER DEFAULT PRIVILEGES IN SCHEMA research GRANT SELECT ON TABLES TO :role_name;`
4. `sql/research/revoke_research_role.sql` — 회수 스크립트(과제 종료·이탈 시)
5. `sql/research/verify_research_access.sql` — 검증 쿼리:
   - research role로 `public` 테이블 SELECT가 **거부**되는지
   - `research` 뷰 SELECT가 **허용**되는지
   - 뷰에 `source` 컬럼이 **없는지**
   - 각 뷰의 행수·기간이 원본과 일치하는지(보정 뷰는 1시간 시프트 감안)
6. `README.md` 또는 `sql/research/README.md`에 실행 순서를 적는다.

### 완료 기준

- 위 SQL 파일들이 존재하고 문법 오류가 없음 (`psql -f ... --dry-run` 대신
  실제 DB에 적용해 검증하되, **role 생성 시 비밀번호는 psql 변수로 주입**한다)
- `research.generation`에 `source` 컬럼이 없음
- Task 1의 보정 규칙이 뷰에 정확히 반영됨
- 검증 SQL이 통과함
- **운영 테이블은 한 줄도 바뀌지 않음**

---

## Task 4: MCP 서버 (`run_sql`)

**선행: Task 3(`research` 스키마 확정)**

### 목적

연구원이 자연어 LLM 클라이언트(Claude Desktop / VS Code / Continue 등)에서
데이터를 탐색할 수 있게 하는 얇은 층. 로컬 stdio로 돌며, **연구원 본인의 role 자격증명**을
쓰므로 개인별 감사 추적이 유지된다.

### 해야 할 일

`mcp-server/` 디렉터리에 독립 패키지로 만든다 (추후 별도 공개 GitHub repo로 분리 예정).
**이 저장소의 수집 로직을 import하지 않는다** — 공개될 코드이므로 수집 방법이 새면 안 된다.

1. `mcp-server/pyproject.toml` — 패키지명 `energy-mcp`, `uvx energy-mcp`로 실행 가능하게
   엔트리포인트를 설정한다. 의존성은 MCP Python SDK와 DB 드라이버로 최소화한다.
2. `mcp-server/energy_mcp/server.py`:
   - **stdio transport** MCP 서버
   - 툴 `run_sql(query: str)` 하나만 노출
     - 환경변수 `ENERGY_MCP_DSN`(연구원 본인 role DSN)으로 접속
     - **읽기전용 강제**: 접속 후 `SET TRANSACTION READ ONLY` 또는 read-only 세션으로 고정.
       DB 권한이 이미 SELECT-only지만 **애플리케이션 층에서도 막는다**(다층 방어)
     - `statement_timeout`을 세션에 설정 (기본 60초, 환경변수로 조정 가능)
     - 결과 행 수 상한(기본 10,000행, 환경변수로 조정)을 두고, 잘렸으면 응답에 명시한다
       — 조용한 절단은 금지
     - 에러는 사용자에게 읽을 수 있는 메시지로 반환한다(스택트레이스 노출 금지)
   - **MCP resource**로 `research` 스키마 사전을 노출한다.
     LLM이 이걸 읽고 SQL을 쓰므로 정확해야 한다. `information_schema`에서 동적으로 읽거나
     정적 마크다운으로 제공한다. **Task 1이 확정한 시간 규약과 `gen_kwh` 단위를 반드시 포함**한다.
3. `mcp-server/README.md` — 설치·설정 방법:
   - `uvx energy-mcp` 실행법
   - Claude Desktop / VS Code MCP 설정 JSON 예시
   - `ENERGY_MCP_DSN` 설정법 (**예시 DSN에 실제 비밀번호를 넣지 않는다**)
4. `mcp-server/tests/test_server.py`:
   - `run_sql`이 쓰기 쿼리(`INSERT`/`UPDATE`/`DELETE`/`DROP`)에 대해 실패하는지
   - 행 수 상한이 동작하고 잘림이 응답에 표시되는지
   - DB 없이 통과하도록 커넥션을 모킹한다

### 완료 기준

- `mcp-server/`가 이 저장소의 다른 코드를 import하지 않음 (독립 패키지)
- `run_sql`이 읽기전용으로 강제됨 (애플리케이션 층 + DB 권한 이중)
- 행 수 상한과 절단 표시가 동작함
- 스키마 리소스에 시간 규약·단위가 포함됨
- 테스트가 DB 없이 통과
- **실제 GitHub repo 생성·push는 하지 않는다** — 코드만 준비한다

---

## Task 5: GitBook 문서

**선행: Task 1(규약), Task 2(테이블), Task 3(뷰), Task 4(MCP)**

### 목적

연구원이 자립할 수 있는 문서. GitBook에 올릴 마크다운을 저장소에 작성한다.

### 해야 할 일

`docs/gitbook/` 아래에 작성한다.

1. `docs/gitbook/README.md` — 개요 + 목차
2. `docs/gitbook/01-data-catalog.md` — **데이터 카탈로그**
   데이터셋별 행수·기간·발전소 수·갱신 주기·알려진 결측.
   실제 수치는 DB에서 조회해 채운다(`docker exec pv-data-postgres psql ...`).
   갱신 주기는 `prefect_flows/deploy.py`의 스케줄에서 가져온다.
3. `docs/gitbook/02-schema-dictionary.md` — **스키마 사전**
   `research` 뷰별 컬럼·의미·단위·시간 규약.
   반드시 명시할 함정:
   - `gen_kwh`의 단위 (KOEN 원천은 MWh 라벨이지만 실제 kWh — 확인 후 정확히 기술)
   - 모든 시각은 **KST 구간시작**으로 통일되어 있다는 점
   - `capacity_confidence`(확실/근사/불확실)의 의미
   - `smp_weighted_avg`의 `period_type`·`price_type` 값 종류
4. `docs/gitbook/03-access-guide.md` — **접속 가이드 + 예제 쿼리**
   - Tailscale 설치·로그인
   - psql / pandas(`read_sql`) / R 접속 예시
   - MCP 서버 설치 및 클라이언트 설정 (Claude Desktop, VS Code, Continue)
   - 자주 쓰는 예제 쿼리 5~10개 (월별 집계, 발전소별 비교, 발전량×일사량 결합 등)
   - **비밀번호는 `<발급받은_비밀번호>` 플레이스홀더로 둔다. 실제 값 금지.**
   - tailnet 주소도 `<tailnet-host>` 플레이스홀더로 둔다
5. `docs/gitbook/04-terms.md` — **이용약관·서약서**
   - 과제 목적 외 사용 금지, 재배포 금지
   - 논문·보고서 인용 의무 (인용 문구 예시 포함)
   - 전체 쿼리가 감사 로그로 기록된다는 고지
   - 과제 종료 시 접근 회수 및 사본 파기
   - **초안임을 명시한다** — 법적 검토 전
6. `docs/gitbook/SUMMARY.md` — GitBook 목차 파일
7. `docs/gitbook/appendix-local-llm.md` — **부록: 로컬 LLM (선택)**
   외부 LLM 서버로 조회 결과가 나가는 것이 꺼려지는 연구원용 오프라인 경로.
   - mistral.rs 설치: `pip install mistralrs` (CPU 전용 prebuilt wheel, GPU·Rust 툴체인 불필요)
   - 모델: Qwen3 4B Q4_K_M (RAM ~3GB). Qwen3.5 GGUF는 arch 미인식 이슈가 있으므로 피할 것
   - `mcp.json` 설정 예시 (Process transport로 `uvx energy-mcp` 기동)
   - **한계를 명시한다**: 4B 모델은 다단계·중첩 쿼리에서 정확도가 떨어진다.
     탐색·스키마 파악용이며 최종 분석 쿼리는 사람이 확인해야 한다
   - 본문 흐름을 방해하지 않게 부록으로 분리하고 `03-access-guide.md`에서 링크만 건다

### 완료 기준

- 위 6개 파일이 존재하고 상호 링크가 맞음
- 카탈로그 수치가 실제 DB 조회 결과와 일치
- 스키마 사전이 Task 3의 실제 뷰 정의와 일치
- **어떤 파일에도 실제 비밀번호·DSN·tailnet 주소가 없음**
- 약관 문서에 "법적 검토 전 초안" 표기가 있음

---

## Task 6: 원천이 깨진 태양광 발전소 원인 조사

**Task 3의 선행 조건.** 이 결과 없이는 뷰에서 이 발전소들을 어떻게 다룰지 정할 수 없다.

### 배경

Task 1의 §6-2가 argmax(최대 발전 시각)가 11~13시를 벗어나는 태양광 12기를 찾았다.
컨트롤러가 데이터를 직접 확인한 결과 **증상이 두 유형으로 갈린다**:

**유형 1 — 일 총량이 23시 한 칸에 (3기)**: `여수태양광`, `탑선태양광_1`, `탑선태양광_3`
2026-06 시간별 평균에서 0~22시가 전부 0.0이고 23시에만 637.1kWh. 일별 값이 시간별 테이블에
잘못 들어간 형태로 보인다.

**유형 2 — 야간 발전량이 정오보다 큼 (9기)**: `신인천전망대`, `영동태양광`,
`영흥태양광 #3_1/2/3`, `삼천포태양광#5_1/2`, `삼천포태양광#6`, `삼천포태양광_4`
`신인천전망대`의 6월 데이터를 연도별로 보면 야간(20~23시) 평균이 정오(10~13시) 평균의
2~3배이고, 이 패턴이 **2021~2026년 내내 일관**된다:

| 연도 | argmax | 야간(20~23시) 평균 | 정오(10~13시) 평균 |
|---|---|---|---|
| 2021 | 18시 | 348.6 | 134.7 |
| 2023 | 17시 | 399.9 | 132.1 |
| 2024 | 17시 | 427.9 | 190.6 |
| 2025 | 17시 | 321.4 | 176.7 |

6월 일몰이 19시 40분경인데 21시에 514kWh가 찍힌다. 물리적으로 불가능하다.
어느 시점에 고장난 게 아니라 처음부터 이 상태다.

### 해야 할 일

1. **유형 2의 원인을 규명한다.** 최소한 다음 가설들을 검증하고 각각 기각/채택 근거를 적어라:
   - 시간 라벨이 크게 어긋남 (몇 시간 시프트?)
   - 원천이 발전량이 아닌 다른 지표를 담고 있음 (누적값? 역률? 다른 단위?)
   - 해당 설비가 태양광이 아님 (ESS 연계? 전망대 조명? `plants.plant_name`을 의심하라)
   - 여러 발전소 값이 한 `plant_id`에 섞임
   - 원천 CSV/API 응답 자체가 그렇게 옴 (수집 버그가 아님)
2. **원천과 대조하라.** 가능한 경로:
   - `pv_data_raw/`, `gen_data_raw/` 아래 원본 CSV에서 해당 발전소 행을 찾아 DB 값과 비교
   - 남부 발전소면 공공데이터포털 API(`SERVICE_KEY`는 `.env`)로 특정 일자를 재조회해 대조
   - 수집·변환 코드 경로 추적 (Task 1의 `docs/time-convention-audit.md` "조사 범위" 절이 지도)
3. **유형 1도 같은 방식으로 확인한다.** 원천이 일별 값인지, 시간별인데 변환에서 뭉개진 건지.
4. **각 발전소를 세 등급 중 하나로 분류한다**:
   - `정상` — 오탐이었음 (근거 필요)
   - `시간별무효` — 시간별 분석에 쓸 수 없음. 일별 합계는 쓸 수 있는지도 판정
   - `전면무효` — 일별 합계조차 신뢰 불가
5. 결과를 `docs/broken-plants-audit.md`에 쓴다. 반드시 포함:

   | plant_name | plant_id | 유형 | 증상 | 원인(또는 미규명) | 근거 | 등급 | 일별합계 사용가능 |
   |---|---|---|---|---|---|---|---|

6. **Task 3이 그대로 쓸 SQL 조각**을 문서 말미에 제시한다 — `plants` 뷰에 붙일
   `data_quality` 컬럼 CASE 식, 또는 제외 대상 `plant_id` 목록.

### 완료 기준

- 12기 전부가 세 등급 중 하나로 분류됨
- 각 판정에 근거(원천 대조 결과 또는 코드 파일:라인)가 붙음
- **원인을 못 밝힌 발전소는 "미규명"으로 남기되 등급은 반드시 매긴다** — 연구원 보호가 우선이므로
  원인 불명이면 보수적으로(무효 쪽으로) 분류하고 그 판단을 명시한다
- Task 3이 복사할 SQL 조각이 있음
- **수집기 코드를 수정하지 않는다.** 조사와 문서화만 한다. 수정이 필요하면 별도 태스크로 제안하라

---

## Task 7: ASOS 일사량 수집 재개 + 과거 백필

**선행: Task 2(`weather_asos` 테이블)**

### 배경

일사량은 태양광 발전량 연구의 핵심 변수인데 **사실상 없다**:
- `data/asos_all_merged.csv` 548,030행 중 `solar radiation` non-null은 **14,664행(2.68%)**
- 존재 범위는 **2019-01-01 08:00 ~ 2019-01-31 19:00, 43개 지점뿐**
- 오늘자 일별 CSV 컬럼은 `date,humidity,temperature,station_name` — **일사량 컬럼 자체가 없다**
- 즉 현재 수집기 `fetch_data/weather/asos_collect.py`가 일사량 필드를 요청하지 않는다

### 해야 할 일

1. **기상청 ASOS 시간자료 API의 일사량 필드를 확인한다.** 응답에 어떤 키로 오는지,
   단위가 무엇인지, **어느 지점이 일사를 관측하는지**(전 지점이 관측하지 않는다) 확인하라.
   `config/station_list.csv`의 95개 지점 중 몇 곳이 해당되는지 실제 API 응답으로 확인할 것.
2. `fetch_data/weather/asos_collect.py`가 일사량을 함께 수집하도록 수정한다.
   - `normalize_weather_data`(`:148`)의 컬럼 매핑에 일사량 추가
   - **기존 기온·습도 수집 동작을 바꾸지 않는다** — 일일 flow가 매일 09:00에 운영에서 돈다
   - 일사 미관측 지점은 NULL로 두고 정상 처리되어야 한다
3. **과거 백필 스크립트**를 만든다. 기존 백필 스크립트들의 패턴을 따르되:
   - 기간을 인자로 받고, 재실행해도 안전(멱등)해야 한다
   - `weather_asos`의 UPSERT가 이미 COALESCE 기반이라 기존 기온·습도를 지우지 않는지 확인하라
     (Task 2 리뷰가 이 동작을 확인했다 — 그 전제가 유지되는지 검증할 것)
   - API 호출량 제한을 고려한 속도 조절 (기존 수집기의 비동기 패턴 재사용)
4. 테스트: 일사량 컬럼 매핑, 일사 미관측 지점의 NULL 처리, 기존 기온·습도 경로 회귀
5. **백필을 실제로 전 기간 실행하지 마라.** 소량 구간(예: 하루)으로 동작을 검증하고,
   전체 실행은 컨트롤러가 결정한다. 실제 API 호출량이 크므로 무단 대량 호출은 금지.

### 완료 기준

- 일일 flow가 일사량을 함께 수집·적재함
- 기존 기온·습도 수집이 회귀 없이 동작함 (테스트로 고정)
- 과거 백필 스크립트가 존재하고 소량 구간으로 검증됨
- **일사 관측 지점 목록과 미관측 지점 처리 방식이 문서화됨** (Task 5 스키마 사전이 쓸 것)
- 테스트가 DB·API 없이 통과

---

## Task 8: 발전소 좌표 검증·수정

**Task 3의 선행 조건.** 좌표가 틀리면 발전량×기상 조인이 통째로 어긋난다.

### 문제

`plants.lat/lon`의 출처가 둘인데 값이 다르다. **DB 실측은 `pv_test/init_db.py` 쪽과 일치**한다.

| 발전소 | `fetch_data/pv/database.py` | `pv_test/init_db.py` = DB 실측 | 차이 |
|---|---|---|---|
| 영동태양광 | 37.1837 / 128.9437 (`:155`) | 37.7519 / 128.8761 (`:53`) | 약 63km |
| 탑선태양광_1·_3 | 35.9078 / 128.8097 (`:188-189`) | 35.2733 / 126.7297 (`:60`) | **약 200km** |

어느 쪽이 옳은지 **아직 아무도 확인하지 않았다.**

### 제약: 주소로 대조할 수 없다

`plants`의 좌표·주소 보유 현황:

| fuel_type | 발전소 | lat/lon | address |
|---|---|---|---|
| solar | 45 | 26 | **3** |
| thermal | 24 | 24 | 24 |
| fuel_cell | 8 | 8 | 8 |
| hydro | 4 | 4 | 4 |
| wind | 10 | 4 | 4 |

태양광은 주소가 3기뿐이라 DB 내부 대조가 불가능하다. **발전소명이 대부분 지명을 포함**한다는 점을 이용하라(여수·탑선·영흥·삼천포·예천·광양항세방·경상대·화순·장흥 등).

### 해야 할 일

1. **두 소스의 좌표를 전수 비교**한다. `fetch_data/pv/database.py`와 `pv_test/init_db.py`에서
   같은 발전소를 가리키는 항목을 짝지어 불일치 목록을 만들어라. 몇 기가 다른지 먼저 세라.
2. **불일치 건마다 어느 쪽이 옳은지 판정**한다. 판정 근거 우선순위:
   - 발전소명의 지명과 좌표의 실제 행정구역이 맞는가 (역지오코딩 대신 좌표 범위로 판단 가능:
     예 — 전남 영광은 대략 35.2~35.4N/126.4~126.6E, 경북 군위는 36.2N/128.6E)
   - 같은 사이트의 다른 설비(예: `영흥태양광`과 `영흥_4`(thermal))가 이미 주소를 갖고 있으면 그 좌표와 비교
   - 발전사 공시자료·공개 발전소 목록 (웹 조회 허용)
3. **좌표가 아예 없는 태양광 19기**도 가능한 범위에서 채워라. 못 채우면 그대로 두고 목록에 남겨라.
   추측으로 채우지 마라 — 없는 게 틀린 것보다 낫다.
4. 결과를 `docs/plant-coordinates-audit.md`에 쓴다:

   | plant_id | plant_name | 현재 DB 좌표 | database.py | init_db.py | 판정 | 근거 | 조치 |
   |---|---|---|---|---|---|---|---|

5. **수정 SQL을 `sql/fix_plant_coordinates.sql`로 낸다.** `plants` UPDATE 문 + 실행 전후 검증 쿼리.
   `plant_id`를 명시적으로 지정하고, 되돌릴 수 있게 이전 값을 주석에 남겨라.
6. **SQL을 실행하지 마라.** 컨트롤러가 검토 후 결정한다.

### 완료 기준

- 두 소스 불일치 목록이 완전함 (몇 기인지 확정)
- 각 불일치에 판정과 근거가 붙음. 판정 불가는 "미확정"으로 남기되 그 사실을 명시
- 수정 SQL이 존재하고 되돌릴 수 있음
- **운영 DB에 쓰지 않음** — SELECT만
- 수집기 코드는 수정하지 않음 (좌표 소스 통합은 별도 태스크로 제안만)

---

## 이번 범위 밖 (별도 처리)

- Tailscale 설치·ACL 설정·연구원 초대 — 인프라 작업, 사람이 직접
- 연구원별 role 실제 발급 및 비밀번호 전달 — 운영 작업
- `asos_backfill.py` 실제 실행 — 검토 후 결정
- MCP 서버 GitHub repo 생성·push — 검토 후 결정
- GitBook 실제 업로드·공개 설정 — 검토 후 결정
- ASOS 일사량 전 기간 백필 실제 실행 — Task 7 검토 후 결정

## 실행 순서

Task 6(깨진 발전소 조사)이 Task 3(뷰 정의)의 선행 조건이므로 번호순이 아니다:

`1 → 2 → 6 → 3 → 4 → 7 → 5`

- Task 6이 `plants` 뷰의 `data_quality` 컬럼 정의를 결정한다
- Task 7은 Task 3의 뷰 정의를 바꾸지 않는다(`solar_radiation` 컬럼은 이미 스키마에 있음).
  다만 Task 5의 데이터 카탈로그 수치에 영향을 주므로 Task 5 앞에 둔다
- Phase 2 (외부 공개 REST API + nginx + Redis + API key)
