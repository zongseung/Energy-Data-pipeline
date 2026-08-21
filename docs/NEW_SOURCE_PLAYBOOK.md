# NEW_SOURCE_PLAYBOOK — 스펙 → 수집·적재 파이프라인 구현 레시피

`intake/<source>.spec.md`(양식: `docs/SOURCE_SPEC_TEMPLATE.md`)를 받아 새 데이터 소스의 수집·적재 파이프라인을 만드는 표준 절차. **기존 패턴을 복제**해 공통 인프라를 100% 재사용한다(범용 플랫폼 + 소스별 반자동 스캐폴딩).

## 0. 원칙
- 공통 인프라는 재사용, **소스별 수집 로직만 새로** 작성.
- 라이브 소스 디버깅은 필수(스펙이 모든 현실을 담지 못함 — CSRF·SSL체인·rowspan 등).
- 단일 writer / 시간규약 / CSV미러 일치 / Prefect E2E를 반드시 검증.

## 1. 모듈 배치 — `fetch_data/<source>/`
- `<source>_collect.py` — 원천 수집(요청/파싱). 스크래핑이면 `fetch_data/smp/smp_scraper.py` 참고.
- `transform_*.py` — 변환(필요 시). wide→long 등은 `fetch_data/gen/transform_gen.py` 참고.
- `_common.py` — upsert/CSV미러/증분 헬퍼. **`fetch_data/smp/_common.py` 복제**가 가장 빠름.
- `pipeline.py`(선택) — 수집→변환 오케스트레이터(증분 모드). **`fetch_data/gen/pipeline.py`**(latest/months/range/full) 복제.

## 2. DB 테이블 — `fetch_data/<source>/database.py`
`fetch_data/smp/database.py` 패턴 복제:
- 독립 `declarative_base()` (기존 테이블 무영향)
- ORM 클래스 + **unique `Index`**(스펙의 unique key)
- `init_db()` / `get_engine()`(=`resolve_db_url`로 도커/호스트 자동전환) / `get_session()`
- 컬럼 `comment`로 시간규약·단위 명시.

## 3. 적재 — upsert + CSV 미러 (`fetch_data/smp/_common.py` 재사용)
- `_upsert(engine, sql, records, label)` — 배치 upsert(`ON CONFLICT ... DO UPDATE`)
- `mirror_table_to_csv(engine, table)` — DB 전체를 `<source>_data/<table>.csv`로 미러(DB=CSV 항상 일치)
- `get_max_timestamp(engine, table, region=None)` — 증분 시작점
- `get_engine_for(db_url)` / `parse_price` 등 그대로 활용.

## 4. 공통 모듈 재사용 — `fetch_data/common/`
- `impute_missing.impute_missing_values` — spline+과거평균 결측보간
- `config` / `db_utils.resolve_db_url` — 환경별 DB URL
- `utils`(now_kst/today_kst/parse_hour_column/KST), `date_utils`, `logger.get_logger`

## 5. 시간 규약
KPX/전력시장 데이터는 1~24시 hour-ending → **구간시작(0~23시)** 변환(`smp` 규약 재사용). 적재 후 원본과 1:1 대조해 한 칸 밀림 없는지 확인.

## 6. Prefect flow — `prefect_flows/<source>_flow.py`
`prefect_flows/smp_flow.py` 패턴 복제:
- `@task(retries=2, retry_delay_seconds=300)` 수집 실행
- `@flow(log_prints=True)` try/except + `notify_slack_success/failure`(`prefect_flows/notify_tasks.py`)

## 7. 배포 등록 — `prefect_flows/deploy.py`
`deploy_smp_flow()` 패턴으로 `deploy_<source>_flow()` 추가:
- `Deployment.build_from_flow(flow, name="...", work_pool_name="pv-pool", schedule=...)`
- main의 등록 리스트에 추가 후 `pv-deployer` 재실행.

## 8. 검증 (필수)
1. **값 1:1 대조**: 수집 표본을 라이브 원본과 대조(시간 밀림·결측·유일성).
2. **DB=CSV**: `mirror_table_to_csv` 산출물이 DB와 일치.
3. **Prefect E2E**: 배포 후 flow run `Completed` 확인.
4. **스펙 동거**: `intake/<source>.spec.md` → `fetch_data/<source>/SPEC.md`로 이동.
