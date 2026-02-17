# Energy-Data-Pipeline 리팩토링 분석 (ver2)

기준 시점: 2026-02-17  
검토 기준: 현재 저장소 코드 실물(`refactoring-analysis.md` 재검증)

## 1. 핵심 발견 사항 (우선순위 순)

### P0. import 시점 실패/부수효과
1. `fetch_data/pv/daily_pv_automation.py`
- 모듈 import 시 `API_KEY`, `DB_URL` 검증 후 `RuntimeError` 발생 가능
- 모듈 레벨 `engine = create_engine(DB_URL)` 존재
- 영향: 테스트/툴링/패키지 import 단계에서 즉시 실패 가능

2. `fetch_data/pv/namdong_collect_pv.py`
- 모듈 레벨 `DB_URL = _resolve_db_url(None)` 계산
- 환경변수 부재 시 런타임 진입 전 실패 가능성이 높음(실행 경로 의존)

### P1. 중복 유틸 함수(실제 중복 유지)
1. DB URL 해석 로직 중복 (`_running_in_docker`, `_resolve_db_url`)
- `fetch_data/pv/nambu_backfill.py`
- `fetch_data/pv/namdong_collect_pv.py`
- `fetch_data/wind/namdong_wind_collect.py`
- `fetch_data/wind/seobu_wind_load.py`
- `fetch_data/wind/hangyoung_wind_load.py`

2. Slack 전송 함수 중복
- `notify/slack_notifier.py` (정식 모듈)
- `fetch_data/pv/namdong_collect_pv.py`
- `fetch_data/wind/namdong_wind_collect.py`
- `prefect_flows/prefect_pipeline.py`

### P1. bare `except:` 잔존
- `fetch_data/common/impute_missing.py`
- `fetch_data/pv/daily_pv_automation.py`
- `fetch_data/pv/nambu_probe_date.py`
- `fetch_data/pv/nambu_merge_pv_data.py`
- 영향: `KeyboardInterrupt`, `SystemExit` 등까지 삼켜 디버깅 난이도 증가

### P1. `__init__.py`의 무거운 import
1. `fetch_data/pv/__init__.py`
- `namdong_collect_pv` import로 `.env` 로딩 및 모듈 초기화 부수효과 발생

2. `fetch_data/weather/__init__.py`
- `collect_asos`를 즉시 import하여 패키지 import 비용/결합 증가

3. `fetch_data/wind/__init__.py`
- `database` 모듈 import로 DB URL 해석 관련 초기화 연결됨

### P2. 레거시/혼동 유발 파일
1. `inspect_both_table.py`
- `plant_info`, `pv_generation` 참조(현 코드의 대표 모델명과 불일치)
- 동일 쿼리 2회 실행 중복 존재
- 유지할 경우 현 스키마 기준 재작성 필요

2. 루트 `main.py`
- 실질 기능 없는 placeholder 엔트리포인트

3. `readme.1md`
- `README.md`와 역할 중복 및 파일명 비표준

### P2. Docker 구성 다중 유지
- 루트 `Dockerfile` + `docker-compose.yml`
- `docker/` 하위 별도 Docker 스택
- `pv_test/` 하위 별도 Docker 스택
- 역할이 일부 다르지만 운영 기준이 문서화되어 있지 않아 유지보수 혼선 위험

### P3. 의존성 정리 필요
`pyproject.toml`에 사용 근거가 약한 항목:
- `async>=0.6.2`
- `asyncio>=4.0.0` (표준 라이브러리와 중복)
- `docker>=7.1.0`
- `git-filter-repo>=2.47.0`

## 2. 기존 v1 대비 상태 변경

1. 이미 반영/부분 반영된 항목
- `.gitignore`에 `*.egg-info/` 이미 포함
- `weather_pipeline.egg-info/`는 현재 존재하지만 ignore 설정은 완료 상태

2. v1 내용 중 수정이 필요한 항목
- `prev_month_range` 중복은 `nambu_backfill.py`가 아니라
  `fetch_data/pv/namdong_collect_pv.py`, `fetch_data/wind/namdong_wind_collect.py`에 존재
- bare `except`는 v1에서 언급한 파일 외에도 다수 파일에 잔존

## 3. 권장 실행 순서

### Step 1 (안정성 우선)
1. `daily_pv_automation.py`의 모듈 레벨 엔진/환경검증을 lazy 초기화로 이동
2. bare `except:`를 `except Exception as e:`로 교체하고 최소 로그 남기기

### Step 2 (중복 제거)
1. `fetch_data/common/db_utils.py` 신설
- `running_in_docker()`, `resolve_db_url()` 통합
2. `notify/slack_notifier.py` 단일화
- 수집/플로우 모듈은 import만 사용

### Step 3 (패키지 경량화)
1. `fetch_data/*/__init__.py`에서 실행 비용 큰 import 제거
2. 필요한 API만 lazy import 또는 직접 모듈 import 사용으로 전환

### Step 4 (정리)
1. `inspect_both_table.py` 삭제 또는 현 스키마 기반 재작성
2. `main.py`, `readme.1md` 정리
3. Docker 스택 역할 정의 후 하나를 표준으로 문서화
4. `pyproject.toml` 미사용 의존성 제거

## 4. 유지 필수 핵심 모듈 (재확인)
- `fetch_data/weather/collect_asos.py`
- `fetch_data/common/impute_missing.py`
- `fetch_data/pv/*.py` (수집/병합/백필/DB 모델)
- `fetch_data/wind/*.py` (수집/적재/DB 모델)
- `prefect_flows/*.py`
- `notify/slack_notifier.py`
- 루트 `Dockerfile`, 루트 `docker-compose.yml`, `prefect.yaml`, `initial_db_ingestion.py`
