# Energy-Data-Pipeline 리팩토링 분석 (v1)

> **NOTE**: 이 문서는 초기 분석본입니다. 재검증된 최신 분석은 `refactoring-analysis-ver2.md`를 참고하세요.
> 전체 진행 현황은 `refactoring-checklist.md`를 참고하세요.
> **전 항목 완료 (2026-02-17)**

## 1. 삭제 대상 (Dead Code / 불필요한 파일) — 완료

- ~~`main.py`~~ → 삭제 완료
- ~~`readme.1md`~~ → 삭제 완료
- ~~`weather_pipeline.egg-info/`~~ → 삭제 완료
- ~~`inspect_both_table.py`~~ → 삭제 완료
- `pv_test/`, `docker/` → 역할 문서화 (`docker-stack-roles.md`)

## 2. 중복 코드 (DRY 위반) — 완료

- ~~`_resolve_db_url()` 5회 중복~~ → `fetch_data/common/db_utils.py`로 통합
- ~~`_running_in_docker()` 5회 중복~~ → `fetch_data/common/db_utils.py`로 통합
- ~~`send_slack_message()` 4회 중복~~ → `notify/slack_notifier.py` 단일화
- ~~`_extract_hour0()` 2회 중복~~ → `fetch_data/common/date_utils.py`로 통합
- ~~`prev_month_range()` 2회 중복~~ → `fetch_data/common/date_utils.py`로 통합

## 3. 불필요한 의존성 — 완료

- ~~`async>=0.6.2`~~ → 제거
- ~~`asyncio>=4.0.0`~~ → 제거
- ~~`docker>=7.1.0`~~ → 제거
- ~~`git-filter-repo>=2.47.0`~~ → 제거

## 4. 구조적 문제 — 완료

- ~~무거운 `__init__.py` import~~ → docstring만 유지로 경량화
- ~~모듈 레벨 엔진 생성~~ → lazy initialization으로 이동
- Docker 설정 3중 중복 → 역할 문서화로 정리

## 5. 코드 품질 이슈 — 완료

- ~~bare `except:`~~ → `except Exception as e:` 교체 (5개 파일)
- ~~중복 `import os`~~ → `notify/slack_notifier.py` 정리
- ~~Prefect 알림 task 중복~~ → `prefect_flows/notify_tasks.py` 통합

## 6. 유지 필수 코드 (핵심 모듈)

### 데이터 수집 (`fetch_data/`)
| 파일 | 역할 | 상태 |
|------|------|------|
| `fetch_data/weather/collect_asos.py` | ASOS 기상 데이터 수집 | 정상 |
| `fetch_data/common/impute_missing.py` | 결측치 보간 (scipy) | 정상 |
| `fetch_data/common/db_utils.py` | DB URL 해석 (통합) | **신설** |
| `fetch_data/common/date_utils.py` | 날짜/시간 유틸 (통합) | **신설** |
| `fetch_data/pv/daily_pv_automation.py` | 남부발전 일일 자동 수집 | 정상 (lazy init 적용) |
| `fetch_data/pv/namdong_collect_pv.py` | 남동발전 PV 수집 | 정상 (중복 제거) |
| `fetch_data/pv/nambu_backfill.py` | 남부발전 과거 데이터 백필 | 정상 (중복 제거) |
| `fetch_data/pv/nambu_bulk_sync.py` | 남부발전 대량 동기화 | 정상 (bare except 수정) |
| `fetch_data/pv/nambu_merge_pv_data.py` | 남부 데이터 전처리/병합 | 정상 (bare except 수정) |
| `fetch_data/pv/nambu_probe_date.py` | API 날짜 탐색 유틸 | 정상 |
| `fetch_data/wind/namdong_wind_collect.py` | 남동발전 풍력 수집 | 정상 (중복 제거) |
| `fetch_data/wind/seobu_wind_load.py` | 서부발전 풍력 적재 | 정상 (중복 제거) |
| `fetch_data/wind/hangyoung_wind_load.py` | 한경풍력 적재 | 정상 (중복 제거) |
| `fetch_data/pv/database.py` | PV SQLAlchemy 모델 | 정상 |
| `fetch_data/wind/database.py` | Wind SQLAlchemy 모델 | 정상 |

### 오케스트레이션 (`prefect_flows/`)
| 파일 | 역할 | 상태 |
|------|------|------|
| `prefect_flows/prefect_pipeline.py` | 기상 ETL 플로우 | 정상 (Slack import 교체) |
| `prefect_flows/notify_tasks.py` | Prefect Slack 알림 task (통합) | **신설** |
| `prefect_flows/deploy.py` | Prefect 배포 설정 | 정상 |
| `prefect_flows/nambu_pv_flow.py` | 남부 PV 플로우 | 정상 |
| `prefect_flows/namdong_wind_flow.py` | 남동 풍력 플로우 | 정상 |
| `prefect_flows/merge_to_all.py` | CSV 통합 유틸 | 정상 |

### 알림 (`notify/`)
| 파일 | 역할 | 상태 |
|------|------|------|
| `notify/slack_notifier.py` | Slack 알림 (canonical) | 정상 (`send_slack_rich_message` 추가) |

### 인프라/설정
| 파일 | 역할 | 상태 |
|------|------|------|
| `Dockerfile` (루트) | Docker 이미지 빌드 | 유지 |
| `docker-compose.yml` (루트) | 서비스 구성 | 유지 |
| `pyproject.toml` | 의존성 관리 | 정상 (불필요 deps 제거 완료) |
| `.github/workflows/ci.yml` | CI 파이프라인 | 정상 (pytest 단계 추가) |
| `prefect.yaml` | Prefect 설정 | 유지 |
| `initial_db_ingestion.py` | DB 초기 적재 | 유지 |
