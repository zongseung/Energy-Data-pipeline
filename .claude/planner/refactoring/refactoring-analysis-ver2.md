# Energy-Data-Pipeline 리팩토링 분석 (ver2)

기준 시점: 2026-02-17
검토 기준: 현재 저장소 코드 실물(`refactoring-analysis.md` 재검증)
**상태: 전 항목 완료**

## 1. 핵심 발견 사항 (우선순위 순)

### P0. import 시점 실패/부수효과 — 해결 완료
1. `fetch_data/pv/daily_pv_automation.py`
- ~~모듈 import 시 `API_KEY`, `DB_URL` 검증 후 `RuntimeError` 발생 가능~~
- ~~모듈 레벨 `engine = create_engine(DB_URL)` 존재~~
- → lazy 초기화로 이동 완료 (Step 1)

2. `fetch_data/pv/namdong_collect_pv.py`
- ~~모듈 레벨 `DB_URL = _resolve_db_url(None)` 계산~~
- → lazy 초기화로 이동 완료 (Step 1)

### P1. 중복 유틸 함수 — 해결 완료
1. ~~DB URL 해석 로직 중복 (`_running_in_docker`, `_resolve_db_url`)~~
- → `fetch_data/common/db_utils.py`로 통합 (Step 2)

2. ~~Slack 전송 함수 중복~~
- → `notify/slack_notifier.py` 단일화 (Step 2)

### P1. bare `except:` 잔존 — 해결 완료
- ~~`fetch_data/common/impute_missing.py`~~
- ~~`fetch_data/pv/daily_pv_automation.py`~~
- ~~`fetch_data/pv/nambu_probe_date.py`~~
- ~~`fetch_data/pv/nambu_merge_pv_data.py`~~
- ~~`fetch_data/pv/nambu_bulk_sync.py`~~ (추가 발견)
- → 전부 `except Exception as e:` 교체 완료 (Step 1)

### P1. `__init__.py`의 무거운 import — 해결 완료
- ~~`fetch_data/pv/__init__.py`~~
- ~~`fetch_data/weather/__init__.py`~~
- ~~`fetch_data/wind/__init__.py`~~
- → 무거운 import 제거, docstring만 유지 (Step 3)

### P2. 레거시/혼동 유발 파일 — 해결 완료
- ~~`inspect_both_table.py`~~ → 삭제 (Step 4)
- ~~`main.py`~~ → 삭제 (Step 4)
- ~~`readme.1md`~~ → 삭제 (Step 4)

### P2. Docker 구성 다중 유지 — 해결 완료
- → 역할 문서화 (`docker-stack-roles.md`) (Step 4)

### P3. 의존성 정리 — 해결 완료
- ~~`async>=0.6.2`~~, ~~`asyncio>=4.0.0`~~, ~~`docker>=7.1.0`~~, ~~`git-filter-repo>=2.47.0`~~
- → `pyproject.toml`에서 제거 완료 (Step 5)

## 2. 기존 v1 대비 상태 변경

1. 이미 반영/부분 반영된 항목
- `.gitignore`에 `*.egg-info/` 이미 포함
- `weather_pipeline.egg-info/` 삭제 완료

2. v1 내용 중 수정이 필요했던 항목 → 반영 완료
- `prev_month_range` 중복 → `fetch_data/common/date_utils.py`로 통합
- bare `except` 추가 파일(`nambu_bulk_sync.py`) 발견 → 수정 완료

## 3. 실행 순서 및 완료 현황

| Step | 내용 | 상태 |
|------|------|------|
| Step 1 | 안정성 우선 (lazy init, bare except) | 완료 |
| Step 2 | 중복 제거 (db_utils, slack_notifier) | 완료 |
| Step 3 | 패키지 경량화 (__init__.py) | 완료 |
| Step 4 | 레거시 파일 정리, Docker 문서화 | 완료 |
| Step 5 | 의존성 정리 (pyproject.toml) | 완료 |
| Step 6 | 추가 코드 품질 (date_utils, notify_tasks) | 완료 |

## 4. 유지 필수 핵심 모듈 (재확인)
- `fetch_data/weather/collect_asos.py`
- `fetch_data/common/impute_missing.py`, `db_utils.py`, `date_utils.py`
- `fetch_data/pv/*.py` (수집/병합/백필/DB 모델)
- `fetch_data/wind/*.py` (수집/적재/DB 모델)
- `prefect_flows/*.py` (파이프라인, 알림 task, 배포)
- `notify/slack_notifier.py`
- 루트 `Dockerfile`, 루트 `docker-compose.yml`, `prefect.yaml`, `initial_db_ingestion.py`
