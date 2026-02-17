# 리팩토링 체크리스트 (ver2 기반)

## Step 1: 안정성 우선 (P0)

- [x] `fetch_data/pv/daily_pv_automation.py` — 모듈 레벨 `engine = create_engine(DB_URL)` 을 lazy 초기화로 이동
- [x] `fetch_data/pv/daily_pv_automation.py` — 모듈 레벨 `API_KEY` / `DB_URL` 검증(`raise RuntimeError`)을 함수 내부로 이동
- [x] `fetch_data/pv/namdong_collect_pv.py` — 모듈 레벨 `DB_URL = _resolve_db_url(None)` 을 lazy 초기화로 이동
- [x] bare `except:` → `except Exception as e:` 변경
  - [x] `fetch_data/common/impute_missing.py`
  - [x] `fetch_data/pv/daily_pv_automation.py`
  - [x] `fetch_data/pv/nambu_probe_date.py`
  - [x] `fetch_data/pv/nambu_merge_pv_data.py`
  - [x] `fetch_data/pv/nambu_bulk_sync.py` (추가 발견)

## Step 2: 중복 유틸 통합 (P1)

- [ ] `fetch_data/common/db_utils.py` 신설
  - [ ] `running_in_docker()` 통합 (5곳 중복 제거)
  - [ ] `resolve_db_url()` 통합 (5곳 중복 제거)
- [ ] `send_slack_message()` 단일화 → `notify/slack_notifier.py` 만 유지
  - [ ] `fetch_data/pv/namdong_collect_pv.py` — import로 교체
  - [ ] `fetch_data/wind/namdong_wind_collect.py` — import로 교체
  - [ ] `prefect_flows/prefect_pipeline.py` — import로 교체
- [ ] `notify/slack_notifier.py` — 중복 `import os` 제거

## Step 3: `__init__.py` 경량화 (P1)

- [x] `fetch_data/pv/__init__.py` — 무거운 import 제거 (docstring만 유지)
- [x] `fetch_data/weather/__init__.py` — 무거운 import 제거 (docstring만 유지)
- [x] `fetch_data/wind/__init__.py` — 무거운 import 제거 (docstring만 유지)

## Step 4: 레거시/불필요 파일 정리 (P2)

- [ ] `main.py` 삭제 (placeholder)
- [ ] `readme.1md` 삭제 (`README.md`와 중복)
- [ ] `weather_pipeline.egg-info/` 삭제 (빌드 아티팩트)
- [ ] `inspect_both_table.py` — 삭제 또는 현재 스키마 기준 재작성
- [ ] Docker 스택 정리
  - [ ] `docker/` 하위 Docker 설정 — 삭제 또는 역할 문서화
  - [ ] `pv_test/` 하위 Docker 설정 — 삭제 또는 역할 문서화

## Step 5: 의존성 정리 (P3)

- [ ] `pyproject.toml`에서 불필요 의존성 제거
  - [ ] `async>=0.6.2`
  - [ ] `asyncio>=4.0.0`
  - [ ] `docker>=7.1.0`
  - [ ] `git-filter-repo>=2.47.0`

## Step 6: 추가 코드 품질 (P3)

- [ ] `_extract_hour0()` 통합 (`daily_pv_automation.py`, `nambu_backfill.py` 중복)
- [ ] `prev_month_range()` 통합 (`namdong_collect_pv.py`, `namdong_wind_collect.py` 중복)
- [ ] Prefect 알림 task를 `notify/` 모듈 활용으로 통합

## 테스트

- [x] `tests/test_refactoring.py` — 리팩토링 전/후 동작 검증 테스트 파일 존재
- [ ] `tests/test_refactoring.py` — 테스트 실행 및 통과 확인
- [ ] CI에 `pytest` 실행 단계 추가
