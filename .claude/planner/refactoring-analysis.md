# Energy-Data-Pipeline 리팩토링 분석

## 1. 삭제 대상 (Dead Code / 불필요한 파일)

### 1-1. `main.py` (루트)
- `print("Hello from weather-pipeline!")` 한 줄만 있음
- 아무 곳에서도 사용하지 않는 placeholder 파일
- **삭제 권장**

### 1-2. `readme.1md` (루트)
- `README.md`와 중복된 파일 (이름도 비정상)
- **삭제 권장**

### 1-3. `weather_pipeline.egg-info/` (루트)
- setuptools 빌드 아티팩트
- `.gitignore`에 추가 후 삭제 권장
- **삭제 권장**

### 1-4. `inspect_both_table.py` (루트)
- `plant_info`, `pv_generation` 등 **구버전 스키마** 참조
- 현재 DB 모델(`PVNambu`, `PVNamdong` 등)과 불일치
- 실행하면 테이블을 못 찾아 에러 발생
- **삭제 또는 현재 스키마에 맞게 재작성 필요**

### 1-5. `pv_test/` 디렉토리
- `pv_test/Dockerfile`, `pv_test/docker-compose.yml`
- 초기 테스트/프로토타입용 Docker 설정
- 루트의 `Dockerfile` + `docker-compose.yml`과 역할 중복
- **삭제 권장** (루트 Docker 설정으로 통일)

### 1-6. `docker/` 디렉토리
- `docker/Dockerfile`, `docker/docker-compose.yml`
- 루트 Docker 설정과 역할 중복 (3중 Docker 설정 상태)
- **삭제 권장** (루트 Docker 설정으로 통일)

---

## 2. 중복 코드 (DRY 위반)

### 2-1. `_resolve_db_url()` — 5회 중복
| 파일 | 위치 |
|------|------|
| `fetch_data/pv/nambu_backfill.py` | 함수 내 정의 |
| `fetch_data/pv/namdong_collect_pv.py` | 함수 내 정의 |
| `fetch_data/wind/namdong_wind_collect.py` | 함수 내 정의 |
| `fetch_data/wind/seobu_wind_load.py` | 함수 내 정의 |
| `fetch_data/wind/hangyoung_wind_load.py` | 함수 내 정의 |

**리팩토링 방안**: `fetch_data/common/db_utils.py`에 한 번만 정의하고 모든 모듈에서 import

```python
# fetch_data/common/db_utils.py
import os
from dotenv import load_dotenv

def resolve_db_url() -> str:
    load_dotenv()
    if _running_in_docker():
        host = os.getenv("POSTGRES_HOST", "postgres")
    else:
        host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    user = os.getenv("POSTGRES_USER", "energy")
    pw = os.getenv("POSTGRES_PASSWORD", "energy1234")
    db = os.getenv("POSTGRES_DB", "energy_db")
    return f"postgresql://{user}:{pw}@{host}:{port}/{db}"
```

### 2-2. `_running_in_docker()` — 4회 중복
| 파일 | 위치 |
|------|------|
| `fetch_data/pv/nambu_backfill.py` | 함수 내 정의 |
| `fetch_data/pv/namdong_collect_pv.py` | 함수 내 정의 |
| `fetch_data/wind/namdong_wind_collect.py` | 함수 내 정의 |
| `fetch_data/wind/seobu_wind_load.py` | 함수 내 정의 |

**리팩토링 방안**: `fetch_data/common/db_utils.py`에 통합

```python
def running_in_docker() -> bool:
    return os.path.exists("/.dockerenv")
```

### 2-3. `send_slack_message()` — 4회 중복
| 파일 | 위치 |
|------|------|
| `notify/slack_notifier.py` | **정식 모듈** (여기가 canonical) |
| `fetch_data/pv/namdong_collect_pv.py` | 자체 구현 |
| `fetch_data/wind/namdong_wind_collect.py` | 자체 구현 |
| `prefect_flows/prefect_pipeline.py` | 자체 구현 + Block Kit 버전 |

**리팩토링 방안**: `notify/slack_notifier.py`의 구현만 유지하고 나머지는 import로 교체

```python
from notify.slack_notifier import send_slack_message
```

### 2-4. `_extract_hour0()` — 2회 중복
| 파일 | 위치 |
|------|------|
| `fetch_data/pv/nambu_backfill.py` | 함수 내 정의 |
| `fetch_data/pv/daily_pv_automation.py` | 함수 내 정의 |

**리팩토링 방안**: `fetch_data/common/date_utils.py`에 통합

### 2-5. `prev_month_range()` — 2회 중복
| 파일 | 위치 |
|------|------|
| `fetch_data/pv/nambu_backfill.py` | 함수 내 정의 |
| `fetch_data/pv/daily_pv_automation.py` | 함수 내 정의 |

**리팩토링 방안**: `fetch_data/common/date_utils.py`에 통합

---

## 3. 불필요한 의존성 (`pyproject.toml`)

| 패키지 | 이유 |
|--------|------|
| `async>=0.6.2` | 표준 라이브러리 `asyncio` wrapper, 실제 사용 없음 |
| `asyncio>=4.0.0` | Python 표준 라이브러리, pip 설치 불필요 |
| `docker>=7.1.0` | 코드에서 Docker SDK 사용 없음 (docker CLI만 사용) |
| `git-filter-repo>=2.47.0` | 개발 도구, 런타임 의존성 아님 |

**조치**: 위 4개 패키지를 `dependencies`에서 제거

---

## 4. 구조적 문제

### 4-1. 무거운 `__init__.py` import
- **`fetch_data/pv/__init__.py`**: `namdong_collect_pv`, `namdong_merge_pv_data`에서 다수 함수 import → 패키지 import만으로 DB 연결 시도 가능
- **`fetch_data/wind/__init__.py`**: `database.py`에서 import → `dotenv` 로딩 + DB URL resolve가 import 시점에 발생
- **`fetch_data/weather/__init__.py`**: `collect_asos`에서 import → 부수효과 발생

**리팩토링 방안**: `__init__.py`에서 무거운 import 제거, lazy import 또는 빈 `__init__.py` 사용

### 4-2. 모듈 레벨 엔진 생성
- **`fetch_data/pv/daily_pv_automation.py`**: 파일 상단에서 `create_engine()` 호출
- 환경변수 없으면 import만으로 `RuntimeError` 발생
- CI smoke test에서 문제 가능

**리팩토링 방안**: engine 생성을 함수 내부로 이동 (lazy initialization)

```python
# Before (문제)
ENGINE = create_engine(resolve_db_url())

# After (개선)
_engine = None
def get_engine():
    global _engine
    if _engine is None:
        _engine = create_engine(resolve_db_url())
    return _engine
```

### 4-3. Docker 설정 3중 중복
| 위치 | 용도 |
|------|------|
| `./Dockerfile` + `./docker-compose.yml` | 루트 (현재 사용 중) |
| `./docker/Dockerfile` + `./docker/docker-compose.yml` | 서브디렉토리 (구버전) |
| `./pv_test/Dockerfile` + `./pv_test/docker-compose.yml` | 테스트용 (구버전) |

**리팩토링 방안**: 루트만 유지, `docker/`와 `pv_test/` 삭제

---

## 5. 코드 품질 이슈

### 5-1. Bare `except:` 절
| 파일 | 위치 |
|------|------|
| `fetch_data/pv/nambu_bulk_sync.py` | 여러 곳 |
| `fetch_data/pv/nambu_merge_pv_data.py` | 데이터 처리 부분 |

**조치**: `except Exception as e:`로 변경하여 `KeyboardInterrupt`, `SystemExit` 등을 잡지 않도록 수정

### 5-2. 중복 import
- **`notify/slack_notifier.py`**: `import os`가 2번 작성됨
- **조치**: 중복 제거

### 5-3. Prefect 알림 task 중복
- `prefect_flows/prefect_pipeline.py`에 `notify_slack_success`, `notify_slack_failure`, `notify_slack_rich` 정의
- `notify/slack_notifier.py`에도 동일 기능 존재
- **조치**: `notify/` 모듈의 함수를 Prefect task로 wrapping하여 사용

---

## 6. 유지 필수 코드 (핵심 모듈)

### 6-1. 데이터 수집 (`fetch_data/`)
| 파일 | 역할 | 상태 |
|------|------|------|
| `fetch_data/weather/collect_asos.py` | ASOS 기상 데이터 수집 | 정상 |
| `fetch_data/common/impute_missing.py` | 결측치 보간 (scipy) | 정상 |
| `fetch_data/pv/daily_pv_automation.py` | 남부발전 일일 자동 수집 | 엔진 생성 이슈 수정 필요 |
| `fetch_data/pv/namdong_collect_pv.py` | 남동발전 PV 수집 | 중복 함수 정리 필요 |
| `fetch_data/pv/nambu_backfill.py` | 남부발전 과거 데이터 백필 | 중복 함수 정리 필요 |
| `fetch_data/pv/nambu_bulk_sync.py` | 남부발전 대량 동기화 | bare except 수정 필요 |
| `fetch_data/pv/nambu_merge_pv_data.py` | 남부 데이터 전처리/병합 | bare except 수정 필요 |
| `fetch_data/pv/nambu_probe_date.py` | API 날짜 탐색 유틸 | 정상 |
| `fetch_data/wind/namdong_wind_collect.py` | 남동발전 풍력 수집 | 중복 함수 정리 필요 |
| `fetch_data/wind/seobu_wind_load.py` | 서부발전 풍력 적재 | 중복 함수 정리 필요 |
| `fetch_data/wind/hangyoung_wind_load.py` | 한경풍력 적재 | 중복 함수 정리 필요 |
| `fetch_data/pv/database.py` | PV SQLAlchemy 모델 | 정상 |
| `fetch_data/wind/database.py` | Wind SQLAlchemy 모델 | 정상 |

### 6-2. 오케스트레이션 (`prefect_flows/`)
| 파일 | 역할 | 상태 |
|------|------|------|
| `prefect_flows/prefect_pipeline.py` | 기상 ETL 플로우 | Slack 중복 정리 필요 |
| `prefect_flows/deploy.py` | Prefect 배포 설정 | 정상 |
| `prefect_flows/nambu_pv_flow.py` | 남부 PV 플로우 | 정상 |
| `prefect_flows/namdong_wind_flow.py` | 남동 풍력 플로우 | 정상 |
| `prefect_flows/merge_to_all.py` | CSV 통합 유틸 | 정상 |

### 6-3. 알림 (`notify/`)
| 파일 | 역할 | 상태 |
|------|------|------|
| `notify/slack_notifier.py` | Slack 알림 (canonical) | 중복 import 수정 필요 |

### 6-4. 인프라/설정
| 파일 | 역할 | 상태 |
|------|------|------|
| `Dockerfile` (루트) | Docker 이미지 빌드 | 유지 |
| `docker-compose.yml` (루트) | 서비스 구성 | 유지 |
| `pyproject.toml` | 의존성 관리 | 불필요 deps 제거 필요 |
| `.github/workflows/ci.yml` | CI 파이프라인 | 정상 (수정 완료) |
| `prefect.yaml` | Prefect 설정 | 유지 |
| `initial_db_ingestion.py` | DB 초기 적재 | 유지 |

---

## 7. 리팩토링 우선순위

### P0 (즉시)
1. 삭제: `main.py`, `readme.1md`, `weather_pipeline.egg-info/`
2. `pyproject.toml`에서 불필요 의존성 4개 제거
3. `notify/slack_notifier.py` 중복 `import os` 제거

### P1 (높음)
4. `fetch_data/common/db_utils.py` 생성 → `_resolve_db_url()`, `_running_in_docker()` 통합
5. `send_slack_message()` 중복 제거 → `notify/slack_notifier.py`에서 import
6. `daily_pv_automation.py` 모듈 레벨 엔진 생성 → lazy initialization

### P2 (중간)
7. `fetch_data/common/date_utils.py` 생성 → `_extract_hour0()`, `prev_month_range()` 통합
8. `__init__.py` 무거운 import 정리 (lazy import로 전환)
9. bare `except:` → `except Exception as e:`로 변경
10. Docker 설정 통일 (`docker/`, `pv_test/` 삭제)

### P3 (낮음)
11. `inspect_both_table.py` 현재 스키마로 재작성 또는 삭제
12. Prefect 알림 task를 `notify/` 모듈 활용으로 통합
13. `.gitignore`에 `*.egg-info/` 추가
