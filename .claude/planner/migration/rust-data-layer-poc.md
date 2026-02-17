# Data Layer Rust 도입 PoC 기획서 (v3 — 수집기 Rust 재작성 반영)

기준일: 2026-02-17
최종 수정: 2026-02-17 (Phase 2: Rust 수집기 재작성 방향 추가)
대상 저장소: `Energy-Data-pipeline`
운영 전제: Python 오케스트레이션(Prefect) 유지 + **수집기(Collector)를 Rust + Polars로 재작성**

> **v3 변경 이력**
> - Phase 0 완료 (No-Go → 성능 최적화 관점 Rust 불필요 확인)
> - Phase 1 미진행 (성능 기반 조건부 치환은 보류)
> - **Phase 2 신설**: 수집기 자체를 Rust로 재작성 (성능이 아닌 구조/안정성/배포 관점)
> - melt 사이트 전수 조사: 2곳 → **6곳** 업데이트
> - 리팩토링 반영: `date_utils.py`, `notify_tasks.py`, `db_utils.py` 통합 완료 상태

---

## 0. 배경 및 문제 정의

### 0.1 현재 구조(요약)
- 저장소는 "단일 파이썬 패키지"가 아닌 **모노레포(여러 top-level 모듈/폴더 공존)** 형태입니다.
  - 예: `fetch_data/`, `prefect_flows/`, `notify/`, `docker/` 등
- CI/테스트/배포는 **모노레포 친화적인 방식(uv, PYTHONPATH)**을 사용합니다.

### 0.2 목표
- Prefect 기반 오케스트레이션은 그대로 유지합니다.
- **1단계**: Python 레벨 최적화(벡터화, 캐싱, I/O 병렬화)를 먼저 적용합니다.
- **2단계**: 1단계 이후에도 병목이 남는 구간만 Rust로 점진 치환합니다.
- 결과 동등성(기존 대비 동일 출력)을 보장하고, 운영 fallback(롤백) 경로를 유지합니다.

### 0.3 코드 리뷰 기반 병목 분석 결과

> 아래 분석은 실제 소스코드를 정적 분석한 결과입니다.
> **실측 프로파일링(cProfile/py-spy)은 Phase 0에서 수행 예정입니다.**

| 후보 함수 | 실제 병목 유형 | 현재 구현 특성 | Rust 기대 효과 |
|-----------|--------------|---------------|---------------|
| `transform_wide_to_long` | **없음** (이미 최적) | pd.melt/to_datetime 등 Cython 기반, 입력 ~200행 | 직렬화 오버헤드로 **오히려 느려질 가능성** |
| `impute_missing_values` | **Python 루프** | `.iloc[i]` 반복, 매번 DataFrame 재필터링 | Python 최적화로 10~100x 가능, Rust는 2~5x |
| `_rows_from_api_payload` | **네트워크 I/O** | 변환 자체는 ~24행, API 호출이 전체의 95%+ | Rust 효과 **0.01% 미만** |
| `upsert_wind_namdong` | **DB I/O** | batch 5000건 upsert, 네트워크 RTT 지배적 | Rust 효과 미미 |
| `backfill()` 내 `to_sql` | **DB I/O** | async 컨텍스트 내 pandas 메서드 호출 | Rust 효과 미미 |

**핵심 발견**: 후보 B(Wide→Long)와 후보 C(DB 적재)는 Rust 전환 대상으로 부적합합니다.
후보 A(결측치 보간)만 CPU 병목이 존재하나, Python 최적화로 먼저 해결 가능합니다.

---

## 1. 범위(스코프) 및 비범위(아웃 오브 스코프)

### 1.1 PoC 범위
- **Phase 0 (선행)**: Python 최적화 + 프로파일링으로 실제 병목 확인
- **Phase 1 (조건부)**: Phase 0 이후에도 CPU 병목이 남는 "순수 계산/변환"만 Rust로 치환
- Python은 **스케줄, 환경/설정, I/O 오케스트레이션, 알림**을 담당합니다.
- 연동은 1차로 **pyo3 + maturin 확장 모듈(import 호출)**, 2차로 **Rust CLI 분리**를 선택적으로 적용합니다.

### 1.2 PoC 비범위
- 전체 파이프라인을 Rust로 전면 이관
- DB 스키마/모델링의 대규모 리팩토링
- Prefect 제거 또는 오케스트레이션 계층 교체
- **I/O 바운드 함수의 Rust 전환** (네트워크/DB가 지배적인 구간)

---

## 2. 후보 분석 및 전략 (코드 리뷰 반영)

### 후보 A: 결측치 보간 코어 — Python 최적화 우선, Rust 조건부 진행

- 대상 파일/함수
  - `fetch_data/common/impute_missing.py:138` (`impute_missing_values`)
  - `fetch_data/common/impute_missing.py:30` (`spline_impute`)
  - `fetch_data/common/impute_missing.py:74` (`historical_average_impute`)
  - `fetch_data/common/impute_missing.py:8` (`find_consecutive_missing_groups`)

- 병목 상세 분석
  - `find_consecutive_missing_groups()`: **`.iloc[i]` 반복 루프** — `.values` numpy 배열로 교체 시 5~10x 개선
  - `historical_average_impute()`: **매 결측값마다 전체 DataFrame 재필터링** — station별 캐싱으로 10~100x 개선
  - `spline_impute()`: **scipy.interpolate.interp1d (C 코드)** — 이미 최적화됨, Rust 전환 불필요

- Python 최적화 방안
  ```python
  # AS-IS: 느림 (pandas accessor 오버헤드)
  for i in range(len(is_missing)):
      if is_missing.iloc[i]:  # ~1000x slower than numpy

  # TO-BE: 빠름 (numpy 직접 접근)
  arr = series.isna().values
  for i in range(len(arr)):
      if arr[i]:
  ```
  ```python
  # AS-IS: 매번 재필터링
  for missing_date in missing_dates:
      station_data = df[df[station_col] == station_name]  # 매번 O(n)

  # TO-BE: 한 번 캐싱 + groupby 벡터화
  station_cache = df.groupby(station_col)
  hist_avg = station_cache.apply(lambda g: g.groupby(['month','day','hour'])[col].mean())
  ```

- Rust 전환 판단 기준
  - Python 최적화 후에도 wall time의 50% 이상이 순수 계산에 소요되면 진행
  - `spline_impute()`는 scipy C 코드이므로 Rust 전환 대상에서 **제외**

### ~~후보 B: Wide→Long 변환 코어~~ — 후보 제외

- 대상 파일/함수
  - `fetch_data/wind/namdong_wind_collect.py:97` (`transform_wide_to_long`)
  - `fetch_data/pv/nambu_backfill.py:188` (`_rows_from_api_payload`)

- **제외 사유**
  - `pd.melt()`, `pd.to_datetime()`, `str.extract()` 등 핵심 연산이 이미 **Cython/C 레벨**로 구현됨
  - 입력 데이터 규모가 ~200행(wind) / ~24행(PV)으로 극히 작음
  - 현재 pandas로 **1~3ms**면 완료 → PyO3 직렬화 오버헤드(5~20ms)로 오히려 느려짐
  - 실제 병목은 이 함수 앞단의 **HTTP API 호출(100~500ms)**

### ~~후보 C: DB 적재 경로~~ — 후보 제외

- 대상 파일/함수
  - `fetch_data/wind/namdong_wind_collect.py:187` (`upsert_wind_namdong`)
  - `fetch_data/pv/nambu_backfill.py` — `backfill()` 함수 내 `df.to_sql()` 호출
    - (참고: 독립 함수가 아니라 async `backfill()` 내부의 pandas 메서드 호출)

- **제외 사유**
  - 병목의 95%+가 **네트워크 I/O (API 호출 + DB round-trip)**
  - backfill 100일 기준: API ~30초, DB ~5초, 데이터 변환 ~0.01초
  - Rust로 변환 계층을 바꿔도 전체의 **0.02%** 미만 절감
  - 최적화 방향: API 배치 요청, 비동기 병렬화, DB 벌크 인서트

---

## 3. PoC 권장 순서 (수정)

### Phase 0: Python 최적화 + 프로파일링 (필수, 선행)

1) **프로파일링 실측** — cProfile / py-spy로 각 함수별 실행시간 측정
2) **impute_missing.py 최적화** — `.iloc` → `.values`, DataFrame 캐싱, groupby 벡터화
3) **I/O 계층 최적화** — API 배치/병렬화, DB 벌크 인서트
4) **최적화 전후 비교 리포트** 작성

### Phase 1: Rust PoC (조건부)

> Phase 0 완료 후, 프로파일링 결과에서 순수 CPU 병목이 여전히 지배적인 함수가 있을 때만 진행합니다.

1) `find_consecutive_missing_groups` + 핵심 루프 → Rust 치환 (가장 유력한 후보)
2) 동등성 테스트 + 벤치마크
3) fallback 경로 검증

> PoC에서 가장 중요한 건 "성능"보다 **동등성/운영성/롤백 가능성**입니다.

---

## 4. 모노레포 반영 아키텍처 및 디렉토리 설계

### 4.1 제안 모노레포 레이아웃
현재 Python 구조를 그대로 두고, Rust는 별도 workspace로 분리합니다.

```
Energy-Data-pipeline/
  fetch_data/               # Python 수집기 (유지, fallback)
    common/                 # db_utils, date_utils, impute_missing
    pv/                     # PV 수집기
    wind/                   # 풍력 수집기
  prefect_flows/            # Prefect 오케스트레이션 (유지)
  notify/                   # Slack 알림 (유지)
  docker/                   # Docker 설정
  tests/
    equivalence/            # 동등성 스냅샷 기반 테스트
    benchmark/              # 고정 입력셋 벤치

  rust/                     # Phase 2: Rust 수집기 workspace
    Cargo.toml              # workspace 루트
    crates/
      common/               # config, DB pool, error types
      transforms/           # Polars 변환 (melt/unpivot)
      collector-nambu/      # 남부발전 수집기 CLI
      collector-namdong-pv/ # 남동 PV 수집기 CLI
      collector-namdong-wind/ # 남동 풍력 수집기 CLI
```

> 참고: `configs/` 디렉토리는 현재 존재하지 않음. 환경변수(`.env`)로 설정 관리 중.

### 4.2 Python 측 호출 지점(예시)
- 기존 함수는 유지하되, 옵션 플래그로 Rust 우회 경로를 둡니다.
  - `use_rust=False` 기본 → 안전
  - `use_rust=True`는 스테이징부터 점진 적용

---

## 5. 연동 방식(권장안) 및 인터페이스 계약

### 5.1 1차 연동: pyo3 + maturin (권장)
- Python에서 `import rust_data_layer` 형태로 호출합니다.
- 장점: I/O/오케스트레이션은 Python 그대로, 핫스팟만 Rust로 치환 가능합니다.
- 설계 원칙(인터페이스 최소화)
  - 입력/출력은 가능하면 **Arrow/Parquet/bytes**로 이동 비용을 최소화합니다.
  - 초기 PoC는 단순성을 위해 **CSV/JSONL + 명시적 스키마**도 허용합니다.
- 주의: **소규모 데이터(<1,000행)에서는 직렬화 오버헤드가 계산 이득을 상쇄**할 수 있음

### 5.2 2차(선택): Rust CLI 분리
- 대량 DB 적재(COPY) 같은 "프로세스 경계가 유리한 작업"은 CLI로 분리할 수 있습니다.
- Python은 subprocess로 실행만 하고, 결과/로그/메트릭을 수집합니다.

### 5.3 인터페이스 계약(동등성의 기준)
- "동등성 100%"의 정의를 명확히 합니다.
  - 행 수 동일
  - key 컬럼(`ts`, `id` 등) 동일
  - 수치 컬럼: **허용 오차 정책**(필요 시) 명시
    - 예: float 연산이면 `abs_err <= 1e-9` 또는 ULP 기준
  - 정렬/타입 캐스팅/NA 처리 규칙 동일

---

## 6. 동등성 테스트 전략(Reverse Engineering 성격 반영)

### 6.1 스냅샷 기반 테스트(권장)
- 고정 입력셋(샘플 payload/파일)을 `tests/equivalence/fixtures/`에 저장합니다.
- Python 결과를 "정답 스냅샷"으로 고정합니다.
- Rust 결과와 비교합니다.
  - shape/columns 체크
  - 샘플 레코드 체크
  - 해시 기반 체크(정렬 기준 고정 후 parquet/csv hash)

### 6.2 드리프트(스키마 변화) 대응
- 원천이 자주 바뀌는 경우:
  - Silver(중간 산출물) 단계에서 "새 컬럼/타입 변화" 탐지 로그를 남기고,
  - Gold(확정 산출물) 스키마는 버전업으로 관리합니다.
- PoC 최소안:
  - "입력 스키마 signature"를 기록하고 이전과 다르면 경고(알림)만 발생시킵니다.

---

## 7. CI/CD(모노레포 + 멀티언어) 설계

### 7.1 CI 목표
- Python + Rust 빌드/테스트가 재현 가능하게 돌아야 합니다.
- 브랜치 보호 규칙의 required checks로 연결합니다.

### 7.2 권장 CI Job 분리
1) **python-uv**: uv sync + 스모크 import + pytest (현재 운영 중)
2) **rust**: cargo fmt/clippy/test (Phase 1 진입 시 추가)
3) **equivalence**: 고정 입력셋으로 Python vs Rust 결과 비교 (Phase 1 진입 시 추가)
4) **benchmark**: Phase 0 벤치마크 + Phase 1 비교 벤치

> 현재 레포는 flat-layout이라 Python job은 **uv + PYTHONPATH + `--no-install-project`** 방식이 안전합니다.

### 7.3 브랜치 전략
- `test-rust`: 스테이징 브랜치(실험/검증)
- `main`: 프로덕션 안정 브랜치
- 개발은 `feature/*` → PR → `test-rust` → PR → `main`

---

## 8. 구현 계획 (수정: Phase 0 + Phase 1)

### Phase 0 — Week 1~2: Python 최적화 + 프로파일링

#### Week 1: 프로파일링 + impute 최적화
1) cProfile / py-spy로 전체 파이프라인 프로파일링 실행
2) `find_consecutive_missing_groups()`: `.iloc[i]` → `.values` 변환
3) `historical_average_impute()`: station별 DataFrame 캐싱 + groupby 벡터화
4) 최적화 전후 벤치마크 비교

**산출물**
- `tests/benchmark/profile_baseline.py` (프로파일링 스크립트)
- `tests/benchmark/profile_results_phase0.md` (결과 리포트)
- impute_missing.py 최적화 커밋

#### Week 2: I/O 최적화 + Go/No-Go 판단
1) API 호출 배치/병렬화 검토 (backfill, wind_collect)
2) DB 벌크 인서트 최적화 검토
3) Phase 0 종합 리포트 작성
4) **Go/No-Go 판단**: Rust Phase 1 진입 여부 결정

**산출물**
- I/O 최적화 커밋(해당 시)
- `docs/phase0_report.md`

### Phase 1 — Week 3~4: Rust PoC (조건부)

> **Phase 0 Go/No-Go에서 "Go" 판정 시에만 진행합니다.**
> Go 조건: Python 최적화 후에도 특정 함수의 순수 CPU 연산이 전체 wall time의 30% 이상을 차지

#### Week 3: Rust workspace + 1st 함수 치환
1) `rust/` workspace 스캐폴딩 구성
2) 대상 함수 Rust 구현 (프로파일링 결과 기반 선정)
3) Python fallback 유지(`use_rust=False` 옵션)
4) 동등성 테스트 작성(행 수/컬럼/샘플 값/정렬 규칙)

**산출물**
- `rust/crates/transforms` (라이브러리)
- `rust/crates/data_layer` (pyo3 모듈)
- `tests/equivalence/*`

#### Week 4: 벤치마크 + 리포트
1) Python 최적화 버전 vs Rust 버전 벤치마크
2) 성능/메모리/오류 로그 비교 리포트
3) 최종 Go/No-Go: 프로덕션 적용 여부 결정

**산출물**
- `tests/benchmark/rust_vs_python.py`
- `docs/poc_report_final.md`

---

## 9. 성공 기준(Go/No-Go)

### Phase 0 → Phase 1 진입 기준
- 프로파일링 실측 데이터가 존재할 것
- Python 최적화 후에도 순수 CPU 병목이 wall time의 **30% 이상**인 함수가 있을 것
- 해당 함수의 입력 데이터 규모가 PyO3 직렬화 오버헤드를 상쇄할 수준일 것 (최소 **10,000행 이상**)

### Phase 1 성공 기준

#### 정확성
- Python 대비 결과 동등성 100%
  - 비교 기준 및 허용 오차 정책 문서화

#### 성능
- **Python 최적화 버전** 대비 wall time 50% 이상 단축(목표)
  - (원본 Python 대비가 아님 — Python 최적화만으로 얻는 이득을 제외한 순수 Rust 이득)
- 메모리 peak 감소(가능하면 수치 제시)

#### 안정성/운영성
- fallback 경로(`use_rust=False`)로 즉시 롤백 가능
- CI에서 재현 가능(로컬/CI 동일 결과)

---

## 10. 리스크 및 대응(모노레포/멀티언어 반영)

1) **Rust 전환이 오히려 느려지는 경우** (신규)
- 원인: 소규모 데이터에서 PyO3 직렬화 오버헤드 > 계산 이득
- 대응: Phase 0 프로파일링에서 데이터 규모를 확인하고, 10,000행 미만이면 Rust 전환 보류
- 지표: PyO3 round-trip 오버헤드 측정 (빈 데이터 왕복 시간)

2) **Python 최적화만으로 충분한 경우** (신규)
- 원인: `.iloc` → `.values`, 캐싱, 벡터화로 10~100x 개선 달성
- 대응: Phase 0 리포트에서 "No-Go" 판정 → Rust PoC 불필요 → 리소스 절약
- 이것은 리스크가 아니라 **최선의 결과**

3) 멀티언어 CI 복잡도 증가
- 대응: job 분리 + 캐시(uv cache, cargo cache) + required checks 최소 세트부터

4) Python↔Rust 디버깅 경계
- 대응: 입력/출력 "스냅샷" 및 "정렬 규칙" 표준화, 실패 시 결과 diff 자동 저장

5) 팀 숙련도 편차
- 대응: Rust 범위를 코어 함수로 제한, API surface 최소화, 코드 템플릿/가이드 제공

6) 리버스 엔지니어링 특성(스키마 변동)
- 대응: Bronze 원문 보존, Silver 느슨한 스키마 + drift 로그, Gold는 버전업

---

## 11. 즉시 실행 액션(체크리스트)

### Phase 0 (즉시 시작)
1) cProfile/py-spy 프로파일링 환경 구성
2) `impute_missing.py` 벤치마크 baseline 측정
3) `.iloc` → `.values` 최적화 적용
4) `historical_average_impute` 캐싱/벡터화 적용
5) 최적화 전후 비교 리포트 작성
6) Go/No-Go 판단

### Phase 1 (Go 판정 시)
7) Rust 대상 함수 확정 (프로파일링 기반)
8) 브랜치 생성: `test-rust`에서 feature 브랜치로 시작
9) `rust/` workspace 생성 + `maturin` 최소 모듈 생성
10) 동등성 테스트(스냅샷) 먼저 작성
11) CI job 추가(rust job)
12) 스테이징 환경(`test-rust`)에서만 `use_rust=True` 활성화

---

## 12. 추가로 넣으면 "진짜 운영 문서"가 되는 항목(권장 보강)

- **데이터 경로 분리 정책**: test/prod 출력 경로, DB 스키마, 버킷 분리
- **관측성(Observability)**: 처리량, 지연, 실패율, drift 이벤트 수를 로그/메트릭으로 관리
- **릴리즈 규칙**: Rust 모듈 버전(semver), Python과 호환 범위 명시
- **보안**: API 키 마스킹, fixture에 민감정보 미포함, CI secret 최소화
- **롤백 절차**: env var로 즉시 Rust 비활성화 + 재처리 방법

---

## 부록 A. 현행 CI 구성 (참고)

> 현재 운영 중인 CI 설정입니다. Phase 1 진입 시 rust job을 추가합니다.

```yaml
name: ci

on:
  push:
    branches: [ "test-rust", "main" ]
  pull_request:
    branches: [ "test-rust", "main" ]

jobs:
  python-uv:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: "3.11"

      - name: Install uv
        run: |
          python -m pip install --upgrade pip
          pip install uv

      - name: Sync deps
        run: |
          if [ -f uv.lock ]; then
            uv sync --frozen --no-install-project
          else
            uv sync --no-install-project
          fi

      - name: Smoke import
        env:
          PYTHONPATH: .
        run: |
          uv run python -c "import fetch_data; print('ok: fetch_data')"
          uv run python -c "import prefect_flows; print('ok: prefect_flows')"
          uv run python -c "import notify; print('ok: notify')"

      - name: Run tests
        env:
          PYTHONPATH: .
        run: uv run pytest tests/ -v
```

---

## 부록 B. 병목 유형별 최적화 전략 요약

| 병목 유형 | 최적화 방향 | 예시 | Rust 적합도 |
|----------|-----------|------|------------|
| **Python 루프 오버헤드** | numpy 배열 직접 접근, 벡터화 | `.iloc[i]` → `.values` | Python으로 해결 가능 (중) |
| **반복 DataFrame 필터링** | 캐싱, groupby 사전 연산 | station별 1회 필터링 | Python으로 해결 가능 (저) |
| **대규모 수치 연산** | numpy/scipy 활용, Rust fallback | 10만행+ 행렬 연산 | Rust 적합 (고) |
| **네트워크 I/O** | 비동기 병렬화, 배치 요청 | API 호출, DB round-trip | Rust 무관 (해당없음) |
| **직렬화/역직렬화** | Arrow IPC, zero-copy | Python↔Rust 데이터 전달 | 데이터 규모에 의존 |

---

# Phase 2: Rust 수집기(Collector) 재작성

> Phase 0의 No-Go 판정은 "성능 최적화를 위한 함수 단위 치환"에 대한 것입니다.
> Phase 2는 **수집기 전체를 Rust로 재작성**하는 별도 트랙으로, 목적이 다릅니다.

## P2-0. 목적 및 동기

### 왜 수집기를 Rust로 재작성하는가?

| 관점 | Python 현재 | Rust 기대 |
|------|-----------|----------|
| **타입 안정성** | 런타임 타입 에러 (API 스키마 변경 시) | 컴파일 타임에 스키마 불일치 탐지 |
| **배포** | Python 환경 + 의존성 관리 필요 | **단일 바이너리** (cross-compile 가능) |
| **리소스** | pandas + aiohttp + sqlalchemy 메모리 | 최소 메모리 풋프린트 |
| **동시성** | GIL 제약 (asyncio로 I/O만 병렬) | tokio 기반 완전 비동기 |
| **데이터 처리** | pandas (Cython) | **Polars (Rust native)** — 동일 에코시스템 |
| **에러 처리** | `try/except` 런타임 | `Result<T, E>` 컴파일 타임 강제 |

### Phase 2 범위

- **포함**: 4개 주요 수집기의 Rust 재작성
  - API 호출 (reqwest/tokio)
  - 데이터 변환 (Polars)
  - DB 적재 (sqlx/tokio-postgres)
- **유지**: Prefect 오케스트레이션 (Python) — subprocess로 Rust 바이너리 호출
- **제외**: impute_missing (Python 유지), Prefect flow 로직, Slack 알림

---

## P2-1. 현행 수집기 인벤토리

### 주요 수집기 (Rust 재작성 대상)

| # | 수집기 | 파일 | 라인 | 데이터 소스 | 대상 테이블 | DB 방식 |
|---|--------|------|------|-----------|-----------|---------|
| 1 | 남부발전 일일 자동수집 | `fetch_data/pv/daily_pv_automation.py` | 245 | 공공API (XML) | `nambu_generation` | DELETE+INSERT |
| 2 | 남부발전 백필 | `fetch_data/pv/nambu_backfill.py` | 383 | 공공API (XML) | `nambu_generation` | DELETE+INSERT |
| 3 | 남동발전 PV 수집 | `fetch_data/pv/namdong_collect_pv.py` | 502 | 웹 CSV 다운로드 | `namdong_generation` | DELETE+INSERT |
| 4 | 남동발전 풍력 수집 | `fetch_data/wind/namdong_wind_collect.py` | 314 | 공공API (JSON) | `wind_namdong` | ON CONFLICT UPSERT |

### 보조 수집기 (2차 대상 또는 유지)

| # | 수집기 | 파일 | 비고 |
|---|--------|------|------|
| 5 | 남부발전 일괄수집 | `fetch_data/pv/nambu_bulk_sync.py` | CSV 출력만 (DB 미사용) |
| 6 | 남동 CSV 병합 | `fetch_data/pv/namdong_merge_pv_data.py` | 오프라인 병합 유틸 |
| 7 | 한경풍력 로더 | `fetch_data/wind/hangyoung_wind_load.py` | 1회성 CSV 로드 |
| 8 | 서부풍력 로더 | `fetch_data/wind/seobu_wind_load.py` | 1회성 CSV 로드 |

### 공통 모듈 (리팩토링 완료 상태)

| 모듈 | 파일 | Rust 대응 |
|------|------|----------|
| DB 유틸 | `fetch_data/common/db_utils.py` | Rust config 모듈로 통합 |
| 날짜 유틸 | `fetch_data/common/date_utils.py` | Rust chrono 유틸로 통합 |
| 결측치 보간 | `fetch_data/common/impute_missing.py` | **Python 유지** (scipy 의존) |
| Slack 알림 | `notify/slack_notifier.py` | Python 유지 (Prefect task) |
| Prefect 알림 | `prefect_flows/notify_tasks.py` | Python 유지 |

---

## P2-2. Melt/Unpivot 전수 조사 (6곳)

> Polars `unpivot()` 으로 전환해야 하는 모든 사이트입니다.
> Polars 레퍼런스: `.claude/planner/polars/10-Reshaping.md`

### Site 1: daily_pv_automation.py:172 (남부 PV 자동수집)

```python
# 현행 (pandas)
v_vars = [c for c in df_raw.columns if c.startswith("qhorgen")]
df_long = df_raw.melt(
    id_vars=["ymd", "hogi", "gencd", "ipptnm", "qvodgen", "qvodavg", "qvodmax", "qvodmin"],
    value_vars=v_vars, var_name='h_str', value_name='generation'
)
df_long["hour0"] = df_long["h_str"].apply(_extract_hour0).astype(int)
df_long["datetime"] = pd.to_datetime(df_long["ymd"]) + pd.to_timedelta(df_long["hour0"], unit="h")
df_long["generation"] = pd.to_numeric(df_long["generation"], errors="coerce").fillna(0)
```

```rust
// Polars 대응 (Rust)
let df_long = df_raw
    .unpivot(v_vars, id_vars)
    .unwrap()
    .lazy()
    .with_columns([
        col("variable").str().extract(r"(\d+)", 1)
            .cast(DataType::Int32).unwrap() - lit(1)  // 0-based hour
            .alias("hour0"),
        col("generation").cast(DataType::Float64).alias("generation"),
    ])
    .with_columns([
        (col("ymd").str().to_date(StrptimeOptions::default())
         + duration(hours=col("hour0")))
        .alias("datetime"),
    ])
    .collect()?;
```

### Site 2: nambu_backfill.py:191 (남부 PV 백필)

```python
# 현행 — Site 1과 동일 패턴
df_long = df_raw.melt(
    id_vars=["ymd", "hogi", "gencd", "ipptnm", "qvodgen", "qvodavg", "qvodmax", "qvodmin"],
    value_vars=v_vars, var_name="h_str", value_name="generation"
)
```

> **Site 1과 동일 변환 로직** → Rust에서 공통 함수로 추출 가능

### Site 3: namdong_collect_pv.py:313 (남동 PV)

```python
# 현행
hcols = hour_columns(df)  # "1시 발전량(KWh)", ..., "24시 발전량(KWh)"
df_long = pd.melt(df, id_vars=["일자", "발전소명"], value_vars=hcols,
                  var_name="시간", value_name="발전량")
df_long["hour"] = df_long["시간"].apply(extract_hour).astype("int64")  # 한글 파싱
df_long["datetime"] = df_long["date"] + pd.to_timedelta(df_long["hour"] - 1, unit="h")
```

> **한글 컬럼명** 파싱 필요 → Polars `str.extract(r"(\d+)시")` 패턴

### Site 4: namdong_wind_collect.py:114 (남동 풍력)

```python
# 현행
df_long = pd.melt(df_wide, id_vars=id_cols, value_vars=value_cols,
                  var_name="hour_col", value_name="generation")
df_long["hour_num"] = df_long["hour_col"].str.extract(r"(\d+)").astype(int)
# 특수 처리: hour=24 → 다음날 00:00
mask_24 = df_long["hour_num"] == 24
df_long.loc[mask_24, "timestamp"] += pd.Timedelta(days=1)
```

> **24시 → 다음날 00:00** 특수 로직 주의

### Site 5: namdong_merge_pv_data.py:117 (남동 CSV 병합)

```python
# 현행 — Site 3과 동일 패턴
df_long = pd.melt(df, id_vars=["일자", "발전소명"], value_vars=hcols,
                  var_name="시간", value_name="발전량")
```

### Site 6: nambu_merge_pv_data.py:74 (남부 CSV 전처리)

```python
# 현행 — Site 1과 유사 패턴
df_long = merged_df.melt(
    id_vars=id_vars, value_vars=value_vars,
    var_name='hour_str', value_name='generation'
)
df_long['hour'] = df_long['hour_str'].str.extract(r'(\d+)').astype(int)
df_long['timestamp'] = df_long['ymd'] + pd.to_timedelta(df_long['hour'], unit='h')
```

### Melt 패턴 요약

| 패턴 | 사이트 | id_vars | hour 추출 | 특수 처리 |
|------|--------|---------|----------|----------|
| **남부 API** | 1, 2, 6 | ymd, hogi, gencd, ipptnm + 일별통계 | `qhorgen(\d+)` → 0-based | - |
| **남동 CSV** | 3, 5 | 일자, 발전소명 | `(\d+)시` (한글) | hour-1 offset |
| **남동 풍력** | 4 | dgenYmd, ippt, hogi, ipptNam | `qhorGen(\d+)` | hour=24 → 다음날 |

---

## P2-3. 데이터베이스 인터페이스

### 테이블 & 스키마

| 테이블 | 컬럼 | PK/Unique | DB 방식 |
|--------|------|-----------|---------|
| `nambu_generation` | datetime, gencd, plant_name, hogi, generation, daily_total/avg/max/min | (datetime, gencd, hogi) | DELETE range + INSERT |
| `namdong_generation` | datetime, plant_name, hour, generation | (datetime, plant_name) | DELETE range + INSERT |
| `wind_namdong` | timestamp, plant_name, generation | `UNIQUE(timestamp, plant_name)` | ON CONFLICT UPSERT |

### Rust DB 전략

```rust
// sqlx 기반 비동기 PostgreSQL
use sqlx::PgPool;

// DELETE + INSERT 패턴 (nambu, namdong PV)
async fn upsert_delete_insert(pool: &PgPool, table: &str, start: NaiveDate, end: NaiveDate, rows: &[Row]) -> Result<()> {
    let mut tx = pool.begin().await?;
    sqlx::query(&format!("DELETE FROM {} WHERE datetime >= $1 AND datetime < $2", table))
        .bind(start).bind(end)
        .execute(&mut *tx).await?;
    // COPY 또는 batch INSERT
    tx.commit().await?;
    Ok(())
}

// ON CONFLICT 패턴 (wind)
async fn upsert_on_conflict(pool: &PgPool, rows: &[WindRow]) -> Result<()> {
    sqlx::query(r#"
        INSERT INTO wind_namdong (timestamp, plant_name, generation)
        VALUES ($1, $2, $3)
        ON CONFLICT (timestamp, plant_name)
        DO UPDATE SET generation = EXCLUDED.generation
    "#)
    .bind(&row.timestamp).bind(&row.plant_name).bind(&row.generation)
    .execute(pool).await?;
    Ok(())
}
```

### 환경변수

| 변수 | 용도 | 현행 |
|------|------|------|
| `DB_URL` | PostgreSQL 접속 URL | `postgresql+psycopg2://...` → Rust: `postgres://...` (드라이버 접두사 변경) |
| `NAMBU_API_KEY` | 남부발전 공공API | 그대로 사용 |
| `NAMDONG_WIND_KEY` | 남동풍력 공공API | 그대로 사용 |
| `NAMDONG_ORG_NO` 등 | 남동PV 웹 수집 파라미터 | 그대로 사용 |

---

## P2-4. Rust 수집기 아키텍처

### 디렉토리 레이아웃

```
Energy-Data-pipeline/
  fetch_data/            # Python 수집기 (유지, fallback)
  prefect_flows/         # Python 오케스트레이션 (유지)
  notify/                # Python 알림 (유지)
  tests/                 # Python 테스트 (유지)

  rust/                  # Rust 수집기 workspace
    Cargo.toml           # workspace 루트
    crates/
      common/            # 공통 유틸리티
        src/lib.rs       # config, db, date_utils, error types
      transforms/        # Polars 변환 로직
        src/lib.rs       # melt_nambu(), melt_namdong_pv(), melt_wind()
      collector-nambu/   # 남부발전 수집기 (CLI)
        src/main.rs      # --mode daily|backfill --start --end
      collector-namdong-pv/  # 남동발전 PV 수집기 (CLI)
        src/main.rs
      collector-namdong-wind/ # 남동발전 풍력 수집기 (CLI)
        src/main.rs
```

### Cargo.toml (workspace)

```toml
[workspace]
resolver = "2"
members = [
    "crates/common",
    "crates/transforms",
    "crates/collector-nambu",
    "crates/collector-namdong-pv",
    "crates/collector-namdong-wind",
]

[workspace.dependencies]
polars = { version = "0.53", features = [
    "lazy", "csv", "json", "temporal", "strings",
    "dtype-date", "dtype-datetime", "dtype-duration",
    "performant",
] }
tokio = { version = "1", features = ["full"] }
reqwest = { version = "0.12", features = ["json", "cookies"] }
sqlx = { version = "0.8", features = ["runtime-tokio", "postgres", "chrono"] }
clap = { version = "4", features = ["derive"] }
chrono = { version = "0.4", features = ["serde"] }
serde = { version = "1", features = ["derive"] }
serde_json = "1"
quick-xml = "0.37"       # XML API 응답 파싱 (남부발전)
dotenvy = "0.15"
tracing = "0.1"
tracing-subscriber = "0.3"
anyhow = "1"
```

### Prefect 연동 (subprocess)

```python
# prefect_flows/nambu_pv_flow.py (수정 후)
from prefect import flow, task
import subprocess

@task(name="Rust 남부발전 수집")
def collect_nambu_rust(mode: str, start: str, end: str):
    result = subprocess.run(
        ["./rust/target/release/collector-nambu",
         "--mode", mode, "--start", start, "--end", end],
        capture_output=True, text=True, timeout=600,
    )
    if result.returncode != 0:
        raise RuntimeError(f"Rust collector failed: {result.stderr}")
    return result.stdout
```

---

## P2-5. 수집기별 구현 계획

### Collector 1: 남부발전 (daily + backfill 통합)

**현행 Python**: `daily_pv_automation.py` (245L) + `nambu_backfill.py` (383L) = 628L

**Rust 구현 범위**:
1. CLI: `--mode daily|backfill --start YYYYMMDD --end YYYYMMDD [--gencd] [--hogi]`
2. API 호출: `reqwest` + `quick-xml` (XML 파싱)
3. 변환: Polars `unpivot()` + hour 추출 + datetime 생성 (공통 함수)
4. DB: `sqlx` DELETE+INSERT (트랜잭션)
5. 로깅: `tracing` (stdout JSON → Prefect가 수집)

**핵심 변환 (Polars)**:
```rust
fn melt_nambu_api(df: DataFrame) -> Result<DataFrame> {
    let v_vars: Vec<&str> = df.get_column_names().iter()
        .filter(|c| c.starts_with("qhorgen"))
        .cloned().collect();

    df.unpivot(v_vars, ["ymd", "hogi", "gencd", "ipptnm", "qvodgen", "qvodavg", "qvodmax", "qvodmin"])?
        .lazy()
        .with_columns([
            col("variable").str().extract(r"(\d+)", 1)
                .cast(DataType::Int32)? - lit(1)
                .alias("hour0"),
            col("value").cast(DataType::Float64).fill_null(lit(0.0))
                .alias("generation"),
        ])
        .with_columns([
            (col("ymd").str().to_date(StrptimeOptions::default())
             + duration(hours=col("hour0")))
            .alias("datetime"),
        ])
        .collect()
}
```

### Collector 2: 남동발전 PV

**현행 Python**: `namdong_collect_pv.py` (502L)

**Rust 구현 범위**:
1. CLI: `--start YYYYMMDD --end YYYYMMDD [--output-dir]`
2. HTTP: `reqwest` + cookie jar (세션 관리, 웹 스크래핑 특성)
3. CSV 파싱: Polars `read_csv()` + 인코딩 처리 (cp949 → utf-8)
4. 변환: 한글 컬럼 파싱 (`"N시 발전량"` → hour), Polars `unpivot()`
5. DB: `sqlx` DELETE+INSERT

**인코딩 처리 (주의)**:
```rust
// cp949 → UTF-8 변환 필요
use encoding_rs::EUC_KR;  // cp949 호환
let (decoded, _, _) = EUC_KR.decode(&raw_bytes);
let csv_content = decoded.into_owned();
```

### Collector 3: 남동발전 풍력

**현행 Python**: `namdong_wind_collect.py` (314L)

**Rust 구현 범위**:
1. CLI: `--start YYYYMMDD --end YYYYMMDD`
2. API 호출: `reqwest` JSON 페이지네이션
3. 변환: Polars `unpivot()` + hour=24→다음날 00:00 특수 로직
4. DB: `sqlx` ON CONFLICT UPSERT (batch 5000)

**24시 특수 처리 (Polars)**:
```rust
// hour=24 → 다음날 00:00 변환
.with_columns([
    when(col("hour_num").eq(lit(24)))
        .then(
            col("date") + duration(days=lit(1))  // 다음날 00:00
        )
        .otherwise(
            col("date") + duration(hours=col("hour_num"))
        )
        .alias("timestamp"),
])
```

---

## P2-6. 구현 로드맵

### Week 1-2: 공통 기반 + Collector 1 (남부발전)

1. `rust/` workspace 스캐폴딩
2. `crates/common`: config 로딩, DB pool, error types, tracing 설정
3. `crates/transforms`: `melt_nambu_api()` 구현 (Polars unpivot)
4. `crates/collector-nambu`: CLI + API 호출 + DB 적재
5. Python 결과와 동등성 테스트 (스냅샷 기반)

**산출물**: `collector-nambu` 바이너리 + 동등성 테스트

### Week 3: Collector 2 (남동 PV)

1. `crates/transforms`: `melt_namdong_pv()` 추가 (한글 파싱)
2. `crates/collector-namdong-pv`: CLI + 웹 CSV 다운로드 + 인코딩 처리
3. 동등성 테스트

**산출물**: `collector-namdong-pv` 바이너리 + 동등성 테스트

### Week 4: Collector 3 (남동 풍력) + CI

1. `crates/transforms`: `melt_wind()` 추가 (24시 처리)
2. `crates/collector-namdong-wind`: CLI + JSON API 페이지네이션
3. CI job 추가 (cargo fmt/clippy/test)
4. Prefect flow 연동 테스트

**산출물**: `collector-namdong-wind` 바이너리 + CI 통합

### Week 5: 통합 테스트 + 배포 준비

1. 전체 수집기 동등성 테스트 일괄 실행
2. Docker 빌드 (multi-stage: Rust build → slim runtime)
3. Prefect flow에서 Rust 바이너리 호출 경로 통합
4. fallback 경로 검증 (`USE_RUST=0` → Python 경로 유지)

---

## P2-7. 성공 기준

### 정확성
- 각 수집기의 Python 버전과 **동일한 DB 결과** 생성
- 동등성 테스트: 행 수, 컬럼, 타입, 값 (float 허용 오차 `1e-9`), NA 위치

### 운영성
- 단일 바이너리 배포 (Docker에서 COPY만으로 설치)
- 환경변수 호환 (기존 `.env` 그대로 사용)
- Prefect에서 subprocess로 호출 가능
- `USE_RUST=0`으로 즉시 Python fallback

### 품질
- `cargo fmt --check` + `cargo clippy -- -D warnings` CI 통과
- 각 crate에 단위 테스트 존재
- tracing 기반 구조화된 로그

---

## P2-8. 리스크 및 대응

| # | 리스크 | 영향 | 대응 |
|---|--------|------|------|
| 1 | **cp949 인코딩 처리** | 남동 PV CSV 파싱 실패 | `encoding_rs` crate, 다중 인코딩 fallback chain |
| 2 | **웹 스크래핑 세션 관리** | 남동 PV 쿠키/세션 유실 | reqwest cookie jar + retry |
| 3 | **API 스키마 변경** | 런타임 파싱 실패 | serde `#[serde(default)]` + drift 로그 |
| 4 | **Polars unpivot 동작 차이** | pandas melt와 미묘한 차이 | 동등성 테스트로 커버 |
| 5 | **DB URL 호환** | `postgresql+psycopg2://` vs `postgres://` | common crate에서 URL 정규화 |
| 6 | **팀 Rust 숙련도** | 유지보수 어려움 | 코어 로직을 transforms에 집중, CLI는 얇게 |

---

## 부록 C. Polars 레퍼런스 (Obsidian)

수집기 Rust 재작성에 필요한 Polars 문서가 `.claude/planner/polars/`에 정리되어 있습니다.

| 문서 | 내용 | 수집기 관련도 |
|------|------|------------|
| `10-Reshaping.md` | **unpivot/melt** 패턴 + 프로젝트 맞춤 예시 | **핵심** |
| `16-Rust-Crate.md` | Cargo.toml feature flags, 환경변수 | **핵심** |
| `17-Pandas-Migration.md` | pandas → Polars 연산 매핑 | **핵심** |
| `05-String-Operations.md` | str.extract, 정규식 패턴 | 높음 |
| `11-Time-Series.md` | 날짜/시간 연산, duration | 높음 |
| `14-Casting.md` | 타입 변환 (strict=False 등) | 높음 |
| `13-Lazy-API.md` | LazyFrame 최적화 | 중간 |
| `12-IO.md` | CSV/JSON 읽기 | 중간 |

---

## 부록 D. 현행 Python 수집기 의존성 매핑

| Python 의존성 | 용도 | Rust 대응 |
|---------------|------|----------|
| `aiohttp` | 비동기 HTTP | `reqwest` + `tokio` |
| `requests` | 동기 HTTP | `reqwest` (blocking) |
| `pandas` | DataFrame 변환 | `polars` |
| `numpy` | 배열 연산 | Polars expression 또는 직접 구현 |
| `sqlalchemy` | DB ORM/연결 | `sqlx` |
| `xml.etree` | XML 파싱 | `quick-xml` |
| `re` | 정규식 | `regex` (std) |
| `dotenv` | 환경변수 | `dotenvy` |
| `argparse` | CLI | `clap` |
| `pathlib` | 파일경로 | `std::path` |
| `encoding (cp949)` | 인코딩 변환 | `encoding_rs` |
