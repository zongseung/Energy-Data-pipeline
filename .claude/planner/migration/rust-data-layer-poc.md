# Data Layer Rust 도입 PoC 기획서 (v2 — 프로파일링 반영판)

기준일: 2026-02-17
최종 수정: 2026-02-17 (코드 리뷰 및 병목 분석 반영)
대상 저장소: `Energy-Data-pipeline`
운영 전제: Python 오케스트레이션(Prefect) 유지 + **Python 최적화 선행** + 병목 구간만 Rust로 점진 치환

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
  fetch_data/
  prefect_flows/
  notify/
  docker/
  scripts/
  configs/
    test.toml
    prod.toml

  rust/                     # 신규 (Phase 1 진입 시 생성)
    Cargo.toml              # workspace
    crates/
      data_layer/           # pyo3 모듈 (python import용)
      transforms/           # 순수 변환 라이브러리 (polars/arrow)
      ingest/               # (선택) bulk ingest CLI
    pyproject.toml          # (선택) maturin 설정을 rust/ 아래로 분리

  tests/
    equivalence/            # 동등성 스냅샷 기반 테스트
    benchmark/              # 고정 입력셋 벤치 (Phase 0부터 사용)
```

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
