# Rust Migration 체크리스트

기준일: 2026-02-17
관련 문서: `rust-data-layer-poc.md` (v2)

---

## Phase 0: Python 최적화 + 프로파일링 (필수)

### 0-1. 프로파일링 환경 구성

- [x] py-spy 또는 cProfile 설치 확인
- [x] 프로파일링용 고정 입력 데이터셋 준비
  - [x] impute_missing용: 합성 기상 데이터 (profile_impute.py 내 generate_weather_data)
  - [ ] ~~transform_wide_to_long용~~ (Phase 0 분석에서 후보 제외)
  - [ ] ~~backfill용~~ (Phase 0 분석에서 후보 제외)
- [x] `tests/benchmark/` 디렉토리 생성
- [x] baseline 프로파일링 스크립트 작성 (`tests/benchmark/profile_impute.py`)

### 0-2. Baseline 측정

- [x] `impute_missing_values()` wall time 측정 (300/1K/3K/10K행)
- [ ] ~~`transform_wide_to_long()` wall time 측정~~ (후보 제외)
- [ ] ~~`_rows_from_api_payload()` wall time 측정~~ (후보 제외)
- [ ] 전체 파이프라인(Prefect flow) 엔드-투-엔드 실행 시간 측정 (향후)
- [x] 각 함수의 **CPU vs I/O 비율** 기록
- [x] 결과를 `docs/phase0_report.md`에 기록

### 0-3. impute_missing.py 최적화

- [x] `find_consecutive_missing_groups()`: `.iloc[i]` → `.values` numpy 배열 접근으로 변경 (**45x 개선**)
- [x] `historical_average_impute()`:
  - [x] groupby 룩업 테이블로 반복 필터링 제거 (**2x 개선**)
  - [x] `col_loc`, `station_all_mean`, `all_mean` 사전 캐싱
- [x] `spline_impute()` slice 객체 캐싱
- [x] 기존 테스트 통과 확인 (`pytest tests/`) — 34 passed, 1 xfailed
- [x] **최적화 전후 동등성 검증**: 신규 테스트 4개 추가, 전부 통과

### 0-4. I/O 계층 최적화 검토

- [ ] API 호출 패턴 분석:
  - [ ] `fetch_namdong_wind_api()`: 배치 요청 가능 여부 확인
  - [ ] `_fetch_api_days()`: 비동기 병렬 요청 가능 범위 확인 (rate limit 고려)
- [ ] DB 적재 패턴 분석:
  - [ ] `upsert_wind_namdong()`: batch size 최적화 여지
  - [ ] `backfill()` 내 to_sql: COPY 기반 벌크 인서트 가능 여부
- [ ] 적용 가능한 최적화 구현 (해당 시)
- [ ] 기존 테스트 통과 확인

### 0-5. Phase 0 리포트 + Go/No-Go 판단

- [x] 최적화 후 프로파일링 재실행
- [x] 전후 비교 리포트 작성 (`docs/phase0_report.md`)
  - [x] 함수별 wall time 변화 (before/after)
  - [x] CPU vs I/O 비율 변화
  - [ ] 메모리 peak 변화 (미측정 — 향후 필요 시)
- [x] **Go/No-Go 판단 기준 확인**:
  - [x] Python 최적화 후에도 순수 CPU 병목이 wall time의 30% 이상인 함수가 있는가? → **아니오 (~15%)**
  - [x] 해당 함수의 입력 데이터가 10,000행 이상인가? → **아니오 (운영 데이터 수천행)**
  - [x] PyO3 직렬화 오버헤드를 상쇄할 규모인가? → **아니오**
- [x] **판정 기록**: **No-Go** (사유: docs/phase0_report.md §5)

---

## Phase 1: Rust PoC (Go 판정 시에만 진행)

### 1-1. Rust 대상 확정

- [ ] Phase 0 프로파일링 결과 기반 대상 함수 최종 선정
- [ ] 대상 함수의 입출력 인터페이스 문서화
  - [ ] 입력 스키마 (컬럼명, 타입, nullable 여부)
  - [ ] 출력 스키마 (컬럼명, 타입, 정렬 규칙)
  - [ ] 엣지 케이스 정리 (빈 입력, 전체 NaN, 단일 행 등)

### 1-2. Rust workspace 구성

- [ ] `rust/` 디렉토리 생성
- [ ] `rust/Cargo.toml` workspace 설정
- [ ] `rust/crates/transforms/` — 순수 변환 라이브러리
  - [ ] `Cargo.toml` 의존성 설정 (polars 또는 arrow-rs)
  - [ ] 핵심 함수 구현
  - [ ] Rust 단위 테스트 작성
- [ ] `rust/crates/data_layer/` — pyo3 바인딩 모듈
  - [ ] `Cargo.toml` + pyo3/maturin 설정
  - [ ] Python callable 함수 노출
  - [ ] 입출력 직렬화 방식 결정 (Arrow IPC / CSV / JSON)
- [ ] `cargo fmt && cargo clippy && cargo test` 전부 통과 확인

### 1-3. Python 연동

- [ ] `maturin develop`로 로컬 빌드/설치 확인
- [ ] Python에서 `import rust_data_layer` 성공 확인
- [ ] 기존 함수에 `use_rust=False` 플래그 추가
  - [ ] `use_rust=True`일 때 Rust 모듈 호출 경로
  - [ ] `use_rust=False`일 때 기존 Python 경로 (변경 없음)
- [ ] 환경변수 `USE_RUST=0/1`로도 제어 가능하게 구성

### 1-4. 동등성 테스트

- [ ] `tests/equivalence/fixtures/` 생성 + 고정 입력 데이터 저장
- [ ] Python 결과를 "정답 스냅샷"으로 저장
- [ ] 동등성 테스트 작성:
  - [ ] 행 수 일치
  - [ ] 컬럼 이름/타입 일치
  - [ ] key 컬럼 값 일치 (ts, id 등)
  - [ ] 수치 컬럼 허용 오차 이내 (abs_err <= 1e-9)
  - [ ] NA 위치 일치
  - [ ] 정렬 순서 일치
- [ ] 엣지 케이스 테스트:
  - [ ] 빈 DataFrame 입력
  - [ ] 전체 NaN 컬럼
  - [ ] 단일 행 입력
  - [ ] 매우 큰 입력 (성능 + 메모리)

### 1-5. 벤치마크

- [ ] Python 최적화 버전 vs Rust 버전 비교 스크립트 (`tests/benchmark/rust_vs_python.py`)
- [ ] 측정 항목:
  - [ ] wall time (고정 입력셋, 반복 평균)
  - [ ] 메모리 peak (tracemalloc 또는 /usr/bin/time -v)
  - [ ] PyO3 직렬화 오버헤드 (빈 데이터 왕복 시간)
- [ ] 입력 크기별 스케일링 그래프 (100/1K/10K/100K행)

### 1-6. CI 구성

- [ ] `.github/workflows/ci.yml`에 rust job 추가:
  - [ ] Rust toolchain 설치 (actions-rust-lang/setup-rust-toolchain)
  - [ ] cargo fmt --check
  - [ ] cargo clippy -- -D warnings
  - [ ] cargo test
  - [ ] 캐시 설정 (Swatinem/rust-cache)
- [ ] equivalence job 추가 (선택):
  - [ ] maturin build
  - [ ] Python vs Rust 동등성 테스트 실행
- [ ] 기존 python-uv job에 영향 없음 확인

### 1-7. 최종 리포트 + Go/No-Go

- [ ] 최종 PoC 리포트 작성 (`docs/poc_report_final.md`)
  - [ ] 동등성 테스트 결과
  - [ ] 벤치마크 결과 (Python 최적화 vs Rust)
  - [ ] 운영성 평가 (fallback, CI, 디버깅 용이성)
  - [ ] 유지보수 비용 평가
- [ ] **최종 판정**: 프로덕션 적용 / 보류 / 중단

---

## 공통 체크포인트

### 각 단계 완료 시 확인사항

- [ ] 기존 `pytest tests/` 전부 통과
- [ ] CI green (python-uv job)
- [ ] 기존 Prefect flow 정상 동작 확인 (스테이징)
- [ ] 변경사항 커밋 + PR 작성

### 롤백 확인사항

- [ ] `use_rust=False` (또는 `USE_RUST=0`)로 즉시 Python 경로 복원 가능
- [ ] Rust 모듈 미설치 환경에서도 기존 파이프라인 정상 동작
- [ ] 롤백 시 데이터 재처리 절차 문서화

---

## 판정 기록

| 일자 | 단계 | 판정 | 사유 |
|------|------|------|------|
| 2026-02-17 | Phase 0 완료 | **No-Go** | Python 최적화로 전체 2x 개선. 최적화 후 순수 CPU 병목은 전체의 ~15%로 축소. 나머지 85%는 pandas/scipy C 코드. 데이터 규모(수천행)도 PyO3 오버헤드를 상쇄할 수준 미달. 상세: `docs/phase0_report.md` |
| - | Phase 1 | 미진행 | Phase 0에서 No-Go 판정. 데이터 규모 10만행+ 증가 시 재검토 |
