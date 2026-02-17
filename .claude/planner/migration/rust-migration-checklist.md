# Rust Migration 체크리스트

기준일: 2026-02-17
관련 문서: `rust-data-layer-poc.md` (v3 — 수집기 Rust 재작성 반영)

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
| 2026-02-17 | Phase 2 | **진행** | 수집기 Rust 재작성 — 성능이 아닌 구조/안정성/배포 관점 |

---

## Phase 2: Rust 수집기 재작성

> Phase 0/1과 별도 트랙. 수집기 전체를 Rust + Polars로 재작성합니다.
> 상세 설계: `rust-data-layer-poc.md` v3 §P2 참조

### 2-0. 사전 준비

- [x] Polars 레퍼런스 문서 작성 (`.claude/planner/polars/` 18개 파일)
- [x] 수집기 전수 조사 (4개 주요 + 4개 보조)
- [x] Melt/Unpivot 사이트 전수 조사 (6곳)
- [x] DB 인터페이스 분석 (3개 테이블, 2가지 upsert 패턴)
- [ ] Rust 툴체인 설치 확인 (`rustup`, `cargo`)
- [ ] Polars 0.53+ 빌드 확인 (로컬)

### 2-1. Workspace 스캐폴딩

- [ ] `rust/` 디렉토리 생성
- [ ] `rust/Cargo.toml` workspace 설정
  - [ ] workspace.dependencies 공통 의존성 정의
  - [ ] polars, tokio, reqwest, sqlx, clap, chrono, serde, quick-xml
- [ ] `rust/crates/common/` 생성
  - [ ] `Cargo.toml`
  - [ ] `src/lib.rs` — config, db pool, error types, tracing 설정
  - [ ] `src/config.rs` — 환경변수 로딩 (DB_URL, API keys)
  - [ ] `src/db.rs` — PgPool 초기화, URL 정규화 (`postgresql+psycopg2://` → `postgres://`)
  - [ ] `src/error.rs` — anyhow + thiserror 에러 타입
- [ ] `rust/crates/transforms/` 생성
  - [ ] `Cargo.toml` (polars 의존)
  - [ ] `src/lib.rs` — 공통 unpivot/melt 인터페이스
- [ ] `cargo build` 성공 확인
- [ ] `cargo fmt && cargo clippy` 통과 확인

### 2-2. Transforms: Polars 변환 로직

- [ ] `melt_nambu_api()` 구현
  - [ ] qhorgen01~24 → unpivot
  - [ ] hour 추출 (0-based): `str.extract(r"(\d+)")` - 1
  - [ ] datetime 생성: ymd + hour offset
  - [ ] generation, daily_total/avg/max/min 타입 변환
  - [ ] 단위 테스트: 고정 입력 → 예상 출력 검증
- [ ] `melt_namdong_pv()` 구현
  - [ ] 한글 컬럼 ("N시 발전량") → unpivot
  - [ ] hour 추출: `str.extract(r"(\d+)시")`
  - [ ] datetime 생성: date + (hour-1) offset
  - [ ] 단위 테스트
- [ ] `melt_wind()` 구현
  - [ ] qhorGen01~24 → unpivot
  - [ ] **24시 → 다음날 00:00** 특수 로직
  - [ ] plant_name = ipptNam + " " + hogi
  - [ ] 단위 테스트 (특히 24시 경계 케이스)
- [ ] 동등성 스냅샷 테스트 프레임워크 구성
  - [ ] `tests/equivalence/fixtures/` 에 Python 결과 스냅샷 저장
  - [ ] Rust 결과와 row-by-row 비교

### 2-3. Collector 1: 남부발전 (collector-nambu)

- [ ] `rust/crates/collector-nambu/` 생성
- [ ] CLI 구현 (clap)
  - [ ] `--mode daily|backfill`
  - [ ] `--start YYYYMMDD --end YYYYMMDD`
  - [ ] `--gencd`, `--hogi` (선택적 필터)
  - [ ] `--db-url` (선택, 기본: 환경변수)
- [ ] API 호출 구현
  - [ ] `reqwest` + `quick-xml` (XML 응답 파싱)
  - [ ] 페이지네이션 (pageNo, numOfRows)
  - [ ] API key 인코딩 처리 (URL-encoded key 지원)
  - [ ] 재시도 로직 (backfill 모드: 당일 실패 → 다음날 시도)
- [ ] 데이터 변환
  - [ ] `melt_nambu_api()` 호출
  - [ ] plant.json 매핑 (gencd → plant_name)
- [ ] DB 적재
  - [ ] DELETE range + batch INSERT (트랜잭션)
  - [ ] 날짜 범위 기반 삭제
- [ ] 로깅
  - [ ] tracing JSON 출력
  - [ ] 수집 건수, 소요시간, 에러 로그
- [ ] Python 동등성 테스트
  - [ ] 동일 API 입력 → 동일 DB 결과 검증
- [ ] `cargo test` 통과

### 2-4. Collector 2: 남동발전 PV (collector-namdong-pv)

- [ ] `rust/crates/collector-namdong-pv/` 생성
- [ ] CLI 구현 (clap)
  - [ ] `--start YYYYMMDD --end YYYYMMDD`
  - [ ] `--output-dir` (CSV 저장 경로)
  - [ ] 환경변수: NAMDONG_ORG_NO, NAMDONG_HOKI_S/E, NAMDONG_PAGE_INDEX
- [ ] HTTP 구현
  - [ ] `reqwest` + cookie jar (세션 관리)
  - [ ] 월별 분할 요청 (date range → monthly chunks)
  - [ ] CSV 다운로드 (POST form data)
  - [ ] 응답 검증 (MIME type, size, comma density)
- [ ] 인코딩 처리
  - [ ] `encoding_rs` 사용 (cp949/euc-kr → UTF-8)
  - [ ] 다중 인코딩 fallback chain
- [ ] 데이터 변환
  - [ ] 컬럼명 정규화 (공백, 개행 제거)
  - [ ] `melt_namdong_pv()` 호출
  - [ ] hogi suffix 처리 (복수 hogi 발전소)
- [ ] DB 적재
  - [ ] DELETE range + batch INSERT
- [ ] 동등성 테스트
- [ ] `cargo test` 통과

### 2-5. Collector 3: 남동발전 풍력 (collector-namdong-wind)

- [ ] `rust/crates/collector-namdong-wind/` 생성
- [ ] CLI 구현 (clap)
  - [ ] `--start YYYYMMDD --end YYYYMMDD`
  - [ ] `--backfill-csv` (CSV 백필 모드)
- [ ] API 호출
  - [ ] `reqwest` JSON 페이지네이션
  - [ ] page-based 반복 (size=100)
  - [ ] API 응답 키 typo 처리 (`response` / `reponse`)
- [ ] 데이터 변환
  - [ ] `melt_wind()` 호출
  - [ ] 24시 경계 처리 검증
- [ ] DB 적재
  - [ ] ON CONFLICT UPSERT (batch 5000)
- [ ] CSV 백필 모드
  - [ ] CSV 파일 읽기 → DB 적재
- [ ] 동등성 테스트
- [ ] `cargo test` 통과

### 2-6. Prefect 연동

- [ ] Prefect flow에서 Rust 바이너리 호출 경로 추가
  - [ ] `subprocess.run()` wrapper task
  - [ ] stdout/stderr 캡처 + 로그 연동
  - [ ] timeout 설정
- [ ] `USE_RUST` 환경변수로 Python/Rust 경로 선택
  - [ ] `USE_RUST=1` → Rust 바이너리 호출
  - [ ] `USE_RUST=0` (기본) → 기존 Python 함수 호출
- [ ] fallback 테스트: Rust 실패 시 Python으로 자동 전환 (선택)

### 2-7. CI 구성

- [ ] `.github/workflows/ci.yml`에 rust job 추가:
  - [ ] Rust toolchain 설치 (`actions-rust-lang/setup-rust-toolchain`)
  - [ ] `cargo fmt --check`
  - [ ] `cargo clippy -- -D warnings`
  - [ ] `cargo test`
  - [ ] 캐시 설정 (`Swatinem/rust-cache`)
- [ ] 기존 python-uv job에 영향 없음 확인
- [ ] (선택) cross-compile job: linux x86_64 바이너리 빌드

### 2-8. Docker 통합

- [ ] Rust 빌드용 Dockerfile 작성 (multi-stage)
  - [ ] Stage 1: `rust:1.xx-slim` → cargo build --release
  - [ ] Stage 2: `debian:bookworm-slim` → 바이너리 COPY
- [ ] docker-compose에 Rust 수집기 서비스 추가 (선택)
- [ ] 기존 Python 컨테이너에 Rust 바이너리 포함 (선택)

### 2-9. 통합 테스트 + 최종 검증

- [ ] 전체 수집기 동등성 테스트 일괄 실행
- [ ] Prefect flow end-to-end 테스트 (스테이징)
- [ ] 성능 비교 (Python vs Rust 수집기)
  - [ ] wall time
  - [ ] 메모리 사용량
  - [ ] DB 적재 건수 일치
- [ ] fallback 경로 검증
  - [ ] `USE_RUST=0`으로 즉시 Python 복원
  - [ ] Rust 바이너리 미존재 시 Python 자동 사용
- [ ] 최종 리포트 작성

---

## 공통 체크포인트 (Phase 2)

### 각 Collector 완료 시 확인사항

- [ ] `cargo test` 전부 통과
- [ ] `cargo clippy -- -D warnings` 경고 없음
- [ ] Python 동등성 테스트 통과 (동일 입력 → 동일 DB 결과)
- [ ] 기존 `pytest tests/` 전부 통과 (Python 측 미영향)
- [ ] 커밋 + PR 작성

### 롤백 확인사항

- [ ] `USE_RUST=0`으로 즉시 Python 경로 복원 가능
- [ ] Rust 바이너리 미설치 환경에서도 기존 파이프라인 정상 동작
- [ ] 롤백 시 데이터 재처리 불필요 (DB 스키마 동일)
