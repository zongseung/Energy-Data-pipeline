# Weather Pipeline 아키텍처 문서

## 프로젝트 개요

Weather Pipeline은 기상청 ASOS 데이터와 한국전력거래소(KPX) 전력수요 데이터를 수집, 처리, 통합하는 ETL 파이프라인입니다. Prefect를 사용하여 워크플로우를 관리하고, PostgreSQL 데이터베이스에 데이터를 저장합니다.

## 시스템 아키텍처

```
┌─────────────────────────────────────────────────────────────┐
│                    Prefect Server                            │
│              (워크플로우 오케스트레이션)                      │
└─────────────────────────────────────────────────────────────┘
                            │
                            │
        ┌───────────────────┴───────────────────┐
        │                                       │
┌───────▼────────┐                    ┌────────▼────────┐
│  Weather Flow  │                    │  Demand Flow    │
│  (일일 실행)    │                    │  (시간별 실행)   │
└───────┬────────┘                    └────────┬────────┘
        │                                       │
        │                                       │
┌───────▼────────┐                    ┌────────▼────────┐
│  ASOS API      │                    │  KPX API        │
│  (기상청)       │                    │  (전력거래소)    │
└───────┬────────┘                    └────────┬────────┘
        │                                       │
        │                                       │
┌───────▼────────┐                    ┌────────▼────────┐
│  결측치 처리    │                    │  5분 데이터      │
│  (스플라인/     │                    │  → DB 저장       │
│   역사적 평균)  │                    └────────┬────────┘
└───────┬────────┘                             │
        │                                       │
        │                                       │
┌───────▼────────┐                    ┌────────▼────────┐
│  CSV 저장      │                    │  1시간 집계      │
│  (일별/통합)   │                    │  + 기상 데이터    │
└────────────────┘                    │  통합            │
                                      └────────┬────────┘
                                               │
                                      ┌────────▼────────┐
                                      │  PostgreSQL DB  │
                                      │  - demand_5min  │
                                      │  - demand_       │
                                      │    weather_1h   │
                                      └─────────────────┘
```

## 디렉토리 구조

```
weather-pipeline/
├── fetch_data/              # 데이터 수집 및 처리 모듈
│   ├── collect_asos.py      # 기상청 ASOS 데이터 수집
│   ├── collect_demand.py    # KPX 전력수요 데이터 수집
│   ├── aggregate_hourly.py  # 1시간 단위 데이터 통합
│   ├── concat_demand.py     # CSV 파일 병합 (레거시)
│   ├── impute_missing.py    # 결측치 처리
│   ├── database.py          # DB 모델 및 연결
│   └── transfer_demand_1h.py # 데이터 전송 유틸
├── prefect_flows/            # Prefect 워크플로우
│   ├── prefect_pipeline.py  # 메인 ETL 플로우
│   ├── merge_to_all.py      # CSV 통합 유틸
│   └── deploy.py            # Prefect 배포 스크립트
├── notify/                  # 알림 모듈
│   └── slack_notifier.py    # Slack 알림
├── docker/                  # Docker 설정
│   ├── docker-compose.yml   # Prefect 서버/워커 설정
│   └── Dockerfile           # 컨테이너 이미지
├── data/                    # CSV 데이터 저장소
│   └── asos_*.csv           # 일별 기상 데이터
├── main.py                  # 진입점
├── pyproject.toml           # 프로젝트 의존성
└── README.md                # 프로젝트 개요
```

## 핵심 모듈 상세

### 1. 데이터 수집 모듈

#### `fetch_data/collect_asos.py`
- **기능**: 기상청 ASOS 시간별 데이터 수집
- **API**: 공공데이터포털 ASOS 시간별 정보 서비스
- **특징**:
  - 비동기 수집 (aiohttp 사용)
  - 42개 관측소 동시 수집
  - 재시도 로직 (최대 3회)
  - 일별 CSV 저장 및 통합 파일 병합

**주요 함수**:
- `fetch_city()`: 단일 관측소 데이터 수집
- `select_data_async()`: 모든 관측소 비동기 수집

#### `fetch_data/collect_demand.py`
- **기능**: KPX 전력수급 데이터 수집 (5분 단위)
- **API**: 한국전력거래소 공개 API
- **특징**:
  - 3개월 단위 자동 분할 다운로드
  - 백필 모드 지원 (DB 마지막 timestamp 이후만 수집)
  - 재시도 로직 (최대 5회, exponential backoff + jitter)
  - HTML 에러 페이지 감지 및 처리
  - PostgreSQL DB 저장 (upsert)
  - CSV 파일 다운로드 지원 (레거시 호환)

**주요 함수**:
- `collect_and_save()`: 날짜 범위 지정 수집 및 DB 저장
- `collect_with_backfill()`: 백필 모드 (DB 마지막 timestamp 이후 수집)
- `collect_recent_hours()`: 최근 N시간 수집
- `download_to_file()`: CSV 파일로 다운로드 (레거시)
- `get_last_timestamp()`: DB에서 마지막 timestamp 조회
- `request_with_retry()`: 재시도 로직이 포함된 HTTP 요청

### 2. 데이터 처리 모듈

#### `fetch_data/impute_missing.py`
- **기능**: 기상 데이터 결측치 처리
- **전략**:
  - 연속 3개 이하: 스플라인 보간 (cubic interpolation)
  - 연속 4개 이상: 역사적 평균값 (같은 지역, 같은 월-일-시)
- **특징**:
  - 지역별 독립 처리
  - 상세한 디버깅 정보 제공
  - 처리 전후 통계 출력

**주요 함수**:
- `impute_missing_values()`: 메인 결측치 처리 함수
- `spline_impute()`: 스플라인 보간
- `historical_average_impute()`: 역사적 평균값 채움

#### `fetch_data/aggregate_hourly.py`
- **기능**: 5분 단위 전력수요를 1시간 평균으로 집계하고 기상 데이터와 통합
- **특징**:
  - PostgreSQL 집계 쿼리 사용 (`date_trunc('hour', timestamp)`)
  - 기상 데이터 CSV 로드 및 조인
  - 공휴일 정보 자동 추가 (캐싱 지원)
  - Upsert 방식 저장 (timestamp + station_name 복합 키)
  - 기상 데이터 없이도 수요 데이터만 저장 가능

**주요 함수**:
- `aggregate_and_save_hourly()`: 날짜 범위 지정 통합 및 DB 저장
- `aggregate_with_backfill()`: 백필 모드 (DB 마지막 timestamp 이후 통합)
- `aggregate_recent_hours()`: 최근 N시간 통합
- `get_hourly_demand_from_db()`: DB에서 5분 데이터를 1시간 평균으로 집계
- `load_weather_from_csv()`: 기상 데이터 CSV 로드
- `merge_and_save_hourly()`: 전력수요와 기상 데이터 병합 및 저장

#### `fetch_data/concat_demand.py`
- **기능**: 여러 전력수요 CSV 파일을 하나로 병합 (레거시)
- **특징**:
  - 날짜 범위 기반 파일 정렬 및 병합
  - 백필 모드 지원 (기존 파일의 마지막 timestamp 이후만 추가)
  - 공휴일 정보 자동 추가
  - EUC-KR 인코딩 지원

**주요 함수**:
- `list_files_sorted()`: 날짜 범위 기준으로 파일 정렬
- `concat_csv_backfill_with_holiday()`: CSV 파일 병합 및 공휴일 추가
- `read_last_datetime()`: 기존 파일의 마지막 timestamp 읽기

### 3. 데이터베이스 모델

#### `fetch_data/database.py`
- **엔진**: SQLAlchemy (async) + asyncpg
- **테이블**:

**`demand_5min`** (5분 단위 전력수급 원본)
- `timestamp` (PK, unique)
- `current_demand`, `current_supply`
- `supply_capacity`, `supply_reserve`
- `reserve_rate`, `operation_reserve`
- `is_holiday`, `created_at`

**`demand_weather_1h`** (1시간 단위 통합 데이터)
- `timestamp`, `station_name` (복합 PK)
- `temperature`, `humidity`
- `demand_avg` (1시간 평균 수요)
- `is_holiday`, `created_at`

**주요 함수**:
- `init_db()`: 데이터베이스 테이블 초기화
- `get_session()`: 비동기 세션 생성기
- `get_last_timestamp_5min()`: `demand_5min` 테이블의 마지막 timestamp 조회 (백필용)
- `get_last_timestamp_1h()`: `demand_weather_1h` 테이블의 마지막 timestamp 조회 (백필용, station_name 옵션 지원)

### 4. Prefect 워크플로우

#### `prefect_flows/prefect_pipeline.py`

**주요 플로우**:

1. **`daily_weather_collection_flow`**
   - 실행 주기: 매일 오전 9시 (전날 데이터)
   - 작업 순서:
     1. 기상 데이터 수집
     2. 결측치 처리
     3. 일별 CSV 저장
     4. 통합 파일 병합
     5. Slack 알림

2. **`hourly_demand_collection_flow`**
   - 실행 주기: 매 시간
   - 작업 순서:
     1. DB 초기화
     2. 최근 2시간 전력수요 수집
     3. 최근 24시간 1시간 통합 데이터 생성

3. **`backfill_flow`**
   - 실행: 수동 또는 주기적
   - 작업 순서:
     1. 전력수요 백필
     2. 1시간 통합 데이터 백필

4. **`full_etl_flow`**
   - 실행: 수동
   - 전체 ETL 프로세스 통합 실행

## 데이터 흐름

### 기상 데이터 흐름
```
ASOS API → collect_asos.py → 결측치 처리 → CSV 저장 → 통합 파일 병합
```

### 전력수요 데이터 흐름
```
KPX API → collect_demand.py → PostgreSQL (demand_5min)
                                              ↓
                                    aggregate_hourly.py
                                              ↓
                              PostgreSQL (demand_weather_1h)
```

### 통합 데이터 생성
```
demand_5min (1시간 평균) + 기상 CSV → demand_weather_1h
```

## 환경 변수

| 변수명 | 설명 | 필수 |
|--------|------|------|
| `SERVICE_KEY` | 기상청 API 키 | ✅ |
| `SLACK_WEBHOOK_URL` | Slack 알림 웹훅 | ❌ |
| `DEMAND_DATABASE_URL` | PostgreSQL 연결 URL | ✅ |
| `PREFECT_API_URL` | Prefect 서버 URL | ❌ |
| `PREFECT_DOCKER_NETWORK` | Docker 네트워크명 | ❌ |

## 의존성

주요 라이브러리:
- **Prefect 3.x**: 워크플로우 오케스트레이션
- **SQLAlchemy 2.x**: ORM (async)
- **asyncpg**: PostgreSQL 비동기 드라이버
- **aiohttp**: 비동기 HTTP 클라이언트
- **pandas**: 데이터 처리
- **scipy**: 스플라인 보간
- **workalendar**: 공휴일 계산

## 확장 가능성

1. **추가 데이터 소스**: 다른 기상 관측소, 전력 관련 데이터
2. **실시간 스트리밍**: Kafka/Kinesis 연동
3. **데이터 분석**: ML 모델 학습 파이프라인 추가
4. **모니터링**: Grafana 대시보드 (docker/grafana 참고)
5. **알림 확장**: 이메일, PagerDuty 등

## 성능 최적화

1. **비동기 처리**: 모든 I/O 작업은 async/await 사용
2. **배치 처리**: 3개월 단위로 분할하여 API 제한 회피
3. **재시도 전략**: Exponential backoff + jitter
4. **DB 연결 풀링**: SQLAlchemy connection pool 사용
5. **캐싱**: 공휴일 정보 캐싱

## 보안 고려사항

1. **API 키 관리**: 환경 변수 사용, .env 파일 gitignore
2. **DB 접근**: 네트워크 격리, 인증 정보 암호화
3. **에러 처리**: 민감한 정보 로그 출력 방지

## 트러블슈팅

### 일반적인 문제

1. **API 호출 실패**
   - 재시도 로직 확인
   - API 키 유효성 확인
   - 네트워크 연결 확인

2. **DB 연결 실패**
   - `DEMAND_DATABASE_URL` 확인
   - PostgreSQL 서버 상태 확인
   - 네트워크 접근 권한 확인

3. **Prefect 플로우 실행 실패**
   - Prefect 서버 상태 확인
   - 워커 로그 확인
   - 환경 변수 설정 확인

## 참고 자료

- [Prefect 문서](https://docs.prefect.io/)
- [SQLAlchemy 문서](https://docs.sqlalchemy.org/)
- [기상청 API 문서](https://www.data.go.kr/)
- [KPX API 문서](https://openapi.kpx.or.kr/)

