# DW(Demand - weather Data) Pipeline

기상청 ASOS 데이터와 한국전력거래소(KPX) 전력수요 데이터를 수집, 처리, 통합하는 ETL 파이프라인입니다. Prefect를 사용하여 워크플로우를 관리하고, PostgreSQL 데이터베이스에 데이터를 저장합니다.
전력수급 데이터는 다음 링크를 통해 확인이 가능함 : https://openapi.kpx.or.kr/openapi.do#
## 주요 기능

- 🌤️ **기상 데이터 수집**: 기상청 ASOS 시간별 데이터 (42개 관측소)
- ⚡ **전력수요 데이터 수집**: KPX 5분 단위 전력수급 데이터
- 🔄 **데이터 통합**: 1시간 단위 기상+전력수요 통합 데이터 생성
- 📊 **결측치 처리**: 스플라인 보간 및 역사적 평균값 기반 보정
- 🗄️ **데이터베이스 저장**: PostgreSQL에 구조화된 데이터 저장
- 📅 **자동화**: Prefect 기반 스케줄링 및 워크플로우 관리
- 🔔 **알림**: Slack 웹훅을 통한 실행 결과 알림

## 프로젝트 구조

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

## 빠른 시작

### 1. 환경 설정

프로젝트 루트에 `.env` 파일을 생성하고 다음 환경 변수를 설정하세요:

```bash
# 필수
SERVICE_KEY=your_weather_api_key
DEMAND_DATABASE_URL=postgresql+asyncpg://user:password@host:5432/database

# 선택
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/...
PREFECT_API_URL=http://prefect-server-new:4200/api
PREFECT_DOCKER_NETWORK=weather-pipeline_prefect-new
```

### 2. 의존성 설치

```bash
# uv 사용 (권장)
uv sync

# 또는 pip 사용
pip install -e .
```

### 3. 데이터베이스 설정

PostgreSQL 데이터베이스를 준비하고 `DEMAND_DATABASE_URL`을 설정하세요. 테이블은 자동으로 생성됩니다.

### 4. Docker Compose로 실행

```bash
cd docker
# 이미지 빌드 및 서버/워커/배포 컨테이너 기동
docker compose up -d --build

# Prefect 서버가 healthy 되면 배포 스크립트 실행
docker compose run --rm weather-deployer
```

### 5. Prefect UI 접속

브라우저에서 `http://localhost:4300`에 접속하여 Prefect UI를 확인하고 플로우를 실행할 수 있습니다.

## 사용 방법

### 개별 스크립트 실행

#### 기상 데이터 수집
```bash
python fetch_data/collect_asos.py
# 입력 예: 20250101,20250131
```

#### 전력수요 데이터 수집
```bash
python fetch_data/collect_demand.py
# 선택:
# 1. 날짜 범위 지정 수집 (DB 저장)
#    - 입력: start,end = 'YYYYMMDD,YYYYMMDD'
# 2. 백필 모드 (DB 마지막 이후 수집)
#    - 자동으로 DB의 마지막 timestamp 이후 데이터만 수집
# 3. 최근 2시간 수집 (1시간 스케줄용)
#    - 최근 2시간 데이터를 수집하여 DB에 저장
# 4. CSV 파일로 다운로드 (기존 방식)
#    - 입력: start,end = 'YYYYMMDD,YYYYMMDD'
```

#### 1시간 데이터 통합
```bash
python fetch_data/aggregate_hourly.py
# 선택:
# 1. 날짜 범위 지정 통합
#    - 입력: 시작일시 (YYYY-MM-DD HH), 종료일시 (YYYY-MM-DD HH)
#    - 5분 데이터를 1시간 평균으로 집계하고 기상 데이터와 통합
# 2. 백필 모드
#    - 자동으로 DB의 마지막 timestamp 이후 데이터만 통합
# 3. 최근 24시간 통합
#    - 최근 24시간 데이터를 통합하여 DB에 저장
```

### Prefect 플로우 실행

#### CLI로 실행
```bash
# 일일 기상 데이터 수집
prefect deployment run 'daily-weather-collection-flow' -p target_date=20250101

# 시간별 전력수요 수집
prefect deployment run 'hourly-demand-collection-flow'

# 백필 플로우
prefect deployment run 'backfill-flow'

# 전체 ETL 플로우
prefect deployment run 'full-etl-flow' -p target_date=20250101
```

#### Prefect UI에서 실행
1. `http://localhost:4300` 접속
2. Deployments 메뉴에서 원하는 플로우 선택
3. "Run" 버튼 클릭하여 실행

### 워크풀 리셋

워크풀을 리셋해야 할 경우:

```bash
docker compose run --rm weather-deployer prefect work-pool delete weather-new-pool || true
docker compose run --rm weather-deployer
```

## 데이터베이스 스키마

### `demand_5min` 테이블
5분 단위 전력수급 원본 데이터

| 컬럼 | 타입 | 설명 |
|------|------|------|
| `id` | Integer | 자동 증가 PK |
| `timestamp` | DateTime | 기준일시 (unique index) |
| `current_demand` | Float | 현재수요(MW) |
| `current_supply` | Float | 현재공급(MW) |
| `supply_capacity` | Float | 공급가능용량(MW) |
| `supply_reserve` | Float | 공급예비력(MW) |
| `reserve_rate` | Float | 공급예비율(%) |
| `operation_reserve` | Float | 운영예비력(MW) |
| `is_holiday` | Boolean | 공휴일 여부 |
| `created_at` | DateTime | 생성일시 |

**인덱스**: `ix_demand_5min_timestamp_unique` (timestamp, unique)

### `demand_weather_1h` 테이블
1시간 단위 기상+전력수요 통합 데이터

| 컬럼 | 타입 | 설명 |
|------|------|------|
| `id` | Integer | 자동 증가 PK |
| `timestamp` | DateTime | 기준일시 (복합 unique index) |
| `station_name` | String(50) | 관측소명 (복합 unique index) |
| `temperature` | Float | 기온(°C) |
| `humidity` | Float | 습도(%) |
| `demand_avg` | Float | 1시간 평균 수요(MW) |
| `is_holiday` | Boolean | 공휴일 여부 |
| `created_at` | DateTime | 생성일시 |

**인덱스**: `ix_demand_weather_1h_timestamp_station` (timestamp, station_name, unique)

## 생성되는 파일

### CSV 파일
- **일별 기상 데이터**: `data/asos_YYYYMMDD_YYYYMMDD.csv`
  - 기상청 ASOS 시간별 데이터 (42개 관측소)
  - 컬럼: date, temperature, humidity, station_name
- **통합 기상 데이터**: `data/asos_all_merged.csv`
  - 모든 일별 파일을 병합한 누적 데이터
- **전력수요 데이터**: `Demand_Data_YYYYMMDD_YYYYMMDD.csv` (레거시)
  - KPX 5분 단위 전력수급 데이터 (CSV 형식)
  - `Demand_Data_all.csv`: 모든 기간을 병합한 파일

### 데이터베이스
- **PostgreSQL**에 구조화된 데이터 저장
- **자동 upsert 처리**: 중복 데이터 방지
  - `demand_5min`: timestamp 기준 upsert
  - `demand_weather_1h`: timestamp + station_name 기준 upsert
- **자동 테이블 생성**: `init_db()` 호출 시 테이블 자동 생성

## 주요 워크플로우

### 1. 일일 기상 데이터 수집 플로우
- **실행 주기**: 매일 오전 9시 (전날 데이터)
- **작업 순서**:
  1. 기상 데이터 수집 (42개 관측소)
  2. 결측치 처리 (스플라인 보간 / 역사적 평균)
  3. 일별 CSV 저장
  4. 통합 파일 병합
  5. Slack 알림

### 2. 시간별 전력수요 수집 플로우
- **실행 주기**: 매 시간
- **작업 순서**:
  1. DB 초기화
  2. 최근 2시간 전력수요 수집
  3. 최근 24시간 1시간 통합 데이터 생성

### 3. 백필 플로우
- **실행**: 수동 또는 주기적
- **작업 순서**:
  1. 전력수요 백필 (DB 마지막 이후 데이터)
  2. 1시간 통합 데이터 백필

### 4. 전체 ETL 플로우
- **실행**: 수동
- **작업 순서**:
  1. 기상 데이터 수집 및 처리
  2. 전력수요 수집
  3. 1시간 통합 데이터 생성

## 결측치 처리 전략

기상 데이터의 결측치는 다음 전략으로 처리됩니다:

1. **연속 3개 이하**: 스플라인 보간 (cubic interpolation)
2. **연속 4개 이상**: 역사적 평균값 (같은 지역, 같은 월-일-시)

## 의존성

주요 라이브러리:
- **Prefect 3.x**: 워크플로우 오케스트레이션
- **SQLAlchemy 2.x**: ORM (async)
- **asyncpg**: PostgreSQL 비동기 드라이버
- **aiohttp**: 비동기 HTTP 클라이언트
- **pandas**: 데이터 처리
- **scipy**: 스플라인 보간
- **workalendar**: 공휴일 계산

전체 의존성은 `pyproject.toml`을 참고하세요.

## 환경 변수

| 변수명 | 설명 | 필수 | 기본값 |
|--------|------|------|--------|
| `SERVICE_KEY` | 기상청 API 키 | ✅ | - |
| `DEMAND_DATABASE_URL` | PostgreSQL 연결 URL | ✅ | `postgresql+asyncpg://demand:demand@demand-db:5432/demand` |
| `SLACK_WEBHOOK_URL` | Slack 알림 웹훅 | ❌ | - |
| `PREFECT_API_URL` | Prefect 서버 URL | ❌ | `http://prefect-server-new:4200/api` |
| `PREFECT_DOCKER_NETWORK` | Docker 네트워크명 | ❌ | `weather-pipeline_prefect-new` |

## 트러블슈팅

### API 호출 실패
- API 키 유효성 확인
- 네트워크 연결 확인
- 재시도 로직 확인 (자동으로 최대 3-5회 재시도)

### DB 연결 실패
- `DEMAND_DATABASE_URL` 환경 변수 확인
- PostgreSQL 서버 상태 확인
- 네트워크 접근 권한 확인

### Prefect 플로우 실행 실패
- Prefect 서버 상태 확인: `docker compose ps`
- 워커 로그 확인: `docker compose logs weather-worker`
- 환경 변수 설정 확인

## 추가 문서

- [ARCHITECTURE.md](./ARCHITECTURE.md): 상세 아키텍처 및 모듈 문서

## 라이선스

이 프로젝트는 내부 사용을 위한 것입니다.

## 기여

이슈 및 개선 사항은 프로젝트 관리자에게 문의하세요.
