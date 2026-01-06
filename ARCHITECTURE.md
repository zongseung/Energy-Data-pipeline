# Energy PV Pipeline 아키텍처

## 개요

본 파이프라인은 **태양광(PV) 발전 데이터**와 **기상 데이터**를 수집하여 PostgreSQL에 저장하고 Grafana로 시각화합니다.

## 시스템 구성도

```
                                    ┌─────────────────────┐
                                    │    External APIs    │
                                    ├─────────────────────┤
                                    │ - 기상청 ASOS API   │
                                    │ - 남부발전 API      │
                                    │ - 남동발전 CSV      │
                                    └──────────┬──────────┘
                                               │
                                               ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│                            Docker Compose Environment                         │
│                                                                               │
│  ┌─────────────────────────────────────────────────────────────────────────┐ │
│  │                         Prefect Orchestration                            │ │
│  │  ┌───────────────────┐    ┌───────────────────┐    ┌─────────────────┐  │ │
│  │  │ prefect-postgres  │    │  prefect-server   │    │    pv-worker    │  │ │
│  │  │      -new         │◄───│      -new         │◄───│                 │  │ │
│  │  │                   │    │                   │    │                 │  │ │
│  │  │ ┌───────────────┐ │    │ ┌───────────────┐ │    │ ┌─────────────┐ │  │ │
│  │  │ │ Flow 상태     │ │    │ │ API Server    │ │    │ │ Docker      │ │  │ │
│  │  │ │ 스케줄 정보   │ │    │ │ :4200         │ │    │ │ Worker      │ │  │ │
│  │  │ │ 실행 로그     │ │    │ │ (외부:4300)   │ │    │ │             │ │  │ │
│  │  │ └───────────────┘ │    │ └───────────────┘ │    │ │ pv-pool     │ │  │ │
│  │  └───────────────────┘    └───────────────────┘    │ └─────────────┘ │  │ │
│  │                                                     └────────┬────────┘  │ │
│  └─────────────────────────────────────────────────────────────┼───────────┘ │
│                                                                 │             │
│                                                                 ▼             │
│  ┌─────────────────────────────────────────────────────────────────────────┐ │
│  │                           Data Pipeline                                  │ │
│  │                                                                          │ │
│  │    pv-deployer (1회 실행)                                                │ │
│  │    ├── prefect_flows/deploy.py                                          │ │
│  │    └── Flow 등록: daily-weather-collection, full-etl                    │ │
│  │                                                                          │ │
│  │    pv-worker (상시 실행)                                                 │ │
│  │    ├── daily-weather-collection-flow (매일 09:00)                       │ │
│  │    │   ├── collect_asos.py → 기상 데이터 수집                           │ │
│  │    │   ├── impute_missing.py → 결측치 처리                              │ │
│  │    │   └── merge_to_all.py → CSV 병합                                   │ │
│  │    │                                                                     │ │
│  │    └── full-etl-flow (수동)                                             │ │
│  │        └── 기상 + PV 전체 ETL                                           │ │
│  └─────────────────────────────────────────────────────────────────────────┘ │
│                                          │                                    │
│                                          ▼                                    │
│  ┌─────────────────────────────────────────────────────────────────────────┐ │
│  │                           Data Storage                                   │ │
│  │  ┌───────────────────────────────┐    ┌──────────────────────────────┐  │ │
│  │  │        pv-postgres            │    │        pv-grafana            │  │ │
│  │  │                               │    │                              │  │ │
│  │  │  ┌─────────────────────────┐  │    │  ┌────────────────────────┐  │  │ │
│  │  │  │ Tables:                 │  │◄───│  │ Dashboards:            │  │  │ │
│  │  │  │ - pv_generation         │  │    │  │ - PV Dashboard         │  │  │ │
│  │  │  │ - weather_data          │  │    │  │ - Geomap               │  │  │ │
│  │  │  │ - plant_info            │  │    │  │ - Time Series          │  │  │ │
│  │  │  └─────────────────────────┘  │    │  └────────────────────────┘  │  │ │
│  │  │                               │    │                              │  │ │
│  │  │  Port: 5434 (외부)            │    │  Port: 3003 (외부)           │  │ │
│  │  └───────────────────────────────┘    └──────────────────────────────┘  │ │
│  └─────────────────────────────────────────────────────────────────────────┘ │
└──────────────────────────────────────────────────────────────────────────────┘
```

## 컨테이너 상세 설명

### 1. prefect-postgres-new
- **목적**: Prefect 서버의 백엔드 데이터베이스
- **저장 데이터**:
  - Flow 실행 상태 및 결과
  - 스케줄 정보
  - Deployment 메타데이터
  - 실행 로그
- **접근**: 내부 네트워크만 (외부 노출 X)

### 2. prefect-server-new
- **목적**: Prefect 오케스트레이션 서버
- **기능**:
  - Web UI 제공 (http://localhost:4300)
  - REST API 제공
  - 스케줄 관리
  - Flow 실행 모니터링
- **의존성**: prefect-postgres-new

### 3. pv-deployer
- **목적**: Flow 배포 (1회성)
- **동작**:
  1. 컨테이너 시작
  2. Prefect API 연결 대기
  3. Work Pool 생성 (`pv-pool`)
  4. Flow Deployment 등록
  5. 종료
- **배포하는 Flow**:
  - `daily-weather-collection`: 매일 09:00
  - `full-etl`: 수동 실행

### 4. pv-worker
- **목적**: 실제 Flow 실행
- **동작**:
  - Docker 타입 Worker로 실행
  - `pv-pool` Work Pool 구독
  - 스케줄된 Flow 자동 실행
  - Docker 소켓 마운트로 컨테이너 생성 가능
- **특징**: 상시 실행 (데몬)

### 5. pv-postgres
- **목적**: PV/기상 데이터 저장
- **테이블**:
  - `pv_generation`: 발전량 데이터
  - `plant_info`: 발전소 정보 (위치 포함)
  - `weather_data`: 기상 관측 데이터
- **접근**: localhost:5434

### 6. pv-grafana
- **목적**: 데이터 시각화
- **대시보드**:
  - PV 발전량 시계열 차트
  - 발전소 위치 Geomap
  - 기상 데이터 차트
- **데이터소스**: pv-postgres
- **접근**: http://localhost:3003

## 데이터 흐름

### 기상 데이터 수집 (자동화)
```
1. pv-worker가 daily-weather-collection-flow 실행 (매일 09:00)
2. collect_asos.py: 기상청 ASOS API 호출
3. impute_missing.py: 결측치 보간
4. merge_to_all.py: 통합 CSV 병합
5. PostgreSQL 저장
```

### PV 데이터 수집 (수동/로컬)
```
남부발전:
1. nambu_probe_date.py: 발전소별 데이터 시작일 탐색
2. nambu_bulk_sync.py: API 호출 → pv_data_raw/
3. nambu_merge_pv_data.py: 전처리 → pv_data_processed/
4. initial_db_ingestion.py: PostgreSQL 적재

남동발전:
1. namdong_collect_pv.py: CSV 다운로드 → pv_data_raw/
2. namdong_merge_pv_data.py: long-format 변환 → pv_data/
3. PostgreSQL 적재 (예정)
```

## 네트워크

```
Network: pv-pipeline_prefect (bridge)

┌─────────────────────────────────────────────────────────────┐
│                                                             │
│  prefect-postgres-new ◄──► prefect-server-new              │
│                              ▲                              │
│                              │                              │
│                        pv-deployer                          │
│                        pv-worker                            │
│                              │                              │
│                              ▼                              │
│  pv-postgres ◄────────────────────────────► pv-grafana     │
│                                                             │
└─────────────────────────────────────────────────────────────┘

외부 노출 포트:
- 3003: Grafana UI
- 4300: Prefect UI
- 5434: PostgreSQL (개발용)
```

## 볼륨

| 볼륨명 | 컨테이너 | 용도 |
|--------|---------|------|
| postgres_data_new | prefect-postgres-new | Prefect 메타데이터 |
| pv_data | pv-postgres | PV/기상 데이터 |
| grafana_data | pv-grafana | 대시보드 설정 |

## 환경 변수

### pv-deployer / pv-worker
```
PREFECT_API_URL=http://prefect-server-new:4200/api
PV_DATABASE_URL=postgresql+psycopg2://pv:pv@pv-db:5432/pv
SERVICE_KEY=<기상청 API 키>
TZ=Asia/Seoul
```

## 확장 계획

### Weather 전용 컨테이너 (향후)
기상 데이터 전용 처리가 필요한 경우:
- `weather-collector`: 기상 데이터 수집 전용
- `weather-postgres`: 기상 데이터 DB 분리
- 컨테이너명에 `weather-` 접두사 사용

### PV 스케줄 자동화 (향후)
- `pv-nambu-collector`: 남부발전 일일 자동 수집
- `pv-namdong-collector`: 남동발전 월별 백필
