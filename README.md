# Energy PV Data Pipeline

남부발전/남동발전 태양광(PV) 발전 데이터 및 기상 데이터를 수집, 전처리하여 PostgreSQL에 저장하고 Grafana 지도 시각화를 제공하는 파이프라인입니다..

## 시스템 아키텍처

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              Docker Compose                                  │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌──────────────────┐     ┌──────────────────┐     ┌──────────────────┐     │
│  │ prefect-postgres │     │   pv-postgres    │     │    pv-grafana    │     │
│  │    -new          │     │                  │     │                  │     │
│  │  ───────────────│     │  ───────────────│     │  ───────────────│     │
│  │  Prefect 메타DB  │     │  PV/기상 데이터  │     │  시각화 대시보드  │     │
│  │  :5432 (내부)    │     │  :5434 (외부)    │     │  :3003 (외부)    │     │
│  └────────┬─────────┘     └────────┬─────────┘     └────────┬─────────┘     │
│           │                        │                        │               │
│           │                        │                        │               │
│  ┌────────▼─────────┐              │                        │               │
│  │ prefect-server   │              │                        │               │
│  │    -new          │◄─────────────┼────────────────────────┘               │
│  │  ───────────────│              │                                        │
│  │  오케스트레이션   │              │                                        │
│  │  :4300 (외부)    │              │                                        │
│  └────────┬─────────┘              │                                        │
│           │                        │                                        │
│     ┌─────┴─────┐                  │                                        │
│     │           │                  │                                        │
│  ┌──▼───┐  ┌────▼────┐             │                                        │
│  │ pv-  │  │  pv-    │             │                                        │
│  │deploy│  │ worker  │─────────────┘                                        │
│  │ -er  │  │         │                                                      │
│  │ ─────│  │ ────────│                                                      │
│  │1회   │  │플로우    │                                                      │
│  │배포  │  │실행      │                                                      │
│  └──────┘  └─────────┘                                                      │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Docker 컨테이너 상세

| 컨테이너 | 이미지 | 포트 | 역할 |
|---------|--------|------|------|
| **prefect-postgres-new** | postgres:14 | 내부 5432 | Prefect 서버 메타데이터 저장 (플로우 상태, 스케줄, 로그) |
| **pv-postgres** | postgres:14 | 5434:5432 | PV 발전량, 기상 데이터 저장용 메인 DB |
| **pv-grafana** | grafana/grafana | 3003:3000 | 데이터 시각화 대시보드 (지도, 시계열 차트) |
| **prefect-server-new** | prefecthq/prefect:2 | 4300:4200 | Prefect 오케스트레이션 서버 (스케줄링, 모니터링 UI) |
| **pv-deployer** | pv-pipeline:latest | - | 플로우 배포 (시작 시 1회 실행 후 종료) |
| **pv-worker** | prefecthq/prefect:2 | - | Docker Worker - 스케줄된 플로우 실제 실행 |

## 데이터 흐름

### 1. 기상 데이터 (Weather)
```
기상청 ASOS API → collect_asos.py → CSV → PostgreSQL (pv-postgres)
                        ↓
              daily-weather-collection-flow (매일 09:00)
```

### 2. 남부발전 PV 데이터
```
남부발전 API → nambu_bulk_sync.py → pv_data_raw/ → nambu_merge_pv_data.py
                                                            ↓
                                              pv_data_processed/ → PostgreSQL
```

### 3. 남동발전 PV 데이터
```
남동발전 CSV → namdong_collect_pv.py → pv_data_raw/ → namdong_merge_pv_data.py
                                                            ↓
                                                  pv_data/ (NAS) → PostgreSQL
```

## 프로젝트 구조

```
Energy-Data-pipeline/
├── docker/
│   ├── docker-compose.yml       # 메인 Docker 구성
│   ├── Dockerfile               # pv-pipeline 이미지 빌드
│   └── grafana/
│       ├── provisioning/        # Grafana 자동 설정
│       │   ├── datasources/     # PostgreSQL 연결
│       │   └── dashboards/      # 대시보드 프로비저닝
│       └── dashboards/          # JSON 대시보드 파일
│
├── fetch_data/
│   ├── common/                  # 공통 유틸리티
│   │   └── impute_missing.py    # 결측치 처리
│   ├── weather/                 # 기상 데이터 수집
│   │   └── collect_asos.py      # ASOS API 수집
│   └── pv/                      # PV 데이터 수집
│       ├── nambu_probe_date.py      # 남부발전 시작일 탐색
│       ├── nambu_bulk_sync.py       # 남부발전 일괄 수집
│       ├── nambu_merge_pv_data.py   # 남부발전 전처리
│       ├── daily_pv_automation.py   # 일별 자동화
│       ├── namdong_collect_pv.py    # 남동발전 CSV 수집
│       ├── namdong_merge_pv_data.py # 남동발전 전처리
│       └── database.py              # PV DB 모델
│
├── prefect_flows/
│   ├── deploy.py                # 플로우 배포 스크립트
│   ├── prefect_pipeline.py      # 메인 플로우 정의
│   └── merge_to_all.py          # CSV 병합 유틸
│
├── pv_data/                     # 남동발전 데이터 (NAS mount)
├── pv_data_raw/                 # 원본 수집 데이터
├── pv_data_processed/           # 전처리된 데이터
│
├── initial_db_ingestion.py      # DB 초기 적재
├── .env                         # 환경변수 (API 키 등)
├── ARCHITECTURE.md              # 상세 아키텍처 문서
└── README.md
```

## 환경 변수 설정

`.env` 파일:
```bash
# API Keys
SERVICE_KEY=your_kma_api_key           # 기상청 API
NAMBU_API_KEY=your_nambu_api_key       # 남부발전 API

# Database (호스트에서 접속 시)
PV_DATABASE_URL=postgresql+psycopg2://pv:pv@localhost:5434/pv

# Slack 알림 (선택)
SLACK_WEBHOOK_URL=https://hooks.slack.com/...
```

## 실행 방법

### 1. Docker 환경 시작
```bash
cd docker
docker-compose up -d
```

### 2. 접속 정보
| 서비스 | URL | 계정 |
|--------|-----|------|
| Grafana | http://localhost:3003 | admin / admin |
| Prefect UI | http://localhost:4300 | - |
| PostgreSQL | localhost:5434 | pv / pv |

### 3. 수동 플로우 실행 (Prefect UI)
1. http://localhost:4300 접속
2. Deployments → `daily-weather-collection` 선택
3. Run → Quick Run

### 4. 로컬 스크립트 실행
```bash
# 남부발전 시작일 탐색
python fetch_data/pv/nambu_probe_date.py

# 남부발전 데이터 수집
python fetch_data/pv/nambu_bulk_sync.py

# 남부발전 전처리
python fetch_data/pv/nambu_merge_pv_data.py

# DB 적재
python initial_db_ingestion.py
```

## 스케줄

| 플로우 | 스케줄 | 설명 |
|--------|--------|------|
| daily-weather-collection | 매일 09:00 | 전날 기상 데이터 수집 |
| full-etl | 수동 | 전체 ETL (기상 + PV) |

## Grafana 대시보드

- **PV Dashboard**: 발전소별 발전량 시계열, 지도 시각화
- 데이터소스: `PV-PostgreSQL` (pv-postgres 연결)

## 로그 확인

```bash
# 전체 로그
docker-compose logs -f

# 특정 컨테이너
docker-compose logs -f pv-worker
docker-compose logs -f prefect-server-new
```

## 트러블슈팅

### Prefect Worker가 플로우를 실행하지 않음
```bash
# Worker 상태 확인
docker-compose logs pv-worker

# Work Pool 확인 (Prefect UI)
http://localhost:4300/work-pools
```

### DB 연결 오류
```bash
# PostgreSQL 상태 확인
docker-compose exec pv-postgres pg_isready -U pv
```
