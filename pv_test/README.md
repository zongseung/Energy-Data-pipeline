# PV Test Environment

남부발전 태양광(PV) 발전량 데이터 시각화를 위한 테스트 환경

## 구조

```
pv_test/
├── README.md
├── docker-compose.yml        # Grafana + SQLite 컨테이너
├── Dockerfile                # Python + SQLite 초기화
├── init_db.py                # CSV -> SQLite 변환 + 발전소 좌표 추가
├── requirements.txt          # Python 의존성
├── .env.example              # 환경변수 템플릿
├── grafana/
│   ├── provisioning/
│   │   ├── datasources/
│   │   │   └── datasources.yml   # SQLite datasource
│   │   └── dashboards/
│   │       └── dashboards.yml
│   └── dashboards/
│       └── pv_dashboard.json     # PV 대시보드 (Geomap 포함)
└── data/
    └── pv.db                     # SQLite DB (자동 생성)
```

## 기능

1. **발전량 시계열 차트**: 발전소별/시간별 발전량 추이
2. **발전소 위치 지도**: OpenStreetMap 기반 Geomap
3. **발전소별 통계**: 일별/월별 발전량 집계

## 데이터 흐름

```
south_pv_all_long.csv (발전량)
        +
한국남동발전_태양광...csv (발전소 정보/주소)
        ↓
   init_db.py (주소 -> 좌표 변환)
        ↓
    pv.db (SQLite)
        ↓
   Grafana (Geomap + 차트)
```

## 지오코딩 API 옵션

| API | 비용 | 특징 | 우선순위 |
|-----|------|------|---------|
| **VWorld** | 무료 (40,000건/일) | 국토교통부 제공, 한국 주소 최적화 | 1 |
| **Kakao** | 무료 | 한국 주소 최적화 | 2 |
| **Nominatim** | 무료, 키 불필요 | OpenStreetMap 기반 | 3 (기본) |

API 키가 없어도 **Nominatim**으로 동작합니다.

## 테이블 스키마

### pv_generation
| 컬럼 | 타입 | 설명 |
|------|------|------|
| id | INTEGER | PK |
| datetime | DATETIME | 일자+시간 |
| plant_name | TEXT | 발전소명 |
| hour | INTEGER | 시간 (1-24) |
| generation | REAL | 발전량 (KWh) |

### pv_plants
| 컬럼 | 타입 | 설명 |
|------|------|------|
| id | INTEGER | PK |
| plant_name | TEXT | 발전소명 (UNIQUE) |
| base_name | TEXT | 기준 발전소명 (매칭용) |
| address | TEXT | 주소 |
| capacity | REAL | 설비용량 |
| latitude | REAL | 위도 |
| longitude | REAL | 경도 |

## 실행 방법

### 1. 환경 변수 설정 (선택)

```bash
cd pv_test
cp .env.example .env
# VWorld 또는 Kakao API 키 입력 (선택사항)
```

### 2. Docker 실행

```bash
docker-compose up -d
```

### 3. 접속

- **Grafana**: http://localhost:3002
  - ID: `admin`
  - PW: `admin`

## 포트

| 서비스 | 포트 |
|--------|------|
| Grafana | 3002 (기존 3001과 충돌 방지) |

## 발전소명 매핑

발전량 CSV와 발전소 정보 CSV의 발전소명이 다릅니다:

| 발전량 CSV | 발전소 정보 CSV | 매핑 기준 |
|-----------|----------------|----------|
| 영흥태양광 | 영흥 태양광 1단지 | `영흥태양광` |
| 삼천포태양광#5 | 삼천포 태양광 5-1단지 | `삼천포태양광` |

`init_db.py`에서 정규화하여 자동 매핑합니다.

## 참고

- **Prefect 파이프라인 없음** (테스트용)
- **SQLite 사용** (경량화)
- **기존 PV/weather 환경과 독립적** 운영
- **Grafana Geomap**: OpenStreetMap 기반 (API 키 불필요)

## API 키 발급

- **VWorld**: https://www.vworld.kr/dev/v4dv_apikey.do
- **Kakao**: https://developers.kakao.com/
