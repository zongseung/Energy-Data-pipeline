# Energy-Data-pipeline

대한민국 발전·전력 데이터를 수집·전처리·적재하고 Grafana로 시각화하는 ETL 파이프라인입니다.
Prefect 2로 오케스트레이션하고 PostgreSQL에 저장합니다.

**수집 도메인**
- **태양광(PV)** — 남부발전(API), 남동발전(koenergy.kr 스크래핑)
- **풍력(Wind)** — 남동발전(공공API), 서부·한경(CSV 적재)
- **비태양광(KOEN gen)** — 남동발전 해양소수력·연료전지·화력(koenergy.kr)
- **기상(Weather)** — 기상청 ASOS
- **SMP(계통한계가격)** — KPX 하루전/실시간 + EPSIS 가중평균(육지/제주)
- **제주(Jeju)** — 계통수급 실시간·수급 월별·연료원별 거래량·시간별 수요

코드·주석·문서는 한국어가 기본입니다.

---

## 디렉터리 구조

```
Energy-Data-pipeline/
├── fetch_data/                         # 수집·변환 코드 (소스별 패키지)
│   ├── common/                         # 공통 인프라
│   │   ├── db_base.py                  #   ★ 엔진/세션 단일 팩토리 (get_engine/get_session)
│   │   ├── db_utils.py                 #   resolve_db_url (컨테이너/호스트 자동 전환)
│   │   ├── config.py · logger.py · utils.py · date_utils.py
│   │   └── impute_missing.py           #   결측치 보간(스플라인+이력평균)
│   ├── weather/  asos_collect.py       # ASOS 기상 수집
│   ├── pv/
│   │   ├── nambu_collect.py            # 남부 PV 일일 수집(라이브)
│   │   ├── nambu_backfill.py           # 남부 PV 과거 백필(수동)
│   │   ├── nambu_bulk_sync.py          # 남부 PV 대량 일괄 수집(standalone)
│   │   ├── nambu_transform.py          # 남부 wide→long 변환
│   │   ├── nambu_probe.py              # 남부 최초 데이터일 탐지
│   │   ├── namdong_collect.py          # 남동 PV 수집(koenergy 스크래핑)
│   │   ├── namdong_transform.py        # 남동 PV 변환
│   │   └── database.py                 # PV 테이블 모델
│   ├── wind/
│   │   ├── namdong_collect.py          # 남동 풍력(API + CSV 백필)
│   │   ├── seobu_backfill.py           # 서부 풍력(CSV)
│   │   ├── hangyoung_backfill.py       # 한경 풍력(CSV)
│   │   └── database.py
│   ├── gen/                            # KOEN 비태양광(해양소수력/연료전지/화력)
│   │   ├── namdong_collect.py · transform_gen.py · pipeline.py · load_gen.py
│   │   └── capacities.py · locations.py
│   ├── smp/
│   │   ├── smp_scraper.py · smp_collect.py · smp_aggregate.py · smp_realtime.py
│   │   ├── smp_backfill.py · legacy_sync.py · _common.py · database.py
│   └── jeju/
│       └── jeju_realtime_collect.py · jeju_sukub_collect.py · jeju_gen_collect.py · jeju_demand_collect.py
│
├── prefect_flows/                      # Prefect flow 래퍼 (수집기엔 @flow 없음)
│   ├── deploy.py                       # 모든 deployment/스케줄 등록
│   ├── prefect_pipeline.py             # 기상 / full-etl
│   ├── nambu_pv_flow.py · namdong_pv_flow.py · namdong_wind_flow.py
│   ├── smp_flow.py · gen_flow.py · jeju_flow.py
│   └── notify_tasks.py · merge_to_all.py
│
├── config/                             # 추적되는 설정 파일
│   ├── station_list.csv                #   ASOS 지점 목록
│   └── plant.json                      #   남부 gencd → 발전소명 매핑
├── inputs/wind/                        # 풍력 백필 원본 CSV (gitignore)
├── scripts/
│   ├── backup_pv_db.sh · restore_pv_db.sh   # DB 백업/복원 (→ NAS)
│   ├── init_wind_tables.py             # 풍력 테이블 초기화(compose wind-init)
│   └── migrations/                     # 일회성·기록용 (직접 실행 안 함)
│       ├── schema_migration.py         #   plants/generation 코어 마이그레이션
│       └── *.sql                       #   dual-write 트리거 등
│
├── docker/                             # ★ 운영 스택 (정본)
│   ├── docker-compose.yml · Dockerfile
│   └── grafana/                        # provisioning + 대시보드
├── notify/slack_notifier.py            # Slack Webhook 알림
├── Makefile · pyproject.toml · uv.lock · .env
└── ARCHITECTURE.md · README.md
```

> **네이밍 규약**: 수집기 파일명은 역할 동사로 통일합니다 — `*_collect`(라이브 수집) · `*_backfill`(일회성/이력) · `*_transform`(wide→long 변환) · `*_probe`(보조 탐지).
> **레이어 규칙**: `@flow`는 `prefect_flows/`에만 두고, 수집기는 단일 진입점 `run(...)`을 노출합니다.

---

## 운영 스택 (docker/)

실제 운영은 `docker/docker-compose.yml` 스택을 사용합니다 (`Makefile` 기준).

| 컨테이너 | 역할 | 포트(host) |
|---|---|---|
| **pv-data-postgres** | 메인 데이터 DB (PV·풍력·SMP·gen·plants·generation) | `5436` |
| **pv-pipeline-grafana** | 대시보드 | `3006` |
| **pv-prefect-server** | Prefect 오케스트레이션 | `4400` |
| **pv-prefect-postgres** | Prefect 메타DB | 내부 |
| **pv-pipeline-worker** | Docker 워크풀(`pv-pool`) 워커 — flow run 컨테이너 기동 | - |
| **pv-deployer** | `pv-pipeline:latest` 빌드 + `deploy.py` 1회 실행 | - |

- 호스트에서 메인 DB 접속: `postgresql+psycopg2://pv:pv@localhost:5436/pv`
- 컨테이너 내부에선 호스트명 `pv-db`(=pv-data-postgres). `resolve_db_url`이 환경을 자동 전환합니다.

```bash
make up        # docker compose -f docker/docker-compose.yml up -d
make rebuild   # 이미지 재빌드 + deployer 재실행 (코드/스케줄 변경 반영)
make logs-worker
make ps
make db        # psql 접속
```

> 별도의 루트 `docker-compose.yml`(pv-main-db:5432·pv-main-grafana:3002)도 존재하나, 이는 옛 스택입니다. 운영은 `docker/` 스택을 기준으로 하세요.

---

## Prefect Flows & 스케줄

`pv-deployer`가 `prefect_flows/deploy.py`로 아래 deployment를 등록합니다 (KST).

| Deployment | 스케줄 | 소스 flow |
|---|---|---|
| `daily-weather-collection` | 매일 09:00 | prefect_pipeline |
| `daily-nambu-pv-collection` | 매일 09:30 | nambu_pv_flow |
| `monthly-namdong-pv-collection` | 매월 10일 10:00 | namdong_pv_flow |
| `monthly-namdong-wind-collection` | 매월 10일 11:00 | namdong_wind_flow |
| `monthly-koen-gen-collection` | 매월 10일 | gen_flow |
| `daily-smp-collection` | 매일 06:00 | smp_flow |
| `monthly-smp-aggregate` | 매월 2일 07:00 | smp_flow |
| `daily-smp-realtime-jeju` | 매일 19:00 | smp_flow |
| `weekly-smp-legacy-sync` | 매주 월 07:00 | smp_flow |
| `jeju-realtime-collection` | 매 5분 | jeju_flow |
| `jeju-sukub-monthly-collection` | 매월 1일 01:00 | jeju_flow |
| `jeju-gen-monthly-collection` | 매월 1일 02:00 | jeju_flow |
| `jeju-demand-quarterly-collection` | 분기 1일 03:00 | jeju_flow |
| `full-etl` | 수동 | prefect_pipeline |

---

## 데이터베이스 구조

메인 DB: **`pv-data-postgres`** (host `localhost:5436`, 컨테이너 `pv-db:5432`, db `pv`). 총 12개 테이블이 **2계층**으로 구성됩니다.

### 계층 모델: 소스별 수집 테이블 → 통합 코어 (dual-write 트리거)

```
수집기 ─INSERT→  nambu_generation ───┐
                 namdong_generation  ├─[AFTER INSERT 트리거]→ generation  (plant_id 자동해소, source='api')
                 wind_namdong/seobu/hangyoung ─┘                ▲
                                                    plants ─FK──┘   (백필 적재분은 source='backfill')
```

수집기는 **발전사별 raw 테이블**에 적재하고, 5개 트리거가 `plant_id`를 해소해 **통합 `generation`** 으로 미러링합니다(없는 발전소는 `plants`에 자동 등록).

### 통합 코어 (정규화 목표)

**`plants`** — 발전소 마스터 (87행). 좌표·용량·연료원·운영사의 단일 진실 원천.

| 컬럼 | 타입 | 비고 |
|---|---|---|
| `plant_id` | serial PK | |
| `plant_name`, `unit_no` | varchar | **UNIQUE(plant_name, unit_no)** |
| `plant_code` | varchar | 외부코드(예: nambu gencd) |
| `fuel_type` | varchar | solar · wind · hydro · thermal · fuel_cell |
| `operator` | varchar | nambu · namdong · seobu · hangyoung |
| `region` | varchar | mainland · jeju |
| `capacity_mw`, `capacity_confidence` | double·varchar | 용량 / 신뢰도(확실·근사·불확실) |
| `lat`,`lon`,`address`,`site_name` | | 위치 |
| `install_angle`,`module_spec`,`inverter_spec` | | PV 전용 스펙 |

> 현재 분포: nambu solar 18 · namdong solar 23 / wind 5 / thermal 24 / fuel_cell 8 / hydro 4 · seobu wind 4 · hangyoung wind 1.

**`generation`** — 시간별 발전량 통합 (약 **344만행**).

| 컬럼 | 타입 | 비고 |
|---|---|---|
| `timestamp`, `plant_id` | timestamp·int | **PK (timestamp, plant_id)** · plant_id→`plants` FK |
| `gen_kwh` | double | 단위 kWh |
| `source` | varchar | `api`(라이브 트리거 ~1.3만) / `backfill`(이력 ~342만) |

- 인덱스: `(plant_id, timestamp DESC)`, **BRIN**(timestamp)
- **`v_generation_hourly`** 뷰: `generation ⋈ plants` (외부/FDW 노출용 — timestamp·plant_name·unit_no·fuel_type·operator·region·lat·lon·gen_kwh)

### 소스별 수집 테이블 (트리거로 `generation` 미러링)

| 테이블 | 행수 | 주요 컬럼 | 트리거 |
|---|---:|---|---|
| `nambu_generation` | 856K | datetime·gencd·hogi·plant_name·generation·daily_*(레거시 집계) | `dualwrite_nambu` |
| `namdong_generation` | 872K | datetime·plant_name·**hour**·generation | `dualwrite_namdong` |
| `wind_namdong` | 172K | timestamp·plant_name·generation · uniq(ts,plant) | `dualwrite_wind_namdong` |
| `wind_seobu` / `wind_hangyoung` | — | 〃 (+ capacity_mw) | `dualwrite_wind_*` |
| `nambu_plants` / `namdong_plants` | 0 | 레거시 메타(현재 미사용) | - |

> ⚠️ raw 테이블은 발전사별로 스키마가 제각각(시간 컬럼 `datetime`/`timestamp`, namdong은 별도 `hour`, nambu는 daily 집계 컬럼)이라 통합 `generation`이 정규화 레이어 역할을 합니다.

### SMP 테이블 (독립 — 트리거 없음)

단위 원/kWh, 시각 KST 구간시작, `region` = land / jeju / unified(2010년 이전 단일가격).

| 테이블 | 행수 | 컬럼 (유니크키) |
|---|---:|---|
| `smp_hourly` | 364K | timestamp · region · price — **uniq(timestamp, region)** |
| `smp_weighted_avg` | 16K | period_type(daily/monthly/yearly) · period · region · price_type(smp/blmp) · weighted_avg — **uniq(4컬럼)** |
| `smp_realtime_jeju` | 79K | timestamp(15분) · region · price · is_confirmed(D+1 확정) — **uniq(timestamp, region)** |

> SMP 적재 시 `smp_data/<table>.csv`로 자동 미러링됩니다.

### 외부 연동
- 이 DB는 논리복제 `pub_all`(generation·plants·smp 등)을 발행합니다.
- **Energy-hub**(:5437, 제주 디지털 트윈)가 FDW로 generation/plants/smp를 소비합니다. demand-postgres(:5433, 전국 수요)·energy-hub-db(:5437)는 별도 프로젝트 스택입니다.

---

## 실행 / 수동 수집

```bash
uv sync                                   # 의존성 설치

# 운영 스택 기동
make up

# 수집기 수동 실행 (호스트, .env 로드 필요)
uv run python -m fetch_data.smp.smp_collect            # SMP 시간별 + 일별 가중평균
uv run python -m fetch_data.smp.smp_aggregate --period all
uv run python -m fetch_data.smp.smp_realtime --backfill # 제주 실시간 과거 일괄

# 남부 PV 백필 (Grafana 3006이 보는 메인 DB로 적재)
uv run python fetch_data/pv/nambu_backfill.py \
  --db-url "postgresql+psycopg2://pv:pv@localhost:5436/pv"
  # 옵션: --start --end --gencd --hogi --slack --debug

# 풍력 테이블 초기화 + CSV 백필
uv run python scripts/init_wind_tables.py

# DB 백업 / 복원 (→ NAS)
scripts/backup_pv_db.sh
scripts/restore_pv_db.sh <백업파일>
```

> 테스트/린트 설정은 없습니다.

---

## 환경 변수 (`.env`)

| 변수 | 용도 |
|---|---|
| `LOCAL_DB_URL` / `PV_DATABASE_URL` / `DB_URL` | PostgreSQL 접속 (호스트/컨테이너) |
| `PREFECT_API_URL` | Prefect 서버 |
| `NAMBU_API_KEY` | 공공데이터포털(남부발전/기상) 키 |
| `NAMDONG_WIND_KEY` | 남동 풍력 공공API 키 |
| `SLACK_WEBHOOK_URL` | Slack 알림 |
| `SMP_LEGACY_DB_URL` | SMP 개인 DB 백업(미설정 시 skip) |
| `NAMDONG_*` | 남동 수집 파라미터(시작일·org·hoki·출력경로) |

---

## 트러블슈팅

1. **호스트에서 `pv-db` DNS를 못 찾음** → 스크립트에 `--db-url`을 `localhost:5436`으로 지정 (또는 `resolve_db_url`이 자동 전환).
2. **Grafana(3006) “No data”** → Grafana가 보는 메인 DB(`localhost:5436`)에 적재했는지 확인.
3. **Prefect 배포는 됐는데 실행 안 됨** → `docker logs -f pv-pipeline-worker`로 워커가 `pv-pool` 구독 중인지 확인. (루트스택 `pv-worker`는 docker.sock 없는 옛 워커이므로 띄우지 말 것.)
4. **koenergy.kr SSL 오류** → 중간 인증서 누락 사이트로, 수집기가 `get_koen_ssl_context`로 체인을 보충합니다.
5. **코드/스케줄 변경 반영** → `make rebuild` (flow는 `pv-pipeline:latest` 이미지로 실행되므로 재빌드 필요).

자세한 아키텍처는 `ARCHITECTURE.md` 참고.
