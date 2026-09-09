# Energy-Data-pipeline

대한민국 발전·전력 데이터를 수집·전처리·적재하는 ETL 파이프라인입니다. 조회는 직접 SQL과 LibreChat+MCP 자연어 서비스로 제공합니다.
Prefect 2로 오케스트레이션하고 PostgreSQL에 저장합니다.

정식 LLM·MCP 승인 서비스는 공용 읽기전용 role `demo_ro`로 사용자가 확인한 SQL만
한 번 실행하도록 구현돼 있습니다. 다만 현재 호스팅 서비스는 아직 레거시 `run_sql`
모드이므로 SQL이 승인 화면 없이 즉시 실행됩니다. 실제 운영 모드는 GitBook의
`LLM·MCP 데이터 처리와 투명성` 페이지에서 확인합니다.

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
├── pipeline/                       # 파이프라인 본체
│   ├── fetch_data/                 #   수집기 (소스별)
│   │   ├── common/                 #     paths·db_utils·generation_core·notify·koen·logger
│   │   ├── config/                 #     station_list.csv · plant.json
│   │   ├── pv/                     #     남부·남동·EKR·동서 태양광
│   │   ├── gen/                    #     KOEN 비태양광 (해양소수력·연료전지·화력·풍력)
│   │   ├── smp/  weather/  oil/  demand/  jeju/  komipo/
│   ├── prefect_flows/              #   flow 래퍼 (수집기엔 @flow 없음)
│   │   └── deploy.py               #     모든 deployment/스케줄 등록 — 정본
│   └── tests/
│
├── ops/                            # 운영 자산
│   ├── docker/                     #   ★ 운영 스택 (docker-compose.yml · Dockerfile)
│   ├── systemd/                    #   부팅 복구 유닛
│   ├── scripts/                    #   DB 백업/복원, 일회성 유틸
│   └── sql/                        #   research 쿼리 · FDW · migrations/
│
├── llm/                            # LLM 데모
│   ├── mcp-server/                 #   energy-mcp (별도 파이썬 프로젝트)
│   └── librechat/                  #   LibreChat + nginx + PgBouncer 스택
│
├── data/                           # ★ 살아남는 유일한 경로 (컨테이너 마운트)
│   ├── asos_*.csv  oil/  komipo/  smp/  backups/
│   └── (중간 산출물은 /tmp/energy-pipeline — DB 가 정본이라 안 쌓는다)
│
├── docs/  intake/
└── Makefile · pyproject.toml · uv.lock · .env · CLAUDE.md · README.md
```

> **네이밍 규약**: 수집기 파일명은 역할 동사로 통일합니다 — `*_collect`(라이브 수집) · `*_backfill`(일회성/이력) · `*_transform`(wide→long 변환) · `*_probe`(보조 탐지).
> **레이어 규칙**: `@flow`는 `pipeline/prefect_flows/`에만 두고, 수집기는 단일 진입점 `run(...)`을 노출합니다.

---

## 운영 스택 (ops/docker/)

실제 운영은 `ops/docker/docker-compose.yml` 스택을 사용합니다 (`Makefile` 기준).

| 컨테이너 | 역할 | 포트(host) |
|---|---|---|
| **pv-data-postgres** | 메인 데이터 DB (PV·풍력·SMP·gen·plants·generation) | `5436` |
| **pv-prefect-server** | Prefect 오케스트레이션 | `4400` |
| **pv-prefect-postgres** | Prefect 메타DB | 내부 |
| **pv-pipeline-worker** | Docker 워크풀(`pv-pool`) 워커 — flow run 컨테이너 기동 | - |
| **pv-deployer** | `pv-pipeline:latest` 빌드 + `deploy.py` 1회 실행 | - |

- 호스트에서 메인 DB 접속: `postgresql+psycopg2://pv:pv@localhost:5436/pv`
- 컨테이너 내부에선 호스트명 `pv-db`(=pv-data-postgres). `resolve_db_url`이 환경을 자동 전환합니다.

```bash
make up        # docker compose -f ops/docker/docker-compose.yml up -d
make rebuild   # 이미지 재빌드 + deployer 재실행 (코드/스케줄 변경 반영)
make logs-worker
make ps
make db        # psql 접속
```

> 과거 루트에 있던 옛 `docker-compose.yml`은 2026-08 에 제거했습니다. 운영은 `ops/docker/docker-compose.yml` 스택을 기준으로 하세요.

---

## Prefect Flows & 스케줄

`pv-deployer`가 `pipeline/prefect_flows/deploy.py`로 아래 deployment를 등록합니다 (KST).

| Deployment | 스케줄 | 소스 flow |
|---|---|---|
| `daily-weather-collection` | 매일 09:00 | prefect_pipeline |
| `daily-nambu-pv-collection` | 매일 09:30 | nambu_pv_flow |
| `monthly-namdong-pv-collection` | 매월 10일 10:00 | namdong_pv_flow |
| `monthly-koen-gen-collection` | 매월 10일 | gen_flow |
| `daily-smp-collection` | 매일 09:00 (전날 데이터) | smp_flow |
| `monthly-smp-aggregate` | 매월 2일 07:00 | smp_flow |
| `daily-smp-realtime-jeju` | 매일 19:00 | smp_flow |
| `weekly-smp-legacy-sync` | 매주 월 07:00 | smp_flow |
| `jeju-realtime-collection` | 매 5분 | jeju_flow |
| `jeju-sukub-monthly-collection` | 매월 1일 01:00 | jeju_flow |
| `jeju-gen-monthly-collection` | 매월 1일 02:00 | jeju_flow |
| `jeju-demand-quarterly-collection` | 분기 1일 03:00 | jeju_flow |

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

# 남부 PV 백필 (메인 DB 5436 으로 적재)
uv run python -m pipeline.fetch_data.pv.nambu_backfill \
  --db-url "postgresql+psycopg2://pv:pv@localhost:5436/pv"
  # 옵션: --start --end --gencd --hogi --slack --debug

# 풍력 테이블 초기화 + CSV 백필

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
3. **Prefect 배포는 됐는데 실행 안 됨** → `docker logs -f pv-pipeline-worker`로 워커가 `pv-pool` 구독 중인지 확인.
4. **koenergy.kr SSL 오류** → 중간 인증서 누락 사이트로, 수집기가 `get_koen_ssl_context`로 체인을 보충합니다.
5. **코드/스케줄 변경 반영** → `make rebuild` (flow는 `pv-pipeline:latest` 이미지로 실행되므로 재빌드 필요).

자세한 아키텍처는 `ARCHITECTURE.md` 참고.
