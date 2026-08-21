# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Energy-Data-pipeline is an ETL system for collecting, processing, and visualizing solar photovoltaic (PV) power generation data from two South Korean power companies (남부발전/Nambu and 남동발전/Namdong) along with weather data (ASOS). Orchestrated with Prefect 2, data is stored in PostgreSQL. 조회는 연구원 직접 SQL 과 LLM 데모(docker/llm-demo, LibreChat+MCP)가 담당한다.

The codebase and all comments/docs are primarily in Korean.

## Build & Run Commands

```bash
# Start all services (PostgreSQL, Prefect server/worker, deployer)
docker compose -f docker/docker-compose.yml up -d --build

# Install dependencies locally with uv
uv sync

# Run a specific script locally
uv run python fetch_data/pv/nambu_backfill.py

# Register Prefect deployments manually
uv run python prefect_flows/deploy.py
```

Tests: `uv run pytest` (`pyproject.toml`의 `[tool.pytest.ini_options]`, CI: `.github/workflows/ci.yml`). lint 설정은 없다.

## Architecture

### Data Sources & Pipelines

Three independent data pipelines, each on its own schedule:

1. **Nambu (남부발전)** - Daily at 09:30 KST
   - API-based collection (`fetch_data/pv/nambu_collect.py`) via public data portal (B552520)
   - Raw data: wide format with 24-hour columns (qhorgen01-24)
   - 적재: `plants`/`generation` 코어 테이블 (generation_core)

2. **Namdong (남동발전)** - Monthly on 10th at 10:00 KST
   - CSV-based collection (`fetch_data/pv/namdong_collect.py`) from koenergy.kr
   - Plant locations hardcoded in `database.py` (`NAMDONG_PLANT_LOCATIONS` dict)
   - 적재: `plants`/`generation` 코어 테이블 (generation_core)

3. **Weather (기상)** - Daily at 09:00 KST
   - ASOS API async collection (`fetch_data/weather/collect_asos.py`) for 41 stations
   - Output: CSV files merged into `data/asos_all_merged.csv`

### Orchestration (Prefect 2)

- **Deployment registration**: `pv-deployer` container runs `prefect_flows/deploy.py` once at startup to register all flows/schedules with the Prefect server
- **Execution**: `pv-worker` subscribes to `pv-pool` work pool and executes scheduled flow runs
- **Flows defined in**: `prefect_flows/*_flow.py` 및 `prefect_flows/prefect_pipeline.py` — 등록 목록은 `deploy.py`의 `DEPLOYMENTS`가 정본
- **Deploy config**: `prefect_flows/deploy.py` creates a Docker-type work pool (`pv-pool`) and registers all deployments

### Network Topology

운영 스택은 `docker/docker-compose.yml` 하나다. Prefect 서버·워커·DB 를
모두 포함하며 `pv-pipeline-network` 브리지 네트워크를 만든다. (과거 루트에 있던
docker-compose.yml 은 외부 Prefect 네트워크를 쓰는 구 스택이었고 2026-08 에 제거했다.)

### Key Services (docker/docker-compose.yml)

| Service | Container | Port | Purpose |
|---------|-----------|------|---------|
| pv-db | pv-data-postgres | 5436 | PV data storage (iSCSI) |
| pv-prefect-server | pv-prefect-server | 4400 | Prefect API/UI |
| pv-worker | pv-pipeline-worker | - | Prefect docker worker |
| pv-deployer | pv-deployer | - | One-shot deployment registrar |

### Database Access

발전량 적재는 `plants`/`generation` 코어 테이블 직접 쓰기다 (`fetch_data/common/generation_core.py`). 구 `pv_nambu`/`pv_namdong`/`wind_*` 테이블·모델은 2026-08 스키마 리팩터(P6)로 DROP·제거됐고, `fetch_data/pv/database.py` 에는 남동 좌표 상수만 남아 있다. DB URL 해석은 `fetch_data/common/db_utils.py:resolve_db_url()` 단일 경로다 (`DB_URL` 우선, 호스트 실행 시 pv-db→localhost 치환). Inside containers, `DB_URL` is set via docker-compose.

### Notifications

Slack 알림은 단일 경로다: `notify/slack_notifier.py` (수집기가 직접 사용)
→ `prefect_flows/notify_tasks.py` (@task 래핑, flow 들이 사용).
`SLACK_WEBHOOK_URL` 환경변수를 읽는다.

## Key Environment Variables

- `DB_URL` / `LOCAL_DB_URL` / `PV_DATABASE_URL` - PostgreSQL connection string (multiple vars used in different contexts)
- `PREFECT_API_URL` - Prefect server endpoint
- `SERVICE_KEY` - Public data portal API key (URL-encoded)
- `SLACK_WEBHOOK_URL` - Slack notifications
- `NAMDONG_*` - Namdong collection parameters (start date, org number, hoki range, output dir)

## Tech Stack

- **Python 3.10+** (containers use 3.11-slim)
- **Prefect 2.14+** (not v3) for orchestration
- **SQLAlchemy 2.0** with sync engine (psycopg2-binary)
- **Pandas** for data transformation
- **aiohttp/asyncpg** for async API calls
- **SciPy** for spline interpolation in missing value imputation
- **uv** as package manager (pyproject.toml, no setup.py)
- **Docker Compose** for deployment

## Backfill

Manual backfill for Nambu data uses `fetch_data/pv/nambu_backfill.py` with configurable date ranges. 
