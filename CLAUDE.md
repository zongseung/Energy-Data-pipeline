# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Energy-Data-pipeline is an ETL system for collecting, processing, and visualizing solar photovoltaic (PV) power generation data from two South Korean power companies (남부발전/Nambu and 남동발전/Namdong) along with weather data (ASOS). Orchestrated with Prefect 2, data is stored in PostgreSQL and visualized via Grafana.

The codebase and all comments/docs are primarily in Korean.

## Build & Run Commands

```bash
# Start all services (PostgreSQL, Grafana, Prefect worker, deployer)
docker-compose up -d --build

# Start standalone Prefect stack (includes its own Prefect server)
docker-compose -f docker/docker-compose.yml up -d --build

# Install dependencies locally with uv
uv sync

# Run a specific script locally
uv run python fetch_data/pv/nambu_backfill.py

# Initialize database tables
uv run python fetch_data/pv/database.py

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
   - Preprocessing (`nambu_merge_pv_data.py`): wide-to-long melt transformation
   - DB table: `pv_nambu` with unique constraint on (timestamp, plant_id, hogi)

2. **Namdong (남동발전)** - Monthly on 10th at 10:00 KST
   - CSV-based collection (`fetch_data/pv/namdong_collect_pv.py`) from koenergy.kr
   - Plant locations hardcoded in `database.py` (`NAMDONG_PLANT_LOCATIONS` dict)
   - DB table: `pv_namdong` with unique constraint on (timestamp, plant_name)

3. **Weather (기상)** - Daily at 09:00 KST
   - ASOS API async collection (`fetch_data/weather/collect_asos.py`) for 41 stations
   - Missing value imputation via spline interpolation + historical averages (`fetch_data/common/impute_missing.py`)
   - Output: CSV files merged into `data/asos_all_merged.csv`

### Orchestration (Prefect 2)

- **Deployment registration**: `pv-deploy` container runs `prefect_flows/deploy.py` once at startup to register all flows/schedules with the Prefect server
- **Execution**: `pv-worker` subscribes to `pv-pool` work pool and executes scheduled flow runs
- **Flows defined in**: `prefect_flows/prefect_pipeline.py` (weather), `prefect_flows/nambu_pv_flow.py` (Nambu wrapper), `fetch_data/pv/namdong_collect_pv.py` (Namdong flow)
- **Deploy config**: `prefect_flows/deploy.py` creates a Docker-type work pool (`pv-pool`) and registers 4 deployments

### Network Topology

The main `docker-compose.yml` does **not** include a Prefect server. The PV pipeline containers join an external Prefect network (`weather-pipeline_prefect-new`) to communicate with a separately running Prefect server. See `ARCHITECTURE.md` for details.

### Key Services (docker-compose.yml)

| Service | Image | Port | Purpose |
|---------|-------|------|---------|
| pv-db | postgres:15 | 5432 | PV data storage |
| pv-grafana | grafana/grafana | 3002 | Dashboard UI |
| pv-worker | custom (Dockerfile) | - | Prefect worker |
| pv-deploy | custom (Dockerfile) | - | One-shot deployment registrar |

### Database Models (`fetch_data/pv/database.py`)

Four SQLAlchemy ORM tables: `pv_nambu`, `pv_namdong`, `plant_info_nambu`, `plant_info_namdong`. The module uses a global engine singleton pattern (`get_engine()`/`get_session()`). DB URL 해석은 `fetch_data/common/db_utils.py:resolve_db_url()` 단일 경로다 (`DB_URL` 우선, 호스트 실행 시 pv-db→localhost 치환). Inside containers, `DB_URL` is set via docker-compose.

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
