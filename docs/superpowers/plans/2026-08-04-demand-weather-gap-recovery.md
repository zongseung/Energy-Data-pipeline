# Demand and Weather Gap Recovery Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Backfill nationwide demand and weather data through the latest available interval and keep it current with one 10-minute deployment in the active Prefect stack.

**Architecture:** Reuse the stopped weather pipeline's proven KPX parser and hourly join behavior, but write through the current project's synchronous SQLAlchemy configuration. The current Prefect `pv-pool` owns collection, hourly repair, materialized-view refresh, and restart catch-up; the old Prefect stack remains stopped.

**Tech Stack:** Python 3.10+, aiohttp, pandas, SQLAlchemy, PostgreSQL 14, Prefect 2, pytest, Docker Compose

## Global Constraints

- Resolve weather credentials as `SERVICE_KEY` first, then `NAMDONG_WIND_KEY`.
- Use only `DEMAND_DB_URL`; do not add a second database variable or driver dependency.
- Preserve `demand_5min(timestamp)` and `demand_weather_1h(timestamp, station_name)` upsert identities.
- Never fabricate weather, demand, Jeju, or SMP measurements.
- Keep `/mnt/nvme/weather-pipeline` Prefect server and worker stopped.
- Do not modify unrelated dirty files or legacy migrations.
- Use the existing `pv-pool`, `pv-pipeline:latest` image, and Asia/Seoul schedules.

---

### Task 1: Weather Credential Fallback

**Files:**
- Modify: `fetch_data/common/config.py`
- Modify: `fetch_data/weather/asos_collect.py`
- Modify: `prefect_flows/deploy.py`
- Create: `tests/test_weather_key.py`

**Interfaces:**
- Produces: `get_service_key() -> str` with `SERVICE_KEY` precedence.
- Produces: weather collection that raises before HTTP access when both keys are blank.
- Produces: flow-run job variables with the resolved value in `SERVICE_KEY`.

- [ ] **Step 1: Write failing credential tests**

```python
import asyncio

import pytest

from fetch_data.common.config import get_service_key
from fetch_data.weather import asos_collect


def test_service_key_takes_precedence(monkeypatch):
    monkeypatch.setenv("SERVICE_KEY", "primary")
    monkeypatch.setenv("NAMDONG_WIND_KEY", "fallback")
    assert get_service_key() == "primary"


def test_namdong_key_is_weather_fallback(monkeypatch):
    monkeypatch.delenv("SERVICE_KEY", raising=False)
    monkeypatch.setenv("NAMDONG_WIND_KEY", "fallback")
    assert get_service_key() == "fallback"


def test_blank_weather_keys_return_blank(monkeypatch):
    monkeypatch.delenv("SERVICE_KEY", raising=False)
    monkeypatch.delenv("NAMDONG_WIND_KEY", raising=False)
    assert get_service_key() == ""


def test_weather_collection_fails_before_http_without_key(monkeypatch):
    monkeypatch.delenv("SERVICE_KEY", raising=False)
    monkeypatch.delenv("NAMDONG_WIND_KEY", raising=False)
    with pytest.raises(RuntimeError, match="SERVICE_KEY.*NAMDONG_WIND_KEY"):
        asyncio.run(asos_collect.select_data_async([], "20260803", "20260803"))
```

- [ ] **Step 2: Run the tests to verify RED**

Run: `uv run pytest tests/test_weather_key.py -q`

Expected: the fallback test fails because `get_service_key()` reads only `SERVICE_KEY`.

- [ ] **Step 3: Implement the fallback and early validation**

Use this exact resolver:

```python
def get_service_key() -> str:
    return os.getenv("SERVICE_KEY") or os.getenv("NAMDONG_WIND_KEY", "")
```

In `select_data_async()`, raise `RuntimeError` when `get_service_key()` is blank.
Pass the resolved key into `fetch_city()` instead of relying on a stale
module-import value. In `prefect_flows/deploy.py`, define
`NAMDONG_WIND_KEY` first and set `SERVICE_KEY = os.getenv("SERVICE_KEY") or
NAMDONG_WIND_KEY` before building job variables.

- [ ] **Step 4: Verify GREEN**

Run: `uv run pytest tests/test_weather_key.py tests/test_refactoring.py -q`

Expected: all selected tests pass.

### Task 2: Demand Persistence and KPX Collection

**Files:**
- Create: `fetch_data/demand/__init__.py`
- Create: `fetch_data/demand/database.py`
- Create: `fetch_data/demand/collect.py`
- Create: `tests/test_demand_collection.py`

**Interfaces:**
- Produces: `get_demand_engine(db_url: str | None = None) -> Engine`.
- Produces: `get_last_5min_timestamp(engine) -> datetime | None`.
- Produces: `get_collection_start(last_ts: datetime | None, now: datetime, recent_hours: int = 1) -> date`.
- Produces: `collect_latest(engine, now: datetime | None = None) -> Awaitable[int]`.
- Produces: `upsert_demand_5min(engine, records: list[dict]) -> int`.

- [ ] **Step 1: Write failing boundary and parser tests**

```python
from datetime import date, datetime

import asyncio
import pandas as pd
import pytest

from fetch_data.demand.collect import get_collection_start, prepare_records
from fetch_data.demand.database import Demand5Min, DemandWeather1H


def test_gap_collection_starts_on_last_database_day():
    now = datetime(2026, 8, 4, 16, 0)
    assert get_collection_start(datetime(2026, 8, 2, 6, 55), now) == date(2026, 8, 2)


def test_current_collection_uses_recent_window_day():
    now = datetime(2026, 8, 4, 0, 20)
    assert get_collection_start(datetime(2026, 8, 4, 0, 15), now) == date(2026, 8, 3)


def test_prepare_records_maps_kpx_columns():
    rows = prepare_records(pd.DataFrame([{
        "기준일시": "2026-08-04 10:00:00",
        "현재수요(MW)": 70000.0,
        "공급능력(MW)": 90000.0,
        "최대예측수요(MW)": 71000.0,
        "공급예비력(MW)": 20000.0,
        "공급예비율(%)": 28.5,
        "운영예비력(MW)": 9000.0,
    }]))
    assert rows[0]["timestamp"] == datetime(2026, 8, 4, 10, 0)
    assert rows[0]["current_demand"] == 70000.0


def test_database_upsert_identities_are_unique():
    demand_indexes = {tuple(column.name for column in index.columns)
                      for index in Demand5Min.__table__.indexes if index.unique}
    hourly_indexes = {tuple(column.name for column in index.columns)
                      for index in DemandWeather1H.__table__.indexes if index.unique}
    assert ("timestamp",) in demand_indexes
    assert ("timestamp", "station_name") in hourly_indexes


def test_empty_requested_range_fails(monkeypatch):
    from fetch_data.demand import collect

    async def empty_download(*args, **kwargs):
        return pd.DataFrame()

    monkeypatch.setattr(collect, "download_range", empty_download)
    with pytest.raises(RuntimeError, match="수집된 전력수요 데이터가 없습니다"):
        asyncio.run(
            collect.collect_range(object(), date(2026, 8, 3), date(2026, 8, 3))
        )
```

- [ ] **Step 2: Run tests to verify RED**

Run: `uv run pytest tests/test_demand_collection.py -q`

Expected: collection fails because `fetch_data.demand` does not exist.

- [ ] **Step 3: Add the minimal database module**

Define SQLAlchemy models matching the production columns and unique indexes.
`get_demand_engine()` reads `DEMAND_DB_URL`, defaulting to
`postgresql+psycopg2://demand:demand@demand-postgres:5432/demand`. Implement
PostgreSQL `insert(...).on_conflict_do_update()` in batches and expose only the
interfaces listed above.

- [ ] **Step 4: Port the active KPX download path**

Port `request_with_retry()`, date-based CSV download, EUC-KR decode, column
mapping, holiday/day-type calculation, and record preparation from
`/mnt/nvme/weather-pipeline/fetch_data/demand/collect_demand.py`. Exclude its
interactive CLI, output-file mode, CSV legacy load, and unused compatibility
functions. `collect_range()` must raise when the requested non-empty range
returns no rows; `collect_latest()` derives its start from the database maximum
and ends on `now.date()`.

- [ ] **Step 5: Verify GREEN**

Run: `uv run pytest tests/test_demand_collection.py -q`

Expected: all demand collection tests pass without a live DB or network.

### Task 3: Hourly Repair and Materialized Views

**Files:**
- Create: `fetch_data/demand/aggregate.py`
- Create: `tests/test_demand_aggregate.py`
- Modify: `fetch_data/demand/database.py`

**Interfaces:**
- Produces: `get_recovery_start(latest: datetime | None, first_unknown: datetime | None, fallback: datetime) -> datetime`.
- Produces: `get_common_end(last_complete_demand_hour: datetime | None, latest_weather: datetime | None) -> datetime | None` using an exclusive end.
- Produces: `aggregate_demand_weather(engine, weather_csv: Path, recover: bool = False, now: datetime | None = None) -> int`.
- Produces: `remove_repaired_unknowns(engine, start: datetime, end: datetime) -> int`.
- Produces: `refresh_demand_views(engine) -> None`.

- [ ] **Step 1: Write failing aggregation-boundary tests**

```python
from datetime import datetime

from unittest.mock import MagicMock

from fetch_data.demand.aggregate import (
    get_common_end,
    get_recovery_start,
    remove_repaired_unknowns,
)


def test_recovery_starts_at_earliest_unknown():
    assert get_recovery_start(
        datetime(2026, 8, 2, 6),
        datetime(2026, 1, 6, 0),
        datetime(2026, 8, 2, 7),
    ) == datetime(2026, 1, 6, 0)


def test_common_end_uses_earlier_complete_source():
    assert get_common_end(
        datetime(2026, 8, 4, 15),
        datetime(2026, 8, 3, 23),
    ) == datetime(2026, 8, 4, 0)


def test_common_end_is_none_without_weather():
    assert get_common_end(datetime(2026, 8, 4, 15), None) is None


def test_unknown_cleanup_requires_real_station_rows():
    engine = MagicMock()
    connection = engine.begin.return_value.__enter__.return_value
    connection.execute.return_value.rowcount = 3
    removed = remove_repaired_unknowns(
        engine, datetime(2026, 8, 1), datetime(2026, 8, 2)
    )
    sql = str(connection.execute.call_args.args[0])
    assert "station_name = 'UNKNOWN'" in sql
    assert "station_name <> 'UNKNOWN'" in sql
    assert removed == 3
```

- [ ] **Step 2: Run tests to verify RED**

Run: `uv run pytest tests/test_demand_aggregate.py -q`

Expected: collection fails because `fetch_data.demand.aggregate` does not exist.

- [ ] **Step 3: Implement full-hour aggregation**

Query complete demand hours with `HAVING COUNT(*) >= 12`. Read only
`date`, `station_name`, `temperature`, and `humidity` from the merged CSV,
normalize `date` to hourly timestamps, and inner-join complete demand hours.
Upsert real station rows in 3,000-row batches. For normal operation use
`floor(now, hour) - 48 hours`; for recovery use the earlier of the first
`UNKNOWN` timestamp and one hour after the current maximum.

- [ ] **Step 4: Remove placeholders and refresh views**

After real rows are committed, execute a scoped delete:

```sql
DELETE FROM demand_weather_1h AS old
WHERE old.station_name = 'UNKNOWN'
  AND old.timestamp >= :start
  AND old.timestamp < :end
  AND EXISTS (
      SELECT 1 FROM demand_weather_1h AS real
      WHERE real.timestamp = old.timestamp
        AND real.station_name <> 'UNKNOWN'
  )
```

Then run `REFRESH MATERIALIZED VIEW mv_latest_weather` and
`REFRESH MATERIALIZED VIEW mv_hourly_national`. Let either SQL error propagate.

- [ ] **Step 5: Verify GREEN**

Run: `uv run pytest tests/test_demand_aggregate.py tests/test_demand_collection.py -q`

Expected: all demand unit tests pass.

### Task 4: Prefect Ownership and SMP Staleness

**Files:**
- Create: `prefect_flows/demand_flow.py`
- Modify: `prefect_flows/deploy.py`
- Modify: `prefect_flows/smp_flow.py`
- Create: `tests/test_demand_deployment.py`
- Create: `tests/test_smp_realtime_empty.py`

**Interfaces:**
- Produces: `unified_demand_collection_flow(force_hourly: bool = False) -> dict[str, int]`.
- Produces: deployment `unified-demand-collection` with cron `*/10 * * * *` in `Asia/Seoul`.
- Produces: `run_smp_realtime_task()` that raises when the source inserts zero rows.

- [ ] **Step 1: Write failing orchestration tests**

```python
import ast
from pathlib import Path


def test_unified_demand_deployment_is_every_ten_minutes():
    tree = ast.parse(Path("prefect_flows/deploy.py").read_text())
    fn = next(n for n in tree.body if isinstance(n, ast.AsyncFunctionDef)
              and n.name == "deploy_unified_demand_flow")
    values = [k.value.value for n in ast.walk(fn) if isinstance(n, ast.Call)
              and getattr(n.func, "id", None) == "CronSchedule"
              for k in n.keywords if k.arg == "cron"]
    assert values == ["*/10 * * * *"]
```

```python
import pytest
from prefect_flows import smp_flow


def test_realtime_smp_zero_rows_is_stale_failure(monkeypatch):
    monkeypatch.setattr(smp_flow, "run_realtime_collection", lambda: 0)
    with pytest.raises(RuntimeError, match="원천 데이터가 비어"):
        smp_flow.run_smp_realtime_task.fn()
```

- [ ] **Step 2: Run tests to verify RED**

Run: `uv run pytest tests/test_demand_deployment.py tests/test_smp_realtime_empty.py -q`

Expected: demand deployment is absent and SMP zero rows do not raise.

- [ ] **Step 3: Add the unified flow**

The async demand task awaits `collect_latest()`. The synchronous hourly task
runs when `force_hourly` is true or the run minute is below 10. Hourly execution
calls `aggregate_demand_weather()`, then refreshes both materialized views. A
failure is re-raised after the existing Slack failure notification pattern.

- [ ] **Step 4: Register the existing-pool deployment**

Add `deploy_unified_demand_flow()` using `pv-pipeline:latest`, `pv-pool`,
`get_job_variables()`, `CronSchedule(cron="*/10 * * * *",
timezone="Asia/Seoul")`, and default `force_hourly=False`. Call it from
`main()` and include it in the printed deployment inventory.

- [ ] **Step 5: Make empty realtime SMP visible**

Immediately after `run_realtime_collection()`, raise
`RuntimeError("제주 실시간 SMP 원천 데이터가 비어 있습니다")` when the count is
zero. Keep existing retries and failure notification behavior.

- [ ] **Step 6: Verify GREEN and run the full suite**

Run:

```bash
uv run pytest tests/test_demand_deployment.py tests/test_smp_realtime_empty.py -q
uv run pytest tests -q
```

Expected: all tests pass.

### Task 5: Production Backfill and Operational Verification

**Files:**
- Update: `ISCSI_DOCKER_RECOVERY.md`
- Runtime data copy: `data/asos_all_merged.csv`

**Interfaces:**
- Consumes: existing Prefect API on `http://127.0.0.1:4400/api`.
- Consumes: production `demand-postgres` and existing Jeju deployments.
- Produces: current demand/weather/Jeju data and one active 10-minute demand deployment.

- [ ] **Step 1: Copy the existing ASOS history once**

Copy `/mnt/nvme/weather-pipeline/data/asos_all_merged.csv` to
`data/asos_all_merged.csv`. Compare byte size and maximum `date`; do not copy or
start any old Prefect database, server, or worker.

```bash
cp /mnt/nvme/weather-pipeline/data/asos_all_merged.csv data/asos_all_merged.csv
ls -l /mnt/nvme/weather-pipeline/data/asos_all_merged.csv data/asos_all_merged.csv
```

- [ ] **Step 2: Build and redeploy the active image**

Run:

```bash
docker compose -f docker/docker-compose.yml build pv-deployer
docker compose -f docker/docker-compose.yml up -d pv-deployer
```

Expected: `pv-deployer` exits 0 and `unified-demand-collection` is active.

- [ ] **Step 3: Recover missing ASOS dates**

Read `data/asos_all_merged.csv`, calculate absent calendar dates from the first
legacy `UNKNOWN` day through yesterday, and trigger the existing
`daily-weather-collection` deployment once per absent date in ascending order.
Expected initial trailing dates are `2026-08-01` through `2026-08-03`; use the
calculated set rather than hard-coding it. Every run must complete before the
next starts.

After the calculated set confirms these three audit-baseline gaps, run:

```bash
PREFECT_API_URL=http://127.0.0.1:4400/api uv run prefect deployment run \
  'daily-weather-collection-flow/daily-weather-collection' \
  --param target_date=20260801 --watch
PREFECT_API_URL=http://127.0.0.1:4400/api uv run prefect deployment run \
  'daily-weather-collection-flow/daily-weather-collection' \
  --param target_date=20260802 --watch
PREFECT_API_URL=http://127.0.0.1:4400/api uv run prefect deployment run \
  'daily-weather-collection-flow/daily-weather-collection' \
  --param target_date=20260803 --watch
```

- [ ] **Step 4: Trigger nationwide recovery**

Trigger `unified-demand-collection` with `force_hourly=true`. Wait for a terminal
state, rerun once if an upstream retry exhausts, then query table maxima and
counts. Confirm real station rows replaced repairable `UNKNOWN` rows.

```bash
PREFECT_API_URL=http://127.0.0.1:4400/api uv run prefect deployment run \
  'unified-demand-collection-flow/unified-demand-collection' \
  --param force_hourly=true --watch
```

- [ ] **Step 5: Recover the Jeju missing day**

Trigger `jeju-sukub-monthly-collection` with `target_month="2026-08"`, then
trigger `jeju-supply-demand-db-sync`. Confirm `jeju_supply_demand` has rows on
`2026-08-03` and still advances on the current day.

```bash
PREFECT_API_URL=http://127.0.0.1:4400/api uv run prefect deployment run \
  'jeju-sukub-monthly-collection/jeju-sukub-monthly-collection' \
  --param target_month=2026-08 --watch
PREFECT_API_URL=http://127.0.0.1:4400/api uv run prefect deployment run \
  'jeju-supply-demand-db-sync/jeju-supply-demand-db-sync' --watch
```

- [ ] **Step 6: Verify views, schedules, and stopped legacy stack**

Confirm both materialized-view maxima match the latest recoverable hourly data,
the 10-minute deployment has a completed run, and the old weather Prefect server
and worker remain stopped. Confirm zero-row realtime SMP is now failed/stale and
that no placeholder price rows were inserted.

- [ ] **Step 7: Update the runbook and run final checks**

Document ownership, key fallback, restart diagnostics, backfill commands, table
freshness SQL, materialized-view refresh behavior, and SMP source staleness.
Run:

```bash
git diff --check
uv run pytest tests -q
docker compose -f docker/docker-compose.yml ps
```

Expected: no whitespace errors, all tests pass, and active infrastructure is
healthy. Do not stage unrelated dirty files.
