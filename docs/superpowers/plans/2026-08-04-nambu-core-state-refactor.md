# Nambu Core State Refactor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Restore daily and backfill Nambu PV collection by replacing every `nambu_generation` state query with shared queries against `plants` and `generation`.

**Architecture:** Add one focused state-query module used by both collectors. Preserve API calls, transformation, filtering, and core UPSERT behavior; only target discovery and completeness checks move to the active schema.

**Tech Stack:** Python 3.10+, SQLAlchemy, PostgreSQL, pytest, Prefect 2, Docker Compose

## Global Constraints

- Do not create, restore, or write `nambu_generation`.
- Preserve the current inactive-plant cutoff: latest data before 2025 is skipped.
- Preserve incomplete-day handling: fewer than 24 hourly rows causes same-day retry.
- Do not delete legacy files or ORM modules in this pass.
- Work in the live `cleanup` branch without reverting unrelated user changes.

---

### Task 1: Specify Core State Behavior

**Files:**
- Create: `tests/test_nambu_state.py`
- Create later: `fetch_data/pv/nambu_state.py`

**Interfaces:**
- Produces: `collection_start(last_dt: datetime | None, hours: int, today: date) -> datetime | None`.
- Produces: state module source with no `nambu_generation` reference.

- [ ] **Step 1: Write failing behavior tests**

```python
from datetime import date, datetime
from pathlib import Path

from fetch_data.pv.nambu_state import collection_start


def test_retries_incomplete_latest_day():
    assert collection_start(datetime(2026, 8, 2, 23), 23, date(2026, 8, 4)) == datetime(2026, 8, 2)


def test_starts_after_complete_latest_day():
    assert collection_start(datetime(2026, 8, 2, 23), 24, date(2026, 8, 4)) == datetime(2026, 8, 3)


def test_skips_inactive_legacy_plant():
    assert collection_start(datetime(2023, 10, 20), 24, date(2026, 8, 4)) is None


def test_new_plant_defaults_to_one_year_back():
    assert collection_start(None, 0, date(2026, 8, 4)) == datetime(2025, 8, 4)


def test_collectors_do_not_query_deleted_nambu_table():
    for path in ("fetch_data/pv/nambu_collect.py", "fetch_data/pv/nambu_backfill.py"):
        assert "nambu_generation" not in Path(path).read_text(encoding="utf-8")
```

- [ ] **Step 2: Verify RED**

Run: `uv run pytest tests/test_nambu_state.py -q`

Expected: collection fails because `fetch_data.pv.nambu_state` does not exist.

### Task 2: Add the Shared Core State Module

**Files:**
- Create: `fetch_data/pv/nambu_state.py`
- Test: `tests/test_nambu_state.py`

**Interfaces:**
- `get_nambu_targets(engine, gencd: str | None = None, hogi: int | None = None) -> list[dict]`
- `count_hours_for_day(engine, plant_id: int, day: date) -> int`
- `find_incomplete_days(engine, plant_id: int, start: date, end: date) -> list[date]`
- `collection_start(last_dt: datetime | None, hours: int, today: date) -> datetime | None`

- [ ] **Step 1: Implement the minimal state module**

Use a `plants LEFT JOIN generation` query filtered by `operator='nambu'` and
`fuel_type='solar'`. Return dictionaries containing `plant_id`, `gencd`,
integer `hogi`, `plant_name`, and `last_dt`. Count completeness by
`generation.plant_id` and `generation.timestamp`.

- [ ] **Step 2: Run the pure behavior tests**

Run: `uv run pytest tests/test_nambu_state.py -q`

Expected: only the source-level legacy-reference test remains failing because
the callers have not been migrated yet.

### Task 3: Migrate Daily and Backfill Callers

**Files:**
- Modify: `fetch_data/pv/nambu_collect.py`
- Modify: `fetch_data/pv/nambu_backfill.py`
- Modify: `prefect_flows/nambu_pv_flow.py`
- Test: `tests/test_nambu_state.py`

**Interfaces:**
- Consumes all four functions from `fetch_data.pv.nambu_state`.
- Preserves `solar_automation_flow()` and the backfill CLI interface.

- [ ] **Step 1: Replace daily state queries**

Remove `_count_hours_for_day()` and its legacy SQL. Make
`get_active_targets()` iterate over `get_nambu_targets()`, calculate hours by
`plant_id`, and call `collection_start()`.

- [ ] **Step 2: Replace backfill state queries**

Remove `_get_targets()`, `_find_incomplete_days()`, and `_iter_dates()`. Use
`get_nambu_targets()` in `main()` and `find_incomplete_days()` in `backfill()`.

- [ ] **Step 3: Update descriptions and errors**

Replace `nambu_generation` wording with `plants/generation`; do not change API
or UPSERT behavior.

- [ ] **Step 4: Verify GREEN and run the full suite**

Run:

```bash
uv run pytest tests/test_nambu_state.py -q
uv run pytest tests -q
```

Expected: all tests pass.

### Task 4: Verify Against the Production Schema

**Files:**
- No source changes.

**Interfaces:**
- Consumes: PostgreSQL at `localhost:5436/pv`.
- Produces: 18 Nambu solar targets discovered through `get_nambu_targets()`.

- [ ] **Step 1: Run the shared query against production**

Use a short Python command with an explicit local SQLAlchemy URL and print the
target count and target codes.

Expected: 18 targets, including `B997` units 1 and 2 and `S997` units 1-3.

- [ ] **Step 2: Inspect the tested refactor diff**

Review the focused diff, but do not commit the implementation in this pass.
Both collector files already contain unrelated user changes, so staging the
whole files would mix ownership. Leave the verified implementation in the
working tree for a later user-controlled integration commit.

### Task 5: Redeploy and Exercise the Flow

**Files:**
- Update: `ISCSI_DOCKER_RECOVERY.md`

**Interfaces:**
- Consumes: `daily-nambu-pv-collection` deployment and `pv-pipeline:latest`.
- Produces: a completed Nambu flow run using the core schema.

- [ ] **Step 1: Rebuild and rerun the deployer**

```bash
docker compose -f docker/docker-compose.yml build pv-deployer
docker compose -f docker/docker-compose.yml up -d pv-deployer
```

Expected: `pv-deployer` exits 0.

- [ ] **Step 2: Trigger one Nambu deployment run**

POST an empty body to `/api/deployments/{deployment_id}/create_flow_run`, then
wait for the resulting flow run to reach a terminal state.

Expected: `Completed` without an `UndefinedTable` error.

- [ ] **Step 3: Delete the obsolete daily Namdong deployment**

Delete only `daily-namdong-pv-collection` through the Prefect API. Verify
`monthly-namdong-pv-collection` remains `READY` and active.

- [ ] **Step 4: Update and verify the runbook**

Record the Nambu core-state migration and successful flow run. Run
`git diff --check`, the full test suite, Docker Compose status, and Prefect
deployment checks.
