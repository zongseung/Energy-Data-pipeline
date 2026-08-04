# Nambu Core State Refactor Design

## Goal

Make the daily and backfill Nambu PV collectors read collection state from
the active `plants` and `generation` schema instead of the deleted
`nambu_generation` table.

## Current Problem

Both collectors write new rows through `upsert_generation()` into the core
schema. Their target and completeness queries were not migrated:

- `nambu_collect.py` reads targets, latest timestamps, and hourly completeness
  from `nambu_generation`.
- `nambu_backfill.py` repeats equivalent target and completeness SQL against
  `nambu_generation`.
- Production contains 18 Nambu solar plants in `plants` and their rows in
  `generation`; `nambu_generation` no longer exists.

This mismatch makes the scheduled `daily-nambu-pv-collection` fail before it
can call the API.

## Design

Create `fetch_data/pv/nambu_state.py` as the single state-query module for
Nambu collectors. It will provide:

- `get_nambu_targets(engine, gencd=None, hogi=None)`: read `plant_id`,
  `plant_code`, `unit_no`, `plant_name`, and the latest generation timestamp
  from `plants LEFT JOIN generation`, restricted to `operator='nambu'` and
  `fuel_type='solar'`.
- `count_hours_for_day(engine, plant_id, day)`: count distinct hours in
  `generation` for one plant and day.
- `find_incomplete_days(engine, plant_id, start, end)`: return dates with fewer
  than 24 hourly rows.

The daily collector will retain its existing rules: skip plants whose latest
data is before 2025, retry an incomplete latest day, otherwise start on the
next day, and stop at yesterday. The backfill CLI will retain its filters and
date range behavior while calling the shared state functions.

No database migration or data copy is required.

## Runtime Cleanup

After the Nambu flow succeeds:

1. Rebuild `pv-pipeline:latest` and rerun `pv-deployer`.
2. Run `daily-nambu-pv-collection` once and verify completion.
3. Delete the obsolete Prefect deployment `daily-namdong-pv-collection`, whose
   deleted entrypoint causes a daily failure. The current
   `monthly-namdong-pv-collection` remains active.

## Testing

- Unit-test start-date selection for complete, incomplete, empty, and inactive
  targets.
- Test that both Nambu collectors import the shared state module and contain no
  `nambu_generation` SQL.
- Run the existing test suite.
- Query the production DB through the shared module and verify 18 targets are
  discovered before redeployment.
- Verify the triggered Prefect Nambu run reaches `Completed`.

## Scope

This change does not delete `pv_test`, the legacy root compose file, or ORM
model modules. Those still have migration or initialization references and
will be considered in a separate deletion pass after live collection is
stable.
