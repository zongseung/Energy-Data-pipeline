# Demand and Weather Gap Recovery Design

## Goal

Recover the known demand and weather gaps through the latest source-available
time, then keep nationwide demand and hourly demand-weather data current from
the existing Prefect stack.

The weather API credential resolves as `SERVICE_KEY` first and
`NAMDONG_WIND_KEY` second. No second Prefect server or worker stack is started.

## Current State

- `demand_5min` stops at `2026-08-02 06:55`.
- `demand_weather_1h` stops at `2026-08-02 06:00`.
- `jeju_supply_demand` has no rows for `2026-08-03`, while realtime rows resume
  on `2026-08-04`.
- `mv_latest_weather` and `mv_hourly_national` stop at
  `2026-01-05 23:00`; no active code refreshes them.
- The current `daily-weather-collection` deployment fails because
  `SERVICE_KEY` is blank, although `NAMDONG_WIND_KEY` is available.
- The old `/mnt/nvme/weather-pipeline` Prefect server and worker are stopped.
  Its `unified-demand-collection-flow` previously owned `demand_5min` and
  `demand_weather_1h`.
- The realtime Jeju SMP source currently returns publication placeholders, not
  numeric prices. The collector therefore cannot legitimately fill the gap
  after `2026-05-31`.

The timestamps above are the audit baseline, not hard-coded recovery bounds.
Implementation always derives the start boundary from the production database
and stops at the latest complete interval supplied by each upstream source.

## Selected Approach

Move only the nationwide demand collection path needed at runtime into this
repository and deploy it to the existing `pv-pool`. Reuse the proven KPX
download, upsert, hourly aggregation, and demand table definitions from the
stopped weather pipeline, trimming file-based legacy paths that are not needed
by the active flow.

This keeps one Prefect control plane and one operational deployment inventory.
Restarting the old stack is rejected because it restores duplicate orchestration
and makes ownership ambiguous. A one-time-only backfill is rejected because the
tables would become stale again.

## Components

### Credential Resolution

`fetch_data.common.config.get_service_key()` returns the first non-empty value
from `SERVICE_KEY` and `NAMDONG_WIND_KEY`. The deployment script uses the same
rule before constructing job variables, so the resolved value reaches every
flow-run container as `SERVICE_KEY`.

If neither variable exists, weather collection raises a configuration error
before making an API request. Logs identify both accepted variable names but do
not print the credential.

### Nationwide Demand Collector

The active collector writes directly to the existing demand PostgreSQL database
through `DEMAND_DB_URL`. The collector converts only the SQLAlchemy driver part
of that URL from `postgresql+psycopg2` to `postgresql+asyncpg` for its async
session; host, port, credentials, and database name remain unchanged. It does
not introduce a second demand database variable. It preserves the current
uniqueness contract:

- `demand_5min`: one row per `timestamp`.
- `demand_weather_1h`: one row per `(timestamp, station_name)`.

The one-time recovery starts from the latest stored five-minute timestamp and
downloads through the latest range available from KPX. It overlaps the boundary
day deliberately because PostgreSQL upsert makes reprocessing safe and the KPX
endpoint is date based.

The recurring flow runs every 10 minutes and normally re-requests the previous
hour. If the latest database timestamp is older than that window, the request
starts on the calendar day containing that timestamp; otherwise it starts one
hour before the run time. This makes restart recovery automatic without
repeatedly downloading the full history.

### Hourly Demand and Weather Aggregation

At the first 10-minute run of each hour, the flow aggregates complete
`demand_5min` hours and joins them to the merged ASOS weather CSV. Recovery
starts one hour after the latest stored `demand_weather_1h` timestamp and ends
at the earlier of:

- the latest complete demand hour; and
- the latest available weather hour.

The process does not fabricate weather rows. If weather is temporarily behind,
the hourly boundary remains pending and is retried after daily weather recovery.
Existing rows are updated by the same `(timestamp, station_name)` upsert.

After an hourly write, the flow refreshes `mv_latest_weather` and
`mv_hourly_national`. A refresh failure fails the flow so dashboards cannot look
healthy while remaining stale.

### Daily Weather Recovery

The existing daily ASOS flow continues to collect the previous day at 09:00
Asia/Seoul. Before normal scheduling is considered restored, recovery reads the
merged ASOS CSV `date` column and builds the expected calendar-date set from the
day after the latest `demand_weather_1h` timestamp through yesterday. Dates with
no ASOS rows are collected in ascending order with the resolved key. Existing
daily files are merged idempotently, so rerunning a boundary date is safe.

The weather recovery completes before the historical hourly demand-weather
aggregation, ensuring the join has source data for the whole recoverable range.

### Jeju Supply-Demand Recovery

The existing Jeju monthly collector is run for the affected month containing
`2026-08-03`, then `jeju-supply-demand-db-sync` upserts the regenerated CSV into
`jeju_supply_demand`. The recurring five-minute collector and ten-minute DB sync
remain the owners of new realtime data.

No new Jeju deployment is added.

### Realtime Jeju SMP Visibility

`daily-smp-realtime-jeju` treats a zero-row result as a failed or explicitly
stale run instead of reporting `Completed`. It does not insert placeholders or
interpolate prices. Recovery remains pending until the upstream source publishes
numeric confirmed values or another authoritative source is selected.

## Data Flow

1. Resolve and validate the weather key.
2. Backfill missing daily ASOS files through yesterday.
3. Backfill `demand_5min` from its database boundary through the latest KPX
   range.
4. Backfill `demand_weather_1h` only through the common complete demand/weather
   boundary.
5. Refresh `mv_latest_weather` and `mv_hourly_national`.
6. Recollect the affected Jeju month and sync it to `jeju_supply_demand`.
7. Deploy and execute the nationwide 10-minute flow in `pv-pool`.
8. Leave the old weather-pipeline Prefect stack stopped.

## Failure Semantics

- Missing credentials fail before network access.
- HTTP and transient network errors use the existing bounded retry policy.
- A non-empty requested historical range that yields no source rows fails the
  task; zero rows are not silently reported as success.
- Database writes remain transactional and use existing unique-key upserts.
- Backfill stops on the first failed date or interval. A rerun resumes safely
  from the persisted database/file boundary.
- Materialized-view refresh failure fails the hourly flow.
- Source-unavailable SMP data remains absent and visible as stale.

## Verification

Automated tests cover:

- `SERVICE_KEY` precedence and `NAMDONG_WIND_KEY` fallback;
- failure when both weather keys are blank;
- database-derived backfill boundaries and recent-window behavior;
- idempotent upsert identity for both demand tables;
- empty-source-result failure;
- hourly aggregation stopping at the common complete boundary; and
- the 10-minute Prefect schedule and deployment entry point.

Production verification records row counts and maximum timestamps before and
after recovery. It confirms:

- `demand_5min` reaches the latest KPX five-minute interval;
- `demand_weather_1h` reaches the latest common complete hour;
- `jeju_supply_demand` contains rows for `2026-08-03`;
- both materialized views advance beyond their audit baseline;
- the new deployment is active and its first scheduled run completes; and
- the old weather-pipeline Prefect containers remain stopped.

## Operational Documentation

Update `ISCSI_DOCKER_RECOVERY.md` with:

- the single active Prefect ownership model;
- the 10-minute nationwide demand deployment;
- weather-key fallback behavior;
- post-restart checks for demand table and materialized-view freshness; and
- the distinction between a collector failure and upstream SMP unavailability.

## Rollback

Pause or delete the nationwide demand deployment from the current Prefect server
if it misbehaves. Because writes are idempotent upserts, no data rollback is
required. The old weather-pipeline stack stays stopped unless a separate,
explicit rollback decision is made.

## Out of Scope

- Fabricating or interpolating missing upstream measurements.
- Restarting the old weather-pipeline Prefect server or worker.
- Rewriting unrelated legacy migrations, README sections, or wind table setup.
- Replacing the KPX or ASOS upstream APIs.
