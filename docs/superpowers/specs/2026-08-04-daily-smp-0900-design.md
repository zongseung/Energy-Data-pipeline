# Daily SMP 09:00 Schedule Design

## Goal

Run `daily-smp-collection` every day at 09:00 KST and collect the previous
day's day-ahead SMP data.

## Existing Behavior

- Prefect currently schedules the deployment at 06:00 KST.
- `run_smp_collection()` already calculates its issue date as
  `date.today() - timedelta(days=1)`.
- The date-selection behavior therefore needs no production-code change.

## Change

1. Change the Prefect `CronSchedule` for `daily-smp-collection` from
   `0 6 * * *` to `0 9 * * *` with timezone `Asia/Seoul`.
2. Update the SMP flow description and deployment summary to say 09:00.
3. Add a small AST-based test that verifies the cron expression in
   `deploy_smp_flow` without importing Prefect or contacting the server.
4. Rebuild the pipeline image and rerun the deployer so Prefect receives the
   updated schedule.

## Verification

- The focused schedule test passes.
- Existing tests pass.
- Prefect reports `daily-smp-collection` as active with cron `0 9 * * *` and
  timezone `Asia/Seoul`.
- The collector continues to use the previous day as its issue date.

## Scope

`daily-smp-realtime-jeju` remains at 19:00. Monthly aggregates, legacy sync,
and other collection schedules are unchanged.
