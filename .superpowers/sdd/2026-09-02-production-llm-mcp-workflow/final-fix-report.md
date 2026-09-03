# Final fix wave report

Base HEAD: `2e241f8cdc4fe07ac463494c65840ba61c10a068`

Status: **DONE_WITH_CONCERNS**. All fourteen findings are implemented or handled
according to their ruling, all required test/build/render/scan evidence is green,
and no live service, live container, environment, log, database, or secret value
was inspected or mutated. The final commit SHA is reported in the controller
handoff because this report is itself part of that commit.

## Finding-by-finding mapping

| # | Result | Implementation and verification |
|---|---|---|
| 1 | Fixed | `PlannerDecision.conditions` is a fixed `Condition(name, value)` array. Both Pydantic objects forbid extra properties, duplicate names are rejected, and `condition_mapping()` preserves ergonomic mapping-shaped storage/output. The regression exercises installed OpenAI SDK 2.54.0 through `httpx.MockTransport`, captures the real `/responses` request, checks its roles/payload/model/text format and exact schema, and recursively asserts `additionalProperties: false` for every object. |
| 2 | Fixed | Formal Compose and bootstrap use separate complete `LLM_APPROVAL_PUBLIC_ORIGIN` and `LLM_EXPORT_PUBLIC_ORIGIN` inputs. The approval URL appends `/approval`; the export URL preserves its complete public origin. Structural and rendered tests cover both. |
| 3 | Fixed | The image has the generic legacy-compatible export-server plus `energy-mcp` CMD and no loader entrypoint. Only formal Compose overrides the entrypoint with `load-secrets energy-mcp`; all eight legacy personal-role services continue to use the image default. |
| 4 | Fixed | Workflows carry a `revision`; clarification answers atomically claim and increment the current revision, and the planner decision is saved only against that claimed revision. A deterministic nested-call regression proves a newer snapshot wins while the stale planner fails closed, without breaking clarification loops. |
| 5 | Fixed | Planner/refusal exceptions and planned-SQL validation failures use a status/revision/expiry-guarded failure transition. The one cached Mongo collection initialization performs a one-time reconciliation of pre-existing `executing` rows to an uncertain failed state, so they are never rerun. |
| 6 | Fixed | SQL hash integrity now binds display, CSRF issuance, confirmation/decline, execution claim, and the final pre-`_execute` check. Any mismatch is hidden or failed closed and requires a new workflow. Tests assert the exact SHA fields and Mongo filters. |
| 7 | Fixed | Every loader profile reads and validates all required files before exporting any variable or running a command. Empty and newline-only values are rejected without printing them. Mongo initialization preloads and validates all three passwords before its first authentication/user mutation. |
| 8 | Fixed with repository-path deviation | Root `README.md` and the repository's existing GitBook pages (`docs/gitbook/README.md`, `01-architecture.md`, and `03-llm-mcp.md`) now distinguish formal hosted/shared `demo_ro` operation from legacy personal-role stdio, state that IP allowlisting and principal-to-role mapping are deferred, explain returning to chat and saying approval is complete, and state formal preview 10 versus legacy maximum 10,000 rows. The requested `docs/llm/01-architecture.md` and `03-operator-guide.md` paths do not exist; the authoritative existing GitBook files were updated rather than creating a second documentation tree. |
| 9 | Fixed | Public tool descriptions state that planning does not execute SQL and that execution requires browser approval followed by returning to chat. Assertions use public `workflow_mcp.list_tools()`. |
| 10 | Fixed | Source-mode tests use clean-checkout-reproducible constraints: owner executable and not world writable. The Dockerfile's explicit in-image `chmod 0555` remains asserted. |
| 11 | Fixed | Mongo is created with timezone-aware decoding, and approval expiry is explicitly labeled UTC. |
| 12 | Fixed | Execution records `executed_at`, monotonic measured `duration_ms`, row count, and error code without result rows. Reconciled uncertain executions use a null duration. |
| 13 | Preserved | Internal `plan_with_openai(..., model=None)` remains; production passes no model argument, while the MCP-exposed `plan_query` schema has no `model` parameter. |
| 14 | No product change | Kept outside product behavior as ruled; the report records the deferred boundary without introducing speculative implementation. |

## TDD evidence

1. Baseline focused MCP tests: `43 passed in 1.26s`.
2. Finding 1 RED: planner suite `2 failed, 1 passed in 1.58s`; failures
   demonstrated the old dictionary schema and missing duplicate-name rejection.
3. Finding 1 GREEN: planner suite `3 passed in 1.53s`.
4. Findings 4/5/6/9/11/12 RED: workflow/approval/modes suites
   `26 failed, 26 passed in 1.61s`, covering the absent revision/CAS, guarded
   failure, hash binding, integrity rejection, audit fields, UTC handling,
   timezone-aware startup recovery, and tool descriptions.
5. MCP focused GREEN: `55 passed in 2.07s`.
6. Findings 2/3/7 RED: configuration suite `7 failed, 15 passed in 0.77s`,
   covering empty secrets, pre-mutation Mongo validation, split origins,
   formal-only loader startup, generic image CMD, and both Compose contracts.
7. Findings 2/3/7 GREEN: configuration suite `22 passed in 0.86s`; related
   shell and JavaScript syntax checks exited zero.
8. Finding 8 RED: documentation suite `4 failed, 7 passed`; failures identified
   stale role/boundary/approval/row-limit claims.
9. Finding 8 GREEN: documentation suite `11 passed in 0.03s`.

`uv run pytest` was attempted once but the sandbox could not write the global uv
cache (`Read-only file system`). The already-synchronized project virtual
environments were used without dependency or network changes.

## Final verification

- MCP full suite: `68 passed in 2.10s`.
- Root full suite: `196 passed, 2 skipped, 8 warnings in 4.55s`.
  Warnings are existing Prefect, Starlette, and Pydantic deprecations.
- Syntax: `sh -n` passed for `load-secrets.sh`, `bootstrap-mongo.sh`, and
  `provision-secrets.sh`; `node --check` passed for `init-mongo-users.js` and
  `mongo-healthcheck.js`.
- Formal and legacy Compose both rendered successfully with
  `--env-file /dev/null`, complete dummy public origins, and disposable
  empty/dummy files; no service was started.
- `git diff --check` passed before staging.

## Image build and value-safe scans

- Built only `energy-mcp`: `llm-demo-energy-mcp`, image
  `sha256:d82658e652627e04e20bbda4dc13aa4eb85af294d0174626c0b348360c0f7f1d`.
  BuildKit completed provenance resolution; pinned bases shown by the build were
  `ghcr.io/astral-sh/uv:0.8.17@sha256:e4644c...` and
  `python:3.11-slim@sha256:9534e5...`.
- Image metadata confirms `entrypoint=null` and the generic CMD
  `mkdir -p /exports && python /serve_exports.py & exec energy-mcp`.
- Provenance label count: 3; names only were
  `com.docker.compose.project`, `com.docker.compose.service`, and
  `com.docker.compose.version`.
- Sensitive image environment-name count: 1, `GPG_KEY`, inherited public base
  image metadata; no secret value was displayed.
- Image-history sensitive-literal line count: 0.
- Tracked high-confidence secret-file count: 0.
- Formal rendered-config high-confidence secret-literal line count: 0.
- Legacy rendered-config high-confidence secret-literal line count: 0.

All scans emitted counts or metadata names only and did not print candidate
secret values. No image container was run.

## Rulings, deviations, and concerns

- IP/Tailscale allowlisting and authenticated principal-to-database-role mapping
  remain explicitly deferred. The formal hosted deployment therefore shares
  `demo_ro`; the legacy stdio Compose retains personal roles.
- Legacy `run_sql` behavior remains intact. Internal planner model selection is
  retained but is not MCP-exposed, and production supplies none.
- Mongo behavior is verified with offline fakes only, as required by the
  no-live-database rule.
- The exact disposable directory `/tmp/energy-llm-final-fix-secrets` contained
  empty files only. Its cleanup command was interrupted by the controller before
  completion could be verified; no value-bearing artifact was created.
