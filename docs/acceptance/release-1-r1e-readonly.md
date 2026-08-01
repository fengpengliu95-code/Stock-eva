# Release 1 R1-E: HTTP GET market-storage read-only boundary

## Scope

This slice fixes the observed side effect where production HTTP GET requests changed the local
market control DuckDB. It does not deploy, access NAS, or modify a production database. All runtime
evidence in this document uses pytest temporary directories.

## Root cause

The write happened at more than one layer:

1. `get_market_store()` called `StorageLayout.ensure_local_runtime_dirs()` for every dependency
   resolution and called `NasMarketStore.reconcile_control_pointer()` for every dataset-backed GET.
2. Every `MarketStore` read method reused `_connect()`. That method creates directories, opens a
   read-write DuckDB connection, runs five `CREATE TABLE IF NOT EXISTS` statements, and runs four
   `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` migrations.
3. `get_calendar_sync_store()` constructed `CalendarSyncStore` with `initialize=True`, so
   `GET /market/status` also initialized its SQLite schema.
4. The local regime and published-snapshot readers were named read-only but opened DuckDB without
   `read_only=True`.

The production symptom (an empty `published_daily_bars` table appearing after GET) is therefore
explained by the request dependency and read-method call graph, not by the immutable dataset reader.

## Implemented ownership boundary

- HTTP market dependencies construct `MarketStore(read_only=True)` and never create runtime
  directories or reconcile a pointer.
- Ordinary `MarketStore` read methods use a schema-free reader connection. Missing files return the
  existing empty result; malformed or incompatible control schema fails closed as structured HTTP
  503 with `reason_code=market_control_read_failed`.
- Dataset manifest/hash/schema validation remains enabled through `NasMarketStore.ensure_readiness()`.
- `GET /market/status` uses `CalendarSyncStore(initialize=False)`.
- Local regime and published-snapshot readers request DuckDB `read_only=True`.
- A read-only dataset store rejects save, publication, export, scheduler-state, and reconciliation
  lifecycles before touching the dataset, staging directory, or control file.
- Schema initialization, publication, and manifest-first pointer repair remain owned by explicit
  writer stores used by CLI/ingestion/automation startup and `FullMarketHistoryService.execute()`.

DuckDB does not allow a `read_only=True` handle and an existing read-write handle for the same file
in one process. To preserve the previous committed snapshot while an in-process writer transaction
is open, the reader first requests `read_only=True` and falls back to a schema-free, SELECT-only
same-configuration handle only when DuckDB returns the exact documented same-database/different-
configuration error. Similar messages and all other open/read errors do not fall back and fail
closed. The fallback does not create directories, set a temp directory, or execute DDL.

## TDD evidence

### RED

Command:

```text
uv run --extra dev pytest -o addopts='' -q tests/test_market_get_read_only.py
```

Before production changes: `4 failed, 2 passed in 0.83s`.

The four expected failures proved that GET changed the control hash/mtime/size/schema, invoked
reconciliation, created missing runtime paths, and silently migrated a malformed schema to HTTP 200.
The explicit writer/reconciler and previous-commit MVCC baselines passed.

A later focused RED changed the concurrent reader to an independent `MarketStore(read_only=True)`;
it failed with DuckDB's same-database/different-configuration `ConnectionException`. Another RED
proved that a read-only `NasMarketStore.save_refresh()` published files before its control writer
guard rejected the request.

### GREEN coverage

`tests/test_market_get_read_only.py` covers:

- one real temporary control DuckDB plus checksum/schema-validated Parquet manifest;
- GET health, storage readiness, market summary/status/supplemental/history, security analysis,
  market regime, sector rotation, classification, fund-flow evidence, portfolio valuation, and
  strategy list;
- exact control hash, mtime, size, schema and complete dataset tree equality before/after GET;
- enabled and disabled supplemental reads without audit-database initialization;
- no reconciliation call from a GET dependency;
- missing dataset-backed and local-mode control/runtime paths remain absent;
- malformed dataset-backed and local control schema returns structured 503 without migration;
- normal local reads request DuckDB `read_only=True` and reject `_connect()` writer access;
- exact configuration mismatch fallback, similar/non-matching error fail-closed behavior;
- an independent read-only API store sees the previous committed value during an in-process writer
  transaction and the new value after commit;
- explicit local writer initialization/save and explicit dataset pointer reconciliation still work;
- read-only dataset writer lifecycles fail before any dataset, staging, or control mutation.

## Verification

Initial clean baseline at `95e70ae`:

```text
611 passed in 87.27s
```

Final focused suite:

```text
20 passed in 1.87s
```

Final full suite after documentation:

```text
623 passed in 64.99s
```

Lint:

```text
uv run --extra dev ruff check backend tests
All checks passed!
```

The repository-wide `ruff format --check backend tests` is not a clean baseline: it reports
pre-existing format differences across files outside this slice. No broad formatting rewrite was
performed. The R1-E new test and newly formatted market-store changes passed the focused format
check (`5 files already formatted`). The final `git diff --check` also passed.

## Deferred acceptance

This worker did not touch the real Application Support control database or deploy a release. A root
production acceptance pass must snapshot the installed control DuckDB and local dataset, execute the
representative GET sequence, and prove the real hash/mtime/schema/tree remain identical.
