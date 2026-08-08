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
5. The dataset-backed regime and sector readers bypassed `StoragePreflight`: missing roots,
   sentinels or manifests were returned as valid empty analysis, while a non-object manifest could
   escape the untyped parser as HTTP 500.
6. Writable manifest reconciliation selected from `published_snapshots` before any explicit writer
   schema initialization, so a legacy control database could not repair its pointer.

The production symptom (an empty `published_daily_bars` table appearing after GET) is therefore
explained by the request dependency and read-method call graph, not by the immutable dataset reader.

## Implemented ownership boundary

- HTTP market dependencies construct `MarketStore(read_only=True)` and never create runtime
  directories or reconcile a pointer.
- Ordinary `MarketStore` read methods use a schema-free reader connection. Missing files return the
  existing empty result; malformed or incompatible control schema fails closed as structured HTTP
  503 with `reason_code=market_control_read_failed`.
- Dataset manifest/hash/schema validation remains enabled through `NasMarketStore.ensure_readiness()`.
- `GET /market/status` uses `CalendarSyncStore(initialize=False)`. Its state read opens SQLite in
  URI `mode=ro` with no wait; a missing database still returns the default state without creating
  anything, while missing schema, invalid payload and an exclusive lock return structured 503
  `calendar_control_read_failed` without changing bytes, mtime or schema.
- Every dataset read validates both `DatasetSentinel` and `DatasetManifest`, then validates each
  manifest item before consulting paths or Parquet. Missing/invalid metadata maps to the same
  structured `market_storage_unavailable` 503 from direct regime and sector routes; only a fully
  valid empty manifest returns HTTP 200 with an empty analysis.
- Local regime and published-snapshot readers request DuckDB `read_only=True`.
- A read-only dataset store rejects save, publication, export, scheduler-state, and reconciliation
  lifecycles before touching the dataset, staging directory, or control file.
- `MarketStore.initialize_schema()` is an explicit writer-only operation. Writable manifest
  reconciliation calls it before reading a pointer and maps initialization failure to
  `DatasetError`; GET dependencies cannot reach it because their control store is read-only.
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

The independent audit-repair RED command was:

```text
uv run --extra dev pytest -o addopts='' -q \
  tests/test_market_get_read_only.py tests/test_full_market_history.py \
  -k 'calendar_status_read_failures or direct_analysis_readers or writable_reconciler_repairs_legacy or reconciler_maps_control_schema or lifespan_writer_owner_repairs_legacy or full_history_writer_owner_repairs_legacy or full_history_cli_writer_owner_repairs_legacy'
```

It produced `13 failed, 3 passed`. The failures reproduced all eight unavailable dataset metadata
states, legacy control-schema reconciliation in direct/lifespan/full-history/CLI owners, and control
schema initialization error mapping. The three calendar cases already passed because the preserved
WIP had implemented the SQLite reader; review then aligned its public error code with the contract.

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
- missing/corrupt/locked calendar control state returns structured 503 without initialization;
- correct taxonomy `baostock.industry_classification` is used by sector GET coverage;
- missing/invalid sentinel, manifest, generation and item metadata fail closed for both direct
  market-regime and sector readers, while a valid empty manifest remains HTTP 200/empty;
- normal local reads request DuckDB `read_only=True` and reject `_connect()` writer access;
- exact configuration mismatch fallback, similar/non-matching error fail-closed behavior;
- an independent read-only API store sees the previous committed value during an in-process writer
  transaction and the new value after commit;
- explicit local writer initialization/save and explicit dataset pointer reconciliation still work;
- legacy writer-owned control schema is migrated before reconciliation in direct, lifespan,
  full-history service and CLI paths;
- read-only dataset writer lifecycles fail before any dataset, staging, or control mutation.

## Verification

Initial clean baseline at `95e70ae`:

```text
611 passed in 87.27s
```

Final focused suite:

```text
uv run --extra dev pytest -o addopts='' -q tests/test_market_get_read_only.py
27 passed in 1.52s
```

Related calendar/dataset/regime/sector/full-history/acceptance suite:

```text
198 passed in 16.30s
```

Final full suite after documentation:

```text
uv run --extra dev pytest -o addopts='' -q
640 passed in 64.87s
```

Lint:

```text
uv run --extra dev ruff check backend tests
All checks passed!
```

The repository-wide `ruff format --check backend tests` is not a clean baseline: it reports
pre-existing format differences across files outside this slice. No broad formatting rewrite was
performed. Five fully changed R1-E files passed focused format check (`5 files already formatted`),
and Ruff range checks passed every changed hunk in the four files with unrelated pre-existing
format drift. The final `git diff --check` also passed.

## Deferred acceptance

This worker did not touch the real Application Support control database or deploy a release. A root
production acceptance pass must snapshot the installed control DuckDB and local dataset, execute the
representative GET sequence, and prove the real hash/mtime/schema/tree remain identical.
