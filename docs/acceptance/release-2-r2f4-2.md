# Release 2 / R2-F4.2 Exact-session Universe

## Scope and status

This document records the Step 6-7 implementation boundary for the exact-session Universe
sidecar. It is an additive control/read-only surface and is not a production enablement claim.
The canonical BaoStock refresh path, Normalize → Quality Gate → Immutable Parquet → SHA-256 →
Manifest → Atomic Publish chain is unchanged. `market_universe_mode` remains `off` by default;
secondary publication, automatic failover and provider switching remain disabled.

## Read-only API and CLI

`GET /api/v1/market/universe?trade_date=YYYY-MM-DD` and
`stock-eva market-universe --date YYYY-MM-DD` emit the same bounded status projection. They read
only a strictly validated, already-created sidecar. They never initialize/migrate the sidecar,
read credentials, construct a provider, access UserStore, or write database, Parquet, manifest,
pointer or canonical state. Every successful or safe response includes `provider_requests=0` and
`writes=false`; no payload, path, SQL, token, URL or raw exception is exposed.

The status decision is deterministic: lexical-invalid date → HTTP 422/CLI 2 without I/O;
lexically-valid date with an unprovable sidecar → HTTP 503/CLI 3; only after sidecar proof is a
future date rejected as `PIT_VISIBILITY_INVALID` (HTTP 422/CLI 2). A valid head on another date is
`stale/DATE_MISMATCH`; a persisted source digest change is stale or, when paired with a durable
terminal attempt, blocked according to the allowlisted terminal matrix. An initialized empty head
is `unavailable/CONTROL_STATE_UNAVAILABLE`, while a success without a promoted head is a control
failure rather than an inferred current state.

## No-migration rehearsal

The offline rehearsal uses a synthetic promoted classification/evidence fixture to populate an
isolated sidecar, reads the status projection, and fingerprints the pre-existing canonical files,
manifest and pointer before/after. The expected result is byte/inode identity for the canonical
chain and no new control directory from a missing-sidecar GET/CLI. No historical partition is
relabeled, no old date is presented as a new date, no provider is replaced, and no real network,
production, NAS or credential operation is part of this rehearsal. A passing rehearsal is
compatibility evidence only; it is not a production GO decision.
