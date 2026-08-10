# Release 1 Acceptance

Status: **PENDING ROOT PRODUCTION EVIDENCE**

R1-E adds immutable daily regime snapshots and a read-only replay audit. A historical
`post_hoc_backfill` proves deterministic results against the verified publication available at
audit time; it does not prove the classification generation was visible on the historical date.
The audit therefore reports post-hoc availability and contemporaneous visibility separately.

## Frozen root command matrix

1. Fingerprint the market dataset, market control database, classification database, supplemental
   audit database and private user database.
2. Run snapshot dry-run for the complete manifest range and prove it changes no file.
3. Run the same range with explicit `--execute`, then repeat it and prove zero inserts plus stable
   snapshot database bytes, sidecars and mtimes.
4. Run `stock-eva release-one-audit --sessions 20` and preserve its single JSON report.
5. Read an exact persisted snapshot through
   `GET /api/v1/analysis/market-regime/snapshots/{as_of}` and measure 20-request p95 latency.
6. Re-fingerprint every protected input and prove only the dedicated derived snapshot database
   changed during the authorized backfill.
7. Browser-check market → sector → leader → stock → sector at desktop and mobile widths, including
   preserved context, raw indicators, contrary evidence, missing fund-flow disclosure and a clean
   console.

## Evidence placeholders

- Installed commit / `RELEASE.json`: PENDING ROOT PRODUCTION EVIDENCE
- Manifest range, generation and selected session count: PENDING ROOT PRODUCTION EVIDENCE
- Backfill inserted/idempotent counts: PENDING ROOT PRODUCTION EVIDENCE
- Replay report and classification coverage gaps: PENDING ROOT PRODUCTION EVIDENCE
- Exact snapshot API latency: PENDING ROOT PRODUCTION EVIDENCE
- Protected-store before/after fingerprints: PENDING ROOT PRODUCTION EVIDENCE
- Desktop/mobile browser decision path: PENDING ROOT PRODUCTION EVIDENCE

This document does not declare Release 1 GO.
