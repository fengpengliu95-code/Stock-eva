# Release 2 / R2-F4.0 acceptance

## Decision

**R2-F4.0 SELECTION CONTRACT GO / SECONDARY BLOCKED / R2-F4 NO-GO**

The reviewed implementation baseline is
`eed3e8d864bd56186a07e5c1c7adbf0aea98188d`. R2-F4.0 delivers an offline,
read-only capability admission shield. It makes the narrow R2-F3 TickFlow Free Daily Bar shadow
qualification visible without granting canonical publication or failover authority.

This is not R2-F4 delivery. Manual and automatic secondary selection remain unimplemented and
disabled. BaoStock remains the only canonical provider.

## Delivered contract

- `SecondaryCapabilitySnapshotV1` records every required semantic state and binds its complete,
  non-null projection to a domain-separated SHA-256 identity.
- The current TickFlow profile can prove only `daily_bar_state=QUALIFIED` from a strict
  `READY / SHADOW_QUALIFIED / 20` R2-F3 status. Adjustment factor and full-session qualification
  remain `UNQUALIFIED`; activity units, suspension, exact-session universe, promoted calendar and
  raw-retention semantics remain `UNKNOWN`; canonical session failover is a literal
  `UNQUALIFIED`.
- `FailoverReadinessV1` exposes configured versus effective policy. Effective auto-failover is
  always false, the eligible secondary is always null, provider and secondary request counts are
  zero, and canonical writes are false.
- Advisory preflight always permits a ready BaoStock primary and always returns
  `SECONDARY_BLOCKED / PRESERVE_POINTER` when the primary is unavailable.
- `GET /api/v1/market/failover-readiness` and `market-failover-readiness` read only strict existing
  control state. The CLI has no `--execute`; its exits are ready/safely blocked `0`, control state
  unavailable `1`, and invalid allowlisted configuration `2`.
- CLI settings use a frozen, explicit non-credential allowlist. Invalid API configuration is a
  sanitized HTTP 422, while projection or policy programming failures remain HTTP 500 and are not
  disguised as control unavailability.

No canonical `ProviderId`, bar model, candidate, selection, storage, manifest, pointer or automation
path was widened.

## Commit chain

- R2-F3 Daily Bar GO base: `8095d54825ddf3e32de783a64d25e8c3fbe547a7`
- Approved R2-F4.0 specification: `89f0761b70e37e896fa3f3c7ebae5d18318ba601`
- Task 1 final identity/readiness shield: `3537e63d983509972cadc2a3b9681a367711334f`
- Task 2 final API/CLI/config boundary: `e02967e47333652cfe7e501c18e5d0b87d3fce9f`
- Initial zero-provider/production-path sentinel proof: `bbbd9d6da4aece3497f0dc5a3c461510c6e2bfc1`
- Strengthened built-in-open/strict-reader sentinel proof:
  `eed3e8d864bd56186a07e5c1c7adbf0aea98188d`

Task 1 used four independent review/fix cycles before CODE GO. Task 2 used three independent
review/fix cycles before CODE GO. The final Task 1 review was H=0/M=0/L=1; the final Task 2 review
was H=0/M=0. The remaining Low item is the maintainability complexity of the already partitioned
ready-capability validator; it does not widen authority or weaken fail-closed behavior.

## Offline acceptance evidence

All commands used private `/tmp` roots. No provider request, credential lookup, Application Support
runtime read, production canonical/provider-control read, or `/Volumes/Stock` access was performed.

- Final focused gate: `206/206` passed, including the provider/production-path hard sentinels.
- Final full repository gate: `2202/2202` passed.
- Ruff check: passed; Ruff format check: 197 files already formatted.
- `python -m compileall -q backend`: passed.
- Strict design validator: 100/100, zero errors, zero warnings; one informational note that this
  backend-only contract has no TypeScript interface.
- `git diff --check`: passed.
- One existing non-blocking warning remains: Starlette's TestClient reports its `httpx`
  compatibility path as deprecated.

## Synthetic strict-status proof

A private temporary-root replay ran the existing 20-session Daily shadow end-to-end fixture, then
opened the result through `DailyShadowRegistryReader(verify_external=True)` and projected it through
the new readiness shield. The exact terminal state was:

```text
status=READY
window_state=SHADOW_QUALIFIED
sessions=20/20
circuit_state=CLOSED
publication_enabled=false
failover_enabled=false
daily_bar_state=QUALIFIED
adjustment_factor_state=UNQUALIFIED
canonical_session_failover_state=UNQUALIFIED
readiness_status=ready
effective_auto_failover_enabled=false
eligible_secondary=null
first_blocked_reason=DAILY_BAR_ONLY
```

The same synthetic test asserts its canonical manifest and immutable partition bytes remain
unchanged. Independent Task 2 review also replayed strict ready, short/corrupt, symlink, wrong-mode,
journaled and locked sidecars; every invalid case returned bounded unavailable and repeated reads
preserved tree, database bytes and mtimes.

The final zero-access test wraps both `builtins.open` and filesystem
`open/stat/lstat/listdir/scandir` with hard sentinels for
`/Users/finlay/Library/Application Support` and `/Volumes/Stock`, replaces BaoStock and TickFlow
adapter constructors with failure sentinels, and executes both the API and CLI through a valid
private layout into the real strict reader with `verify_external=true`. The deliberately absent
private database remains unavailable; both entry points invoke the strict reader, construct no
provider, and access no forbidden path.

## Protected compatibility evidence

The R2-F2 golden manifest validated all nine tracked objects:

```text
GET.json                                      1ecffe3f1572aa19520025cf885051fd8036d7eddabee0375b3f132c2d009452
candidate.json                                8d58e85b3edd94f0e7d2af2fe372634b63612cebe8a981774116e4a264eee8e1
evidence.json                                 80c262de15a8027b6259993f938687b56aad0cda78b38d582387e2da83a18d66
manifest.json                                 f3c4cf48aa680190c9bd652584360e888418bb7cb4eba0248377d4fc24d7ef1f
partition.parquet                             73b94d4ae45e7c05a39e479f1321cefac8f0da7cd661f0d53a0b6478a4507592
reader_models.json                            f53e9824bafb294249f215d46162f266b134113e93e2c490df899a25f9c770e6
selection.json                                92e0d74dfeba21b676262b6f639bec37b2fd09858fb2b984d81754a81bc08db1
manifests/ev-2ce5ef73d443e9e13ffce1d4.json     6fc19b6fd02db6da947d43e36762c64e5874099cc37ca662832d878d4c42cf7c
objects/85/obj-852d9da7f238f2161a963f2e.parquet 852d9da7f238f2161a963f2e659b70137e88f2d4e8031e98c4e63dd35bf2ff46
```

`shasum -a 256 -c sha256sums.txt` reported all nine objects `OK`, and the R2-F2 compatibility tests
passed. R2-F3 protected identities are unchanged from the R2-F3 GO base, including:

```text
tests/fixtures/r2f3_daily_shadow/legacy_manifest.json                    c2a1e8a72c6d09da21f0e8f172599ef076eeed907f24d8902d0f392959f3c986
docs/acceptance/release-2-r2f3-daily-bar-shadow.md                       54dabfa45970da5c9da8b81636109d4b192fb7bf7aedb1ba1ed25abe591c6547
docs/acceptance/release-2-r2f3-daily-shadow-qualification-report.md      3e95753198dddb0a7a78b4edfe127c5e44c9239476f62d2c0431799ad9a9e79f
backend/app/market/daily_shadow_models.py                                1f7fb78cd1f722da3bc61c5c488006820bc9fcd57a7ac2a131342ede37bec1ec
backend/app/market/daily_shadow_candidates.py                            1aacd02d6ddcb4f54c0005c889bbd0bec995e9b4ff1959b94a3b650a631582c1
backend/app/market/providers/tickflow_daily_shadow.py                    8bbee0ed0cf6d5ad74e8f78f0ed8ec6593d4be870df1c931bf653661112529af
```

The diff from `8095d54825ddf3e32de783a64d25e8c3fbe547a7` is empty for every explicitly
frozen canonical and R2-F3 source surface listed in the R2-F4.0 plan. The only production-code
changes are the new isolated `market/failover.py`, two Settings fields plus the credential-free CLI
projection, and read-only API/CLI wiring.

## Explicit non-goals and next gate

- No secondary provider was called or admitted to canonical publication.
- No symbol-level mixing, candidate fallback, manifest-source widening, pointer move or automation
  branch exists.
- No adjustment factor, activity-unit, suspension, exact-universe, promoted-calendar or retention
  semantic was guessed or promoted.
- Configuring auto-failover true still cannot make it effective.
- R2-F4 remains NO-GO.

The next blocking subversion is **R2-F4.1 promoted runtime calendar generations and next-year
fail-closed maintenance**. It must complete its own design, RED/GREEN implementation and independent
review before any later universe, replication, canonical qualification or failover execution work.
