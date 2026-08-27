# Release 2 / R2-F3 Task14 Free discovery acceptance

## Current decision

`OFFLINE RUNNER CODE GO / REAL FREE CANARY AUTHORIZATION REQUIRED / SHADOW NOT STARTED`

The exact reviewed implementation commit is
`099cb1ca46931459c6ce6177a164aedb47843ad5`. It replaces the earlier
credential-gated TickFlow discovery default with a separate
`FREE_DAILY_DISCOVERY` capability. Missing `STOCK_EVA_TICKFLOW_TOKEN` is not an error in this
mode. Only the explicitly selected legacy `AUTHENTICATED_DISCOVERY` capability may read or require
that credential.

No real TickFlow request was made while implementing or reviewing this change. This record does
not claim that TickFlow Free is currently reachable, that any live capability has been discovered,
that Daily Bar is qualified, that adjustment factors are qualified, that a shadow session has
started, or that failover is ready.

## Frozen Free canary contract

The Free canary is limited to the official `https://free-api.tickflow.org` origin and exactly four
ordered requests:

1. `GET /v1/exchanges` for connectivity and exchange metadata;
2. `POST /v1/instruments` for the fixed five-symbol instrument sample;
3. `GET /v1/universes/CN_Equity_A` for A-share universe metadata;
4. `GET /v1/klines/batch` for the same five symbols, one fixed trade date, `period=1d` and
   `adjust=none`.

The fixed provider-symbol sample is `600000.SH`, `600519.SH`, `601318.SH`, `000001.SZ` and
`000002.SZ`. Realtime quote, minute K-line and all authenticated/full-service endpoints are outside
the contract and are forbidden. The canary has `max_attempts=1`, at most four provider requests,
an 8 MiB response limit, fixed timeout bounds, TLS enforcement, no redirects and strict response
schema/date/symbol validation.

The production path initializes the pinned official `tickflow==0.1.24` `TickFlow.free()` factory in
an isolated child process without performing an SDK request. Its closed child environment contains
no credential. Actual Free requests use the bounded streaming transport with environment proxy
lookup disabled and never attach an authorization header.

## Zero-write and qualification boundary

The Free runner has no evidence writer, canonical writer, candidate publisher, manifest publisher,
pointer publisher or shadow starter. Success and failure both report:

- `writes=false`, `writes_evidence=false` and `writes_canonical=false`;
- `complete_candidate=false` and `starts_shadow=false`;
- `daily_bar_qualified=false` and `adjustment_factor_qualified=false`.

Before a successful real canary, connectivity, instrument metadata, universe metadata and
historical Daily Bar remain `UNKNOWN`. A complete four-request success may report only those four
Free capabilities as `DISCOVERED`; this is discovery evidence, not shadow qualification.

The following states are frozen independently and cannot be inferred from a Daily Bar response:

- adjustment factor: `UNQUALIFIED`;
- suspension semantics: `UNKNOWN`;
- price/volume/amount units: `UNKNOWN`;
- rate-limit and quota: `UNKNOWN`;
- raw-retention contract: `UNKNOWN`;
- realtime quote and minute K-line: `FORBIDDEN`.

Daily Bar Shadow qualification and adjustment-factor qualification therefore remain separate
workstreams. A successful canary cannot transition the provider to shadow, start the 20-session
window, publish a candidate or enable failover.

## Descriptor and control-state gate

The existing isolated registry contains the earlier authenticated-capability review and cannot be
reused as proof of the Free request graph. Before the real canary, a separately reviewed
TermsEvidence record must be attached to the TickFlow provider record with the exact contract
version:

```text
r2f3-tickflow-free-daily-v1-f9d924340b978d6c8362fed3eeb08438ea53be4a25460071d105f0e4f9a7169f
```

This is a bounded control-state preflight write to the isolated provider registry and performs zero
provider requests. It is not part of the canary. The canary itself remains strictly zero-write. If
the descriptor is absent, stale or mismatched, execution fails before constructing the SDK or HTTP
client and the authorization is not consumed on a provider request.

The retained isolated root is
`/Users/finlay/Library/Application Support/Stock EVA/r2f3-task14-canary-20260826`. The proposed
historical date is fixed to `2026-08-10`, which the current canonical manifest independently records
as a BaoStock trading partition with 3,195 rows. The manifest was read only to select the date; the
canary is not allowed to mutate or publish against it.

## Offline verification evidence

- Initial implementation `af652e0a6b41fbb009a440a177b9c54b667a7fd8` received `NO-GO` for an
  authenticated/Free review-identity collision and general-environment enumeration.
- Remediation `099cb1ca46931459c6ce6177a164aedb47843ad5` introduced the independent Free
  descriptor, closed eight-name runtime settings projection, isolated SDK child and
  `trust_env=false` HTTP client.
- Independent read-only re-review of the exact remediation commit: H=0, M=0, decision `GO`.
- Focused Free suite: 104 passed.
- Full offline suite: 2,017 passed.
- Strict design validator: 100/100, zero warnings.
- Changed-file Ruff check and format check, compileall, `uv lock --check` and
  `git diff --check`: passed.
- All nine R2-F2 golden objects matched their recorded SHA-256 values.
- The pinned SDK initializer smoke test reported version `0.1.24`, Free base URL, zero retries and
  zero provider requests.
- Full-repository Ruff still reports only pre-existing findings in the unmodified
  `replace_markdown_kline.py` and `scripts/markdown_to_pdf.py`; these are outside Task14.

All tests and reviews were offline. No token, provider request, production DB, Parquet, manifest,
pointer, NAS, LaunchAgent or canonical dataset was read or mutated by this implementation window.

## Requested controlled operation

A new authorization is required because the prior authenticated-discovery authorization does not
cover this breaking Free capability contract. The requested operation is:

1. attach the exact reviewed Free TermsEvidence descriptor to the existing isolated Task14
   provider registry, with zero provider requests;
2. run one Free discovery canary for the fixed confirmed A-share trade date `2026-08-10`;
3. permit only the four requests and fixed five-symbol sample listed above, with
   `max_attempts=1`;
4. keep the canary at zero DB/Parquet/evidence/manifest/pointer writes;
5. do not call realtime, minute, factor or authenticated endpoints;
6. do not start shadow or trigger another request after success or failure.

Live execution must stop after emitting the sanitized report. Any contract mismatch, transport
failure, incomplete response or schema failure remains fail-closed.
