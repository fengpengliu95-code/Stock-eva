# Release 2 / R2-F3 Task14 Free discovery acceptance

## Current decision

`CANARY #1 FAIL-CLOSED / V2 OFFLINE CODE AWAITING INDEPENDENT REVIEW / NEW CANARY NOT AUTHORIZED / SHADOW NOT STARTED`

The V1 reviewed implementation commit is
`099cb1ca46931459c6ce6177a164aedb47843ad5`. It replaces the earlier
credential-gated TickFlow discovery default with a separate
`FREE_DAILY_DISCOVERY` capability. Missing `STOCK_EVA_TICKFLOW_TOKEN` is not an error in this
mode. Only the explicitly selected legacy `AUTHENTICATED_DISCOVERY` capability may read or require
that credential.

The first authorized Free canary completed its four requests and then failed closed with
`failure_class=symbol_schema`. It did not discover or qualify any capability. V2 removes an
unsupported full-universe SH/SZ-only assumption and adds fixed endpoint attribution; V2 is not
eligible for another real request until its exact commit passes independent review. This record
does not claim that TickFlow Free is currently usable, that Daily Bar or adjustment factors are
qualified, that a shadow session has started, or that failover is ready.

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

The V1 Free TermsEvidence was attached under the user-authorized preflight with contract version:

```text
r2f3-tickflow-free-daily-v1-f9d924340b978d6c8362fed3eeb08438ea53be4a25460071d105f0e4f9a7169f
```

The preflight performed zero provider requests and advanced only the isolated provider record from
state version 2 to 3 while retaining `CANARY`. V2 deliberately changes the adapter/source
descriptor and now requires:

```text
r2f3-tickflow-free-daily-v2-ea6cfd3c52fc38f82cf17951a85f02bc605177aaead4753f3e11896b56daabd4
```

The existing V1 registry attachment is therefore stale for V2. A read-only gate replay proved it
fails before SDK or HTTP client construction with zero provider requests and writes. No V2
TermsEvidence may be attached until independent review returns GO. That review gate is now
complete; descriptor attachment remains separately authorization-gated.

The retained isolated root is
`/Users/finlay/Library/Application Support/Stock EVA/r2f3-task14-canary-20260826`. The historical
date was fixed to `2026-08-10`, which the current canonical manifest independently records as a
BaoStock trading partition with 3,195 rows. The manifest was read only to select the date; the
canary did not mutate or publish against it.

## Offline verification evidence

- Initial implementation `af652e0a6b41fbb009a440a177b9c54b667a7fd8` received `NO-GO` for an
  authenticated/Free review-identity collision and general-environment enumeration.
- Remediation `099cb1ca46931459c6ce6177a164aedb47843ad5` introduced the independent Free
  descriptor, closed eight-name runtime settings projection, isolated SDK child and
  `trust_env=false` HTTP client.
- Independent read-only re-review of the exact remediation commit: H=0, M=0, decision `GO`.
- V2 RED reproduced the unsupported universe regex and missing endpoint attribution; six new
  focused regressions are GREEN.
- V2 code `0da87a708176f3f92cbcc79244db2061ac76aef5` plus report-enum documentation
  correction `d32b63c45cd5fa67cc50515a5c5e316061d72b11` received final independent
  read-only review `GO`, H=0 and M=0. The review made zero network/provider requests and confirmed
  the worktree was clean.
- V2 focused Free suite: 29 passed; related provider/registry/shadow/golden/LaunchAgent suite:
  418 passed.
- V2 full offline suite: 2,023 passed.
- Strict design validator: 100/100, zero warnings.
- Changed-file Ruff check and format check, compileall, `uv lock --check` and
  `git diff --check`: passed.
- All nine R2-F2 golden objects matched their recorded SHA-256 values.
- The pinned SDK initializer smoke test reported version `0.1.24`, Free base URL, zero retries and
  zero provider requests.
- Full-repository Ruff still reports only pre-existing findings in the unmodified
  `replace_markdown_kline.py` and `scripts/markdown_to_pdf.py`; these are outside Task14.

All V2 tests were offline. No token, production DB, Parquet, manifest, pointer, NAS, LaunchAgent or
canonical dataset was mutated by development or verification.

## Controlled canary #1 evidence

- authorization ID: `r2f3-tickflow-free-canary-20260827-01`;
- descriptor content SHA-256:
  `faf0a52e5cb491aab2e96e4edee3ad1703ef5af482d94e6818c4eae6b905b7d4`;
- descriptor manifest SHA-256:
  `4ab599966228fb25f7357487c9b9aa8e42020c396911b160173a4fc2c01b2e95`;
- trade date `2026-08-10`, four requests, fixed five symbols and `max_attempts=1`;
- result: `unavailable / symbol_schema`, `provider_requests=4`;
- all four discoverable capabilities remained `UNKNOWN`; quote/minute remained `FORBIDDEN` and
  adjustment factor remained `UNQUALIFIED`;
- `daily_bar_qualified=false`, `adjustment_factor_qualified=false`, `starts_shadow=false`;
- `writes=false`, `writes_evidence=false`, `writes_canonical=false`;
- post-canary registry SHA-256 remained
  `d3fb9907d3095c5fe7c7a788ea0d4a8d920dba852a26ce7b42ba7fa80d98f01e`;
- canonical manifest SHA-256 remained
  `052799bddd785c8a4204e0bc3352b16edaa636934d819a156f247bb8393873a7`;
- evidence files, shadow files, jobs, qualification windows and session reports all remained zero.

The zero-write contract is GO. Free capability discovery and shadow qualification remain NO-GO.
Because no payload was retained, the exact offending live symbol and failing parser endpoint are
unknown; `CN_Equity_A` containing a non-SH/SZ member is only the leading hypothesis, not a fact.

## Next controlled-operation gate

The first authorization is consumed. The exact V2 implementation has completed independent review
with H=0, M=0 and `GO`. A future V2 controlled operation now requires, in order:

1. a new explicit user authorization;
2. attach the exact V2 TermsEvidence descriptor with zero provider requests;
3. run one zero-write Free discovery canary for `2026-08-10`;
4. permit only the four requests and fixed five-symbol sample listed above, with
   `max_attempts=1`;
5. keep the canary at zero DB/Parquet/evidence/manifest/pointer writes;
6. do not call realtime, minute, factor or authenticated endpoints;
7. do not start shadow or trigger another request after success or failure.

No item above is currently authorized. Any future execution must stop after one sanitized report;
contract mismatch, transport failure, incomplete response or schema failure remains fail-closed.
