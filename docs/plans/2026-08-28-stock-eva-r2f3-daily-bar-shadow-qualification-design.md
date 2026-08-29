# Stock EVA R2-F3 TickFlow Free Daily Bar Shadow Qualification Design

**Author:** Codex delivery team

**Date:** 2026-08-28; amended 2026-08-29 (Asia/Shanghai)

**Status:** **REVIEW REMEDIATION IMPLEMENTED / RE-REVIEW PENDING / DAILY BAR SHADOW NO-GO**

**Scope:** The capability-scoped `TICKFLOW_FREE_DAILY_BAR_OHLC_V1` shadow lane and its
20-consecutive-confirmed-session qualification window.

**Authority:** The user's standing R2-F development and validation authorization permits the
required local reads/writes and bounded real-provider validation. It does not weaken the version
delivery gate, provider terms gate, fail-closed behavior, canonical isolation, or automatic
failover default-off rule.

**Predecessor:** Task14 Free discovery at commit
`619d1f3dd30823506607fadeccbbec6c29cebbae` is GO. Its fixed-five, zero-write discovery result is
not a shadow session and cannot be counted in this window.

## Context

Task14 proved that the credentialless TickFlow Free origin can return one structurally valid
fixed-five-symbol historical `1d` response using `adjust=none`. It deliberately left adjustment
factor, suspension semantics, volume/amount units, rate-limit/quota and raw-retention semantics as
`UNQUALIFIED` or `UNKNOWN`.

The existing Tasks10-13 whole-provider pipeline cannot safely represent that narrower fact. Its
normalizer, candidate gate, canonical comparison and terminal promotion require a proved factor
contract and can promote a provider-level window to `qualified`. Making factor fields nullable or
relabeling a Daily-only result would permit an OHLC comparison to masquerade as complete provider
qualification.

The production canonical dataset also contains valid pre-R2-F2 legacy manifest entries. For
example, the immutable `2026-08-10` BaoStock partition is bound by path, row count and SHA-256 but
does not contain R2-F2 candidate lineage fields. The Tasks10-13 canonical reader rejects that
partition because it requires the newer lineage graph. A Daily-only reader must therefore support
the reviewed legacy descriptor without inventing missing lineage, while remaining descriptor-
bound and read-only.

The first Task 7 independent review additionally proved two real-data constraints that the initial
specification had modeled incorrectly. Canonical suspended placeholders legitimately carry
`quality_status=partial`, and the exact active symbol set changes normally from session to session.
The amended contract therefore hashes every session's full canonical identity separately while the
20-session version vector binds the stable eligibility/mapping policy. Daily listing/suspension
changes cannot be mistaken for a contract change or make the qualification window unreachable.
The stable policy hash explicitly includes the only reviewed legal suspended `partial` issue set,
`missing_adjust_factor` plus `suspended_placeholder`; changing that allowlist changes the policy,
descriptor/Terms contract version and version vector.

### Decision

Use an additive capability sidecar:

```text
immutable BaoStock manifest + exact trade-date Parquet
        | read-only descriptor/fingerprint validation
        v
canonical active-stock OHLC projection + exact universe hash
        | deterministic sh./sz. -> provider symbol mapping
        v
TickFlow Free sequential <=100-symbol daily shards, adjust=none, one attempt
        | all shards final-success or whole session invalid
        v
immutable source-shaped evidence -> Daily candidate -> OHLC reconciliation
        | separate append-only control DB/window
        v
TICKFLOW_FREE_DAILY_BAR_OHLC_V1: OBSERVING -> SHADOW_QUALIFIED

No SessionSelection, canonical writer, provider-level qualification or failover transition
```

Three approaches were considered:

1. **Recommended — parallel capability sidecar.** Reuse immutable evidence primitives but add a
   Daily-specific canonical projection, candidate, reconciliation report and control DB. This
   preserves every Tasks10-13 hash and prevents capability leakage.
2. Add `qualification_track` columns to every existing registry/job/session/terminal table. This
   avoids a second DB but changes frozen DDL, foreign keys, terminal hashes and golden fixtures.
3. Make existing factor fields optional and treat Daily as a partial complete candidate. This is
   rejected because a nullable field could silently bypass the factor gate and promote the whole
   provider.

The sidecar qualifies one narrow capability: whole-session parity of unadjusted-requested OHLC for
the exact set of canonical BaoStock stock rows that are active and non-suspended on that session.
It does not qualify the provider's listing universe, suspended/no-row behavior, volume, amount,
factors, corporate actions, indexes, publication eligibility or operational production soak.

## Functional Requirements

- FR-1: The qualification profile MUST be the exact literal
  `TICKFLOW_FREE_DAILY_BAR_OHLC_V1`. No caller-supplied provider, profile, endpoint, period,
  adjustment mode, origin or symbol mapping may widen it.
- FR-2: Daily qualification state MUST be separate from `provider_record.admission_state` and the
  Tasks10-13 `qualification_window`. Its states MUST be `PENDING`, `OBSERVING`,
  `SHADOW_QUALIFIED`, `RESET` or `UNAVAILABLE`; it MUST NOT write provider state `qualified`.
- FR-3: The sidecar MUST use a distinct local control database named
  `daily_bar_shadow.sqlite3`, a distinct lock and Daily candidate/report namespaces. It MAY reuse
  the existing immutable `ShadowEvidenceStore`/reader protocol because `window_id`, plan and every
  object hash bind the capability-specific identity.
- FR-4: A canonical Daily reader MUST be read-only and accept exactly two manifest entry forms:
  the five-field legacy descriptor (`path`, `row_count`, `sha256`, `source`, `trade_date`) or the
  exact complete R2-F2 lineage descriptor. It MUST never synthesize absent R2-F2 lineage; the
  snapshot MUST record `lineage_state=LEGACY_UNAVAILABLE` or `R2F2_VERIFIED`.
- FR-5: The reader MUST validate the dataset sentinel, exact manifest schema/generation, manifest
  fingerprint and SHA-256, unique trade-date entry, safe relative partition path, descriptor hash,
  exact row count, exact canonical Parquet schema, source `baostock`, requested date and stable
  descriptor/stat identity before and after the read. A mutation at any stage is unavailable.
- FR-6: The canonical eligible universe MUST contain every and only row with
  `security_type=stock`, `exchange in {sh,sz}`, canonical symbol matching `^(sh|sz)\\.\\d{6}$`,
  `is_trading=true`, `is_suspended=false`, `quality_status=ready`, finite positive OHLC and legal
  OHLC ordering. Index rows are excluded explicitly. Unknown security types, exchanges, quality
  states, duplicate symbols or invalid rows invalidate the entire projection.
- FR-7: Suspended/non-trading canonical stock rows MAY be excluded only into a separately hashed
  `canonical_exclusion_sha256`; a canonical `partial` row is legal here only for the exact reviewed
  suspended-placeholder quality-issue set. These rows cannot be used to infer TickFlow
  suspension/no-row semantics.
  The exact sorted active symbol set, exact excluded symbol set, manifest identity and partition
  identity MUST produce a deterministic `canonical_universe_sha256`.
- FR-8: The only symbol mapping is the total reversible function
  `sh.600000 -> 600000.SH` and `sz.000001 -> 000001.SZ`. Every input and returned key MUST validate;
  duplicates, aliases, extra keys, missing keys, BJ/HK/US or any non-reviewed exchange fail closed.
  The sorted pair list MUST produce a per-session `symbol_mapping_sha256` bound to the request plan
  and candidate. The stable mapping-contract hash and canonical universe-policy hash, not the
  naturally changing daily pair list, MUST be included in the window version vector.
- FR-9: The request plan MUST sort canonical symbols, partition them deterministically into
  consecutive shards of at most 100 symbols, and create exactly one `historical_daily_1d` logical
  request per shard. The plan MUST bind both the complete per-session canonical-universe identity
  and a separate exact active-symbol-set hash. Shard order, membership, date, `period=1d`, exact
  Asia/Shanghai trade-date bounds expressed as UTC epoch milliseconds, `adjust=none`, schema hash
  and unit-state hash MUST be immutable and hash-bound. A response timestamp is valid only when its
  Asia/Shanghai calendar date equals the requested trade date; a UTC natural-day match is
  insufficient.
- FR-10: Each execution MUST process exactly one trade-date session. It MUST perform at most 40
  provider requests, one attempt per shard, sequentially, with no SDK batch concurrency, hidden
  retry, sleep, pagination fallback, authenticated endpoint, realtime quote, minute K-line,
  factor, universe or instrument request.
- FR-11: Production initialization MUST retain the pinned `tickflow==0.1.24`
  `TickFlow.free()` preflight with exact Free origin and zero SDK retries, but all provider I/O MUST
  use Stock EVA's bounded HTTP transport with `trust_env=false`, no redirects, exact host pinning,
  5-second connect/write/pool bounds, 30-second read bound and at most 8 MiB per response.
- FR-12: Each response MUST be strict source-shaped compact K-line data for exactly its requested
  shard. It MUST contain exactly one row per requested symbol on the exact date, no extra optional
  semantic fields beyond the reviewed schema, finite positive OHLC, legal OHLC ordering,
  nonnegative syntactically valid activity values and no duplicate timestamp/symbol.
- FR-13: Unknown activity units MUST not prevent OHLC structural parsing, but volume and amount
  MUST be retained only as opaque source values and MUST NOT enter qualification, conversion,
  coverage tolerance, UI claims or publication. `units_state` remains `UNKNOWN`.
- FR-14: `adjust=none` records the requested mode only. The Daily candidate MUST record
  `adjustment_request_state=NONE_REQUESTED` and `factor_evidence_state=UNQUALIFIED`; it MUST NOT
  manufacture factor values, anchoring, direction or adjusted returns.
- FR-15: A session is evidence-ready only when every planned ordinal has one final successful
  attempt and one contiguous page identity. If any request fails, times out, rate-limits, returns
  partial/extra/duplicate/wrong-date data or the process crashes, the entire candidate is invalid.
  Failed partial response rows and bytes MUST not become readable evidence.
- FR-16: Only final successful source-shaped shard rows may be persisted through the immutable
  evidence lane. Failed-attempt audit may contain only fixed endpoint/shard identity, attempt,
  elapsed/count/byte metrics and an allowlisted failure class; it MUST not contain payload, URL,
  header, token, provider text or offending symbol.
- FR-17: A new capability-specific review descriptor and TermsEvidence MUST bind the exact Free
  adapter, endpoint/source schema, SDK/wheel, symbol mapping, canonical universe policy, candidate,
  per-session active-symbol-set binding contract, reconciliation policy, retention decision and
  request-budget hashes. Task14 V1/V2 discovery descriptors cannot authorize this wider whole-
  session plan.
- FR-18: The user-selected retention policy is final-success source evidence only. The capability
  record MUST still report provider raw-retention semantics as `UNKNOWN`; this state cannot be
  converted into a general redistribution or long-term retention claim.
- FR-19: `DailyBarShadowCandidate` MUST bind provider/profile, trade date, evidence/completion and
  request-plan hashes, canonical snapshot/manifest/partition/universe/exclusion hashes, exact
  per-session active-symbol-set and mapping hashes, stable universe-policy and
  adapter/schema/policy/terms hashes, exact expected/observed counts, source-row aggregate hash,
  normalized OHLC aggregate hash and all semantic-state literals.
- FR-20: Daily candidate self-quality MUST require exact expected/observed symbol equality,
  exact-one-row coverage, date match, finite positive prices, legal OHLC ordering, no duplicate,
  no extra symbol and unchanged canonical snapshot before and after evidence collection. A failed
  gate publishes no candidate.
- FR-21: Daily normalization MUST use `Decimal` from reviewed numeric strings/numbers, preserve
  unadjusted-requested OHLC only, reject booleans/non-finite values and never transform volume,
  amount or factors. Serialization and aggregate hashes MUST be deterministic UTF-8/NFC JSON.
- FR-22: `DailyBarReconciliationPolicy(version="r2f3-free-daily-ohlc-v1")` MUST compare exact
  symbol sets and each open/high/low/close against the canonical value with absolute error at most
  one CNY price tick (`0.01`). Every price cell MUST pass; any mismatch makes the session outcome
  `MISMATCH`. No percentage waiver or adjusted-return comparison is allowed.
- FR-23: Reconciliation MUST use a closeable canonical capability: verify the exact canonical
  snapshot immediately before comparison and again after comparison. Changed manifest, partition,
  fingerprint, row count, symbol set or hash is `UNAVAILABLE`, not a mismatch and not success.
- FR-24: Session publication MUST be atomic and ordered: immutable evidence commit, Daily
  candidate/report bundle commit, then one sidecar DB transaction attaching their exact hashes and
  updating the window. Locks MUST not be held during provider network I/O and canonical bytes MUST
  never be written.
- FR-25: The sidecar DB MUST use frozen versioned DDL, `journal_mode=DELETE`,
  `synchronous=FULL`, `foreign_keys=ON`, a private lock, compare-and-swap state versions,
  append-only attempt/session/attestation rows and immutable exact-hash foreign-key closure. Every
  successful job, candidate reference, session report and terminal attestation MUST carry the same
  per-session `canonical_symbol_set_sha256`; mismatch is unavailable and cannot advance the window.
- FR-26: The read path MUST take a shared lock, copy bounded descriptor-validated bytes into
  memory and open SQLite query-only. Missing DB/table, lock contention, corrupt schema, unknown
  migration or any unreadable evidence/candidate bundle referenced by the current epoch MUST return
  `UNAVAILABLE` with zero writes or initialization. External verification is bounded to the current
  epoch's at most 20 successful session graphs.
- FR-27: One qualification window MUST contain exactly 20 distinct consecutive dates from one
  immutable `ConfirmedSessionSnapshot` and the exact same stable contract/policy version vector.
  Each date retains its own exact canonical universe, exclusion and mapping hashes; normal
  session-to-session active-set changes do not change the version vector. Processing may be
  historical, but each session MUST use real provider requests after its canonical partition was
  published. The report MUST label this `observation_mode=HISTORICAL_SHADOW`; it is not R2-F5 live
  production soak evidence.
- FR-28: Any missing confirmed date, out-of-order date, duplicate success, failure, mismatch,
  unavailable result, calendar change, contract/hash change, canonical universe-policy change or
  terms change MUST end the current epoch as `RESET`. A later attempt starts a new immutable epoch;
  prior reports remain queryable and cannot be overwritten. A legitimate change in the exact
  active/excluded symbol set or immutable partition identity between two different dates MUST NOT
  reset the window; those identities remain session-local and fully hash-bound.
- FR-29: After the twentieth valid session, only the capability window may become
  `SHADOW_QUALIFIED`. TickFlow provider admission MUST remain no higher than its prior state,
  `daily_bar_qualified=true`, `adjustment_factor_qualified=false`,
  `publication_eligible=false` and `failover_enabled=false`.
- FR-30: The plan/default CLI path MUST be zero-network and zero-write. Execute MUST require an
  explicit trade date, a safe external authorization identifier, provider-request acknowledgement,
  an initialized sidecar contract, the exact reviewed descriptor and a canonical ready snapshot.
- FR-31: A scheduler invocation MUST lease and execute at most one session. It MUST not
  automatically run the next date. An endpoint circuit opens after the frozen failure threshold;
  OPEN slots record `SKIPPED_CIRCUIT_OPEN`. After cooldown, one fixed-five zero-write HALF_OPEN
  probe may close the circuit, but success MUST NOT automatically launch a whole-session request.
  The production execute path MUST wire this probe to the same credentialless, pinned, bounded
  one-attempt Daily transport; canonical/evidence/candidate artifacts remain unwritten by the probe.
- FR-32: The read-only CLI/API status MUST expose profile, window epoch/state, count/required,
  first/last date, last outcome, version-vector hash, observation mode, semantic states and
  eligibility flags. It MUST not expose raw rows, URLs, request parameters, symbols or arbitrary
  provider exceptions.
- FR-33: No Daily sidecar path may call `SessionSelection`, canonical `CandidateStore`, canonical
  evidence writer, `NasMarketStore.save_refresh`, Parquet publication, manifest/pointer publisher,
  provider admission promotion or failover code.
- FR-34: Existing Task14 fixed-five discovery, authenticated TickFlow, Tushare, Tasks10-13,
  BaoStock Normalize/Quality Gate/immutable Parquet/SHA-256/Manifest/Atomic Publish and R2-F2
  golden bytes/hashes MUST remain unchanged.

## Non-Functional Requirements

- NFR-1 (fail closed): Partial response, partial request graph, partial evidence, unknown schema,
  ambiguous semantics or a changed descriptor never produces a candidate or increments the window.
- NFR-2 (canonical isolation): Canonical roots are read-only capabilities. Pre/post manifest,
  Parquet and pointer fingerprints must be identical for every success/failure test and real run.
- NFR-3 (determinism): All identities and hashes derive from versioned semantic inputs, sorted
  symbol/shard order and canonical JSON, never process timing or dictionary iteration order.
- NFR-4 (bounds): One session has <=40 sequential requests, <=100 symbols/request, <=8 MiB/response,
  <=4000 rows/evidence, one attempt/request and bounded metadata/file/DB sizes.
- NFR-5 (security): No secret is read in Free mode. Logs, exceptions, reports and API/CLI output
  exclude raw payload, symbols, URL, headers, cookies, environment dumps and provider exception text.
- NFR-6 (atomicity): Staging cannot be read as evidence/candidate. Only an exclusive directory
  rename plus commit marker makes a bundle visible; DB references are attached after bundle commit.
- NFR-7 (concurrency): Network, bundle and DB locks are disjoint. One job/session lease and CAS
  prevents duplicate qualification, and stale/crashed leases cannot produce terminal success.
- NFR-8 (compatibility): Existing registry schema, provider states, public status response, evidence
  hashes, candidate hashes, selection bytes and Tasks10-13 terminal graph are byte-compatible.
- NFR-9 (reviewability): Production changes are preceded by witnessed RED tests and followed by
  focused, related, full offline, lint/format/compile/diff, strict-spec and independent read-only
  review gates.
- NFR-10 (availability claims): `SHADOW_QUALIFIED` means 20-session historical whole-session OHLC
  parity only. It must not be described as factor-ready, publication-ready, automatic failover-ready
  or a completed long-term production soak.

## Acceptance Criteria

### AC-1: Legacy canonical projection is strict (FR-4-FR-7, NFR-1, NFR-2)

Given the real-shaped legacy five-field manifest and a valid BaoStock partition, when the Daily
reader opens one date, then it returns an exact active-stock projection with
`lineage_state=LEGACY_UNAVAILABLE` and deterministic manifest/partition/universe/exclusion hashes.
Missing/extra descriptor fields, unsafe path, bad hash/count/schema, unknown exchange/security/
quality state, duplicate symbol or a mid-read mutation returns unavailable and performs zero writes.

### AC-2: Full-session plan is deterministic (FR-8-FR-11, NFR-3, NFR-4)

Given 3,193 eligible canonical stocks, when a plan is built twice, then it contains 32 sequential
daily shards with identical sorted membership and hashes, no metadata/factor/realtime/minute call,
and every shard has at most 100 symbols, one attempt and `adjust=none`.

### AC-3: Task14 sample cannot qualify (FR-1, FR-9, FR-19)

Given the fixed-five Task14 plan/evidence or any evidence whose expected universe is not the exact
canonical snapshot, when candidate construction is attempted, then it fails closed and the sidecar
DB/window remains unchanged.

### AC-4: Every shard is all-or-nothing (FR-12-FR-16, NFR-1, NFR-6)

Given a timeout, 429, 5xx, short/oversize/malformed response, missing/extra/duplicate symbol,
wrong date, invalid OHLC or crash at any shard, when execute runs, then later shards are not called,
no readable evidence/candidate exists, the failure audit is sanitized, and the session cannot count.

### AC-5: Final-success evidence only (FR-15, FR-16, FR-17, FR-18, NFR-5, NFR-6)

Given all shards succeed exactly once, when evidence commits, then every planned ordinal and only
its final success page is hash-bound and readable. Given any failed partial attempt, no row or byte
from that attempt appears in the committed evidence, report or candidate.

### AC-6: Daily quality is factor-independent (FR-13, FR-14, FR-19, FR-20, FR-21, FR-22)

Given complete exact-date OHLC and unknown units/factor/suspension semantics, when the Daily
candidate is built, then its Daily self-quality may pass while `units=UNKNOWN`,
`suspension=UNKNOWN`, `factor=UNQUALIFIED`. The same evidence remains invalid for the existing full
provider/factor candidate path.

### AC-7: Reconciliation is exact whole-session OHLC (FR-20-FR-23)

Given exact symbol equality and every OHLC cell within `0.01`, when reconciliation completes, then
the report is ready. One missing/extra symbol or one cell beyond the tolerance makes the whole
session mismatch. Volume, amount and adjusted returns are not read as comparison inputs.

### AC-8: Canonical change aborts without side effect (FR-23, FR-24, NFR-2)

Given the canonical manifest or partition changes between the pre- and post-comparison checks, when
the worker finishes, then the outcome is unavailable, no candidate/terminal success is attached,
the window does not increment and no canonical byte is changed.

### AC-9: Sidecar transaction and read-only status are closed (FR-24, FR-25, FR-26)

Given crash points before/after evidence commit, candidate rename and DB transaction, when state is
read, then no incomplete graph is terminal. Missing/corrupt/uninitialized DB or bundle reports
unavailable through CLI/API with no file/table/migration creation.

### AC-10: Consecutive window resets correctly (FR-27, FR-28)

Given sessions 1-10 pass and session 11 fails, gaps, duplicates or changes any version/calendar/
universe-policy hash, when the terminal transaction runs, then the prior epoch becomes reset and
the consecutive count returns to zero. Normal session-local active-set changes do not reset it. All
11 immutable reports remain queryable.

### AC-11: Twentieth session qualifies only Daily Bar (FR-27-FR-29, NFR-10)

Given exactly 20 consecutive confirmed dates with the same version vector and terminal Daily
success graphs, when session 20 commits, then the Daily capability becomes `SHADOW_QUALIFIED`.
Provider admission is unchanged, adjustment factor remains unqualified, publication/failover remain
false and the output is labeled historical shadow rather than live soak.

### AC-12: CLI, circuit and scheduler are bounded (FR-30, FR-31, FR-32)

Given plan mode, missing acknowledgement/review, OPEN circuit or HALF_OPEN probe, when the command
runs, then plan/rejection/open skip/probe are bounded and no whole-session work follows a probe.
One execute or scheduler slot can attempt only one date and exposes only sanitized counts/states.

### AC-13: Existing contracts remain byte-compatible (FR-33, FR-34, NFR-8)

Given the complete offline suite and frozen golden fixtures, when the Daily sidecar is installed,
then BaoStock canonical bytes/hashes, R2-F2 evidence/candidate/selection, Task14 discovery and
Tasks10-13 registry/terminal outputs are unchanged and no symbol-level mixing path exists.

## Edge Cases and Error Scenarios

- EC-1: Canonical manifest has both legacy and a partial subset of R2-F2 lineage fields -> reject;
  only exact legacy or exact complete lineage is accepted.
- EC-2: The trade-date partition contains two indexes plus stocks -> exclude only exact index rows;
  unknown or duplicate security types reject the projection.
- EC-3: A canonical stock is marked suspended/non-trading, including the exact reviewed `partial`
  suspended-placeholder form -> hash into exclusions and make no TickFlow request for it; do not
  infer provider suspension semantics.
- EC-4: Eligible symbol count is zero or above 4,000 -> unavailable before client construction.
- EC-5: Provider response echoes a lowercase, aliased or unexpected symbol -> fail closed; do not
  normalize a response key that differs from the exact requested provider symbol.
- EC-6: One shard returns two timestamps, zero rows or one timestamp outside the requested
  Asia/Shanghai trade date -> whole session invalid; never choose a convenient row or accept the
  next local session merely because its timestamp remains inside the requested UTC natural day.
- EC-7: HTTP client or SDK initializer closes incorrectly -> execution unavailable and no evidence.
- EC-8: A 429 or transport failure opens the capability endpoint circuit at its frozen threshold;
  later scheduled slots skip until cooldown without repeatedly requesting the whole universe.
- EC-9: HALF_OPEN fixed-five probe succeeds -> circuit closes, but the current slot ends; the next
  separately authorized slot may attempt a full session.
- EC-10: Evidence publishes but candidate validation fails -> immutable evidence may remain for
  audit, no candidate/window increment is attached, and the session report is unavailable/failure.
- EC-11: Candidate bundle commits but DB transaction crashes -> orphan bundle is not terminal and
  may be identified by bounded audit; retry uses idempotent hashes and cannot double-count.
- EC-12: Same date is submitted twice -> one immutable terminal success at most; duplicate request
  returns the existing sanitized result without provider I/O.
- EC-13: Calendar changes or the next expected date is unavailable -> reset/unavailable; weekdays
  are never guessed.
- EC-14: Twenty valid historical dates are fetched in one day -> may qualify historical Daily OHLC
  parity only; it does not satisfy R2-F5 elapsed production soak.
- EC-15: Daily qualifies while factor/units/suspension remain unresolved -> status shows the mixed
  capability states explicitly and all publication/failover gates remain closed.
- EC-16: Provider or canonical failure occurs -> no old provider observation may be relabeled as a
  new date and no gap may be skipped.

## API Contracts

Python command surface:

```text
market-provider-daily-shadow --provider tickflow --date YYYY-MM-DD
market-provider-daily-shadow --provider tickflow --date YYYY-MM-DD --execute \
  --external-authorization-id SAFE_ID --acknowledge-provider-requests
```

The default command reads only the canonical snapshot and reviewed sidecar status, computes the
bounded plan and returns `provider_requests=0`, `writes=false`. Execute attempts exactly one date.

Read-only HTTP surface:

```text
GET /api/v1/market/provider-daily-bar-shadow?provider=tickflow
```

```typescript
type DailyBarShadowState =
  | "PENDING"
  | "OBSERVING"
  | "SHADOW_QUALIFIED"
  | "RESET"
  | "UNAVAILABLE";

interface DailyBarShadowStatus {
  status: "ready" | "unavailable";
  provider: "tickflow";
  profile: "TICKFLOW_FREE_DAILY_BAR_OHLC_V1";
  state: DailyBarShadowState;
  epochId?: string;
  consecutiveSessions: number;
  requiredSessions: 20;
  firstTradeDate?: string;
  lastTradeDate?: string;
  lastOutcome?: "SUCCESS" | "FAILURE" | "MISMATCH" | "UNAVAILABLE" | "SKIPPED_CIRCUIT_OPEN";
  observationMode: "HISTORICAL_SHADOW";
  versionVectorSha256?: string;
  unitsState: "UNKNOWN";
  suspensionSemanticsState: "UNKNOWN";
  factorEvidenceState: "UNQUALIFIED";
  dailyBarQualified: boolean;
  adjustmentFactorQualified: false;
  publicationEligible: false;
  failoverEnabled: false;
  unavailableReason?: string;
}
```

Execute result is a bounded projection of the same state plus `trade_date`, `request_count`,
`expected_symbols`, `observed_symbols`, `outcome`, evidence/candidate/reconciliation SHA-256 values
when successful, circuit state and literal false canonical/publication/failover write flags.

## Data Models

| Entity | Required fields and invariants |
| --- | --- |
| `DailyCanonicalSnapshot` | date, lineage state, manifest generation/SHA/fingerprint, partition path/SHA/row count/fingerprint, active/excluded counts and hashes, exact active OHLC aggregate hash; frozen and re-verifiable |
| `DailyShadowContract` | provider/profile, Free origin, adapter/endpoint/schema/SDK/wheel/mapping/universe-policy/per-session-symbol-set-binding/candidate/reconciliation/terms hashes, shard/request/byte/time bounds, semantic states; exact reviewed descriptor |
| `DailyShadowPlan` | job/window/epoch/date, canonical snapshot/full-universe/active-symbol-set/mapping hashes, sorted <=100-symbol shards, exact ordinal set, request-plan SHA |
| `ShadowEvidenceManifest` | reused unchanged; capability identity is bound by Daily window/job/request fields; final-success pages only |
| `DailyBarShadowCandidate` | identities/hashes listed by FR-19, per-session universe/mapping plus stable universe-policy hash, exact counts, semantic states, `quality_verdict=PASS`; no factor/adjusted-return fields |
| `DailyBarReconciliationReport` | exact set/counts, four price-cell comparison counts, max absolute deltas, tolerance `0.01`, verdict and report SHA; no activity/factor metrics |
| `DailyShadowAttemptAudit` | fixed endpoint/shard ordinal, attempt, request/byte/time counters, allowlisted outcome/failure; zero payload/URL/provider text |
| `DailyShadowSessionReport` | immutable epoch/job/date/version/calendar/universe/active-symbol-set/evidence/candidate/reconciliation hashes and terminal outcome |
| `DailyShadowWindow` | provider/profile/epoch, state, exact stable version/calendar/universe-policy hashes, first/last/next dates, consecutive/required counts, last report, CAS version |
| `DailyShadowCircuit` | endpoint, CLOSED/OPEN/HALF_OPEN, failure count, cooldown/probe lease, CAS version |

The sidecar schema owns only these capability records and references. It has no foreign key or
write authority to the canonical dataset, R2-F2 candidate/selection tables or provider admission
table.

## Out of Scope

- OS-1: Adjustment-factor discovery/qualification, factor anchor/direction, adjusted returns and
  corporate-action equivalence.
- OS-2: Volume/amount unit qualification or comparison, suspension/no-row semantics, provider
  listing-universe qualification, indexes, realtime, minute, financials or authenticated service.
- OS-3: Canonical publication, pointer/manifest/Parquet mutation, provider selection, symbol-level
  mixing, fallback or automatic failover.
- OS-4: Changing the existing Tasks10-13 registry schema/models/hashes, provider-level
  qualification or Task14 fixed-five discovery contract.
- OS-5: Claiming TickFlow legal/operational longevity, quota, raw-retention rights, factor readiness
  or full replacement fitness from Daily OHLC parity.
- OS-6: R2-F5 live elapsed 20-session production soak, chaos drills, primary-failure failover drill,
  NAS replication and Release 2 re-entry.
- OS-7: Retrying provider calls, increasing timeouts, shortening cooldown, lowering coverage or
  tolerance, using an old observation as a new session, silently skipping gaps or hand-editing any
  evidence/candidate/DB/canonical object.
