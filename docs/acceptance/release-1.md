# Release 1 Acceptance

Status: **GO — installed functionality passed and a fresh quiet-window AC-12 rerun kept every
protected store byte-for-byte and state-for-state identical.**

R1-E adds immutable daily regime snapshots and a read-only replay audit. Historical
`post_hoc_backfill` records prove deterministic results against the verified publication available
at audit time; they do not prove that the classification generation was visible on each historical
date. The installed audit therefore reports post-hoc availability and contemporaneous visibility
separately.

## Reviewed and installed release

- Main was fast-forwarded without touching the user-owned dirty roadmap, PDF or root package files.
- The independent combined reviewer approved code commit
  `99c84d012ac1eacdd9291629ea737185f4a0fb3d` with no P0/P1 findings.
- The first official installer pass reported `mutation=false`, then installed that exact code
  commit. `RELEASE.json` recorded the same Git SHA and `built_at=2026-08-11T09:38:13Z`.
- The fresh AC-12 pass reran the official installer from current main after the first acceptance
  evidence commit. Its preflight again reported `mutation=false`; the installed release is
  `75c64e4364fadbeab8a1e4a964bfa208e91a7ff6`, with the reviewed code unchanged and
  `built_at=2026-08-11T10:42:52Z`.
- All five LaunchAgents are loaded. API, workspace and local dataset readiness are ready. The
  installed dataset generation is `generation-6c48729bea29429d86891b102f0c8e52`.
- The installer reported the local mirror was newer than the mounted NAS archive and copied zero
  bytes.

## Integrated verification

The reviewed release tree at the installed SHA passed:

```text
uv run --extra dev pytest -q --basetemp=/tmp/stock-eva-r1e-full-backend
→ 100% passed

uv run --extra dev ruff check backend tests
→ All checks passed

cd workspace && npm test -- --run
→ TypeScript passed; 14 files / 72 tests passed

cd workspace && npm run build
→ Vite production build passed

git diff --check
→ clean
```

The workspace build reported npm audit debt of one low and one high advisory. `node_modules` is a
build-only staging dependency and is removed from the installed static runtime; no dependency was
silently upgraded during this acceptance pass.

## Full manifest snapshot backfill

The verified manifest covers 271 trading dates from `2025-07-01` through `2026-08-10`, contains
271 published objects and 864,625 rows.

1. The complete-range dry-run returned `status=dry-run`, 271 selected dates and
   `writes_snapshot_data=false`. The snapshot database remained absent.
2. The explicit complete-range execution returned `inserted=271`, `idempotent=0`,
   `conflicts=0`, `errors=0` and `writes_snapshot_data=true`.
3. Repeating the exact command returned `inserted=0`, `idempotent=271`, `conflicts=0`,
   `errors=0` and `writes_snapshot_data=false`.
4. Before and after the repeat, the only snapshot file had identical SHA-256
   `d7bc106b01cd73822d4443e916b9a20a7d5daeb65273c1919ccbd659a1b75093`, size
   2,912,256 bytes and mtime `1786442281677234721` ns. No sidecar appeared.
5. The fresh AC-12 quiet-window repeat covered the same 271 dates and returned
   `inserted=0`, `idempotent=271`, `conflicts=0`, `errors=0` and
   `writes_snapshot_data=false` from the installed runtime. It ran from 18:43:24 to 19:02:27 CST.

## Twenty-session replay audit

`release-one-audit --sessions 20` completed within the 180-second budget and returned
`status=ready`, `writes_snapshot_data=false`:

- range `2026-07-14` through `2026-08-10`;
- `snapshot_count=20`, `matching_result_count=20`;
- `post_hoc_verified_sessions=20`, `after_close=0`;
- `classification_available_sessions=2`, `contemporaneous_classification_sessions=1`,
  `contemporaneous_unverified_sessions=19`;
- current classification coverage 5,202 / 5,205 = 99.9424%, with `sh.603468`, `sz.001232` and
  `sz.301707` unmapped;
- every future counter was zero: price, lineage, evidence, membership and effective-window reads;
- the honest quality issues were `classification_history_observed_late` and
  `classification_history_unavailable`.

The audit does not relabel post-hoc classification as historically known evidence.

## Exact API and installed browser

The exact persisted GET for `2026-08-10` returned HTTP 200, the expected dataset generation and a
path-free snapshot. Its result is honestly `degraded`, with `range / neutral`, rather than a full
A-share bull/bear claim. Twenty serial warm HTTP reads had p95 2.797 ms and max 3.180 ms, below the
500 ms budget.

The fresh installed-runtime readback again returned HTTP 200, the same generation and a path-free
`degraded` snapshot. Its twenty serial reads had p95 3.076 ms and max 23.192 ms.

Installed-browser checks used real backend data:

- At 1440×900, overview → sector workspace → `M73研究和试验发展` →
  `sh.603259` → sector preserved `as_of=2026-08-11`, taxonomy and sector in the URL and restored
  the same selected sector and leader.
- Market and sector views showed backend order, raw scores, support and contrary evidence,
  `narrow_provisional`, the full-A-share limitation and missing Release 2 fund-flow evidence.
- The stock cockpit loaded 260 valid sessions with QFQ daily K, real volume, MA5/10/20/60/120/250,
  MACD and RSI14. It retained market, sector and leader context and contained no buy/sell advice.
- At 390×844, the sector and stock views had no document-level horizontal overflow
  (`scrollWidth=375`, `innerWidth=390`), and the real sector, leader, indicators and missing
  fund-flow boundary remained visible.
- Desktop and mobile console warning/error lists were empty.
- The mobile return URL preserved the exact sector context. The temporary tab was finalized before
  the asynchronously restored sector content was independently re-read on mobile; desktop real-data
  restoration and the full frontend route tests cover that content restoration, but this document
  does not claim a second independent mobile post-return content read.
- After the fresh reinstall, a representative installed-browser pass again loaded the real overview,
  opened `M73研究和试验发展`, entered `sh.603259`, observed 260 valid sessions with QFQ daily K,
  volume, MA5/20/60/250, MACD and RSI14, and returned to the exact sector URL with the same sector,
  leader and score restored. It found no buy/sell recommendation wording. This fresh pass does not
  add a new console-log claim because that browser client's console-message API was unavailable;
  the earlier full browser pass above retains its independently recorded clean-console evidence.

## Protected-store fingerprint result

### Fresh quiet-window closure

A new baseline was frozen at 18:42 CST, after the scheduled 18:40 refresh had completed and before
the official reinstall. The final matrix was read at 19:11:52 CST, before the next scheduled refresh
at 19:20. The window included the official reinstall, the full 271-date idempotent replay, the
20-session audit, exact API reads and the representative installed-browser flow.

Every protected target and same-name sidecar inventory was identical at both ends:

- local immutable dataset tree: 292 entries, content
  `9a1dbf35d0b018ed05c254bc9aebf0ab9f452a652708e4c10ebf80ac79f69bba`, state
  `adfa1ab402550715f69bcb383faf1e4f3f87cd19b2b442094c76484c906f0f00`;
- mounted NAS archive tree: 284 entries, content
  `29032a7c32d5d29a291947e4694ab20d9b8ecb9ccfa42c38806efd375e0db9f1`, state
  `6aa2198bab0843a4f6721b95ecfd557544bb960e9b91e1a215b8a72b1192d0d0`;
- market control DB `099dae430c46598a4e13edda1642f120c0569d59cae79140c63de9906361f314`;
- classification DB `45fd786ca80dd760e79b28432d3929de1b8da76111e265ea152ef5a8388f1941`;
- supplemental audit DB `45df0cfce85d8282a41feed7fae7b795f70df9a67e7d7b2e766dd6a2172c03dc`;
- private user DB `8adba0364d399a56270d3a051db5db8d04fd0243cdecb32bdb01a81384e57e32`;
- regime snapshot DB `d7bc106b01cd73822d4443e916b9a20a7d5daeb65273c1919ccbd659a1b75093`,
  size 2,912,256 bytes and unchanged mtime. No SQLite sidecar appeared.

The installer again classified the local mirror as newer than the mounted NAS archive and copied
zero data bytes. This fresh matrix closes AC-12 without suspending or modifying any LaunchAgent.

### Historical confounded window

The initial and final fingerprints were identical for:

- local immutable dataset tree: content
  `45b7ad6d80e5cfc1c6efc8b1093229d50b674366189a8cb9e4dd1d55471ec6bc` and state
  `3964e54c1d340afed034e628745ee85fb48a4a0bedc8446f84ebbf28ae89becf`;
- mounted NAS archive tree: content
  `4d26572dce7093b0c26bd062ebd6fc5791589d8de9a099b7d78818e294b61ca9` and state
  `8ea277c3234e7c35b8c721c5ac70b9b2249e5bf1f3191938eca269f6fc8b777d`;
- classification DB `45fd786ca80dd760e79b28432d3929de1b8da76111e265ea152ef5a8388f1941`;
- supplemental audit DB `45df0cfce85d8282a41feed7fae7b795f70df9a67e7d7b2e766dd6a2172c03dc`;
- private user DB `8adba0364d399a56270d3a051db5db8d04fd0243cdecb32bdb01a81384e57e32`.

The snapshot DB was the expected product write: absent before backfill, then the 271-record file
described above.

The market control DB was **not** byte-identical across the original pre-install-to-final window:

- before: SHA-256 `cf961f5d74e778cff86c3f1779ac1b5aa7d62005697c96be1f7dc0c491494040`,
  mtime `2026-08-11T07:15:19+08:00`;
- after: SHA-256 `75c0d2321cccbcef60990ef80a0003936d89f72bb1b7efda2809c0dfd9864694`,
  mtime `2026-08-11T18:11:30+08:00`.

The acceptance run crossed the normal 18:10 after-close schedule. `refresh.log` records that the
refresh LaunchAgent started the `2026-08-11` attempt at 18:10:12 and completed its expected
`retry_wait/provider_error` state update at 18:11:30, exactly matching the DB mtime. The immutable
dataset tree remained unchanged because the provider failed before publication.

After that scheduled writer finished, a fresh controlled fingerprint window ran an explicit
one-date idempotent snapshot execution (`inserted=0`, `idempotent=1`,
`writes_snapshot_data=false`) plus an exact GET. Every protected fingerprint, including market
control and the snapshot DB SHA/mtime, was byte-for-byte identical before and after that controlled
window. This proves the R1-E operations were read-only/idempotent in the controlled window, but it
does not make the original pre-install-to-final fingerprints identical.

## Verdict and next gate

The code review, full tests, installation, complete backfill, idempotency, replay, API performance,
browser evidence and fresh protected-store matrix are accepted. **Release 1 is GO.** The historical
18:10 mismatch remains documented as a scheduling-confounded attempt rather than being rewritten as
a pass. R2 has not started and remains locked pending explicit user confirmation to continue.
