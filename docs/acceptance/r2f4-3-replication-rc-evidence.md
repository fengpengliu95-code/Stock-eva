# R2-F4.3 RC acceptance evidence

Status: Candidate / NO-GO pending the next independent SPEC and QUALITY audit.
No independent GO is claimed. The boundary is LOCAL_CHAIN_ONLY: all commands
used local temporary fixtures and fakes; no real NAS/SMB, provider, credential,
LaunchAgent installation, or production operation occurred.

## Reviewed commits

reviewed_implementation_spec_commit: 9cf91afb55c64940419e45dfc1d2a71e2b4051c9
evidence_base_commit: 9cf91afb55c64940419e45dfc1d2a71e2b4051c9

X6 is the implementation/spec/test closure commit, reviewed and tested from
its clean tree. It retains NFR-11's one immutable sidecar generation with
10,000 terminal dead-letter heads/intents/events, public status warmup and
five monotonic samples below 500 ms, with a whole-sidecar physical
fingerprint before/after proving zero writes. Every missing, corrupt, locked,
empty, healthy, retry-wait, dead-lettered, and replicated case now receives a
real HTTP GET with before/after sidecar fingerprints, exact full JSON equality,
replicated digest/time checks, redaction, and zero provider requests. EC-25
starts from an existing ready pointer/binding and strictly fingerprints
manifest, binding, and pointer artifacts, distinguishing exact prior-state
preservation from the one expected new ready state across five fault points.
EC-38 independently runs both CLI dry-run commands for each configured root in
direct, symlink-descendant, and symlink-ancestor scenarios, with full-tree
fingerprint equality. Both normative specs remain exactly equal to the
129-row matrix projection.

Y6 is this evidence-only successor. Its own hash is deliberately not embedded;
after committing Y6, `git diff 9cf91afb55c64940419e45dfc1d2a71e2b4051c..Y6
--name-only` is the proof that only this document changed.

## Traceability evidence at X6

The authoritative matrix contains 129 rows (42 FR, 16 NFR, 31 AC, 40 EC),
90 distinct direct anchors, 154 requirement-to-anchor mappings, and maximum
anchor reuse 4. Every anchor resolves in the AST catalog. Every AC has exact
known FR/NFR parents and nonblank Required tests; both normative documents
match the matrix row-for-row. The semantic digest is
`b8f147c5ff45568a241f7c36047b28ca0d73a0e4d3cc9a6922cf07d8cb0b5bb3`.
That digest is a drift guard, not semantic proof: the matrix's requirement
summaries, reviewed test bodies, direct rationales and manual review remain
the evidence. The validator's body-token check is explicitly heuristic.

Generated from the matrix and AST catalog on X6:

    ./.venv/bin/pytest -q -rA $(awk -F'::' '{split($1,a,":"); print a[1] "::" $2}' /tmp/r2mapped_nodes_x6.txt)

This catalog contained 90 concrete mapped anchors and the generated command
executed 100 parameterized pytest cases; all 100 passed. The focused
replication/restore/CLI/spec command collected 219 nodes and all 219 passed.

## Full and static verification at X6

    ./.venv/bin/pytest -q -rA 2>&1 | rg '^PASSED ' | wc -l
    2961

The full suite passed all 2961 collected/executed tests. Only existing
deprecation warnings were emitted; there were no test failures. The focused
suite had no concurrency flake. The existing multiprocess tests still emit
the known Python 3.12 fork deprecation warning; no rerun was needed for a
failure.

    ./.venv/bin/ruff check backend tests
    All checks passed; exit 0
    ./.venv/bin/ruff format --check backend tests
    Changed files already formatted; exit 0
    ./.venv/bin/python -m compileall -q backend tests
    exit 0
    git diff --check
    exit 0
    ./.venv/bin/pytest -q tests/test_r2f4_3_spec_crosswalk.py
    7 passed; exit 0

## CLI and safety boundary

Offline CLI probes at X6 cover typed `dry_run`, explicit execute dispatch to
the injected central worker with `operation_day`, rejected mutable-root/path
aliases across every configured mutable root, acknowledged init semantics,
and sanitized status JSON. The status matrix asserts eight public states are
exactly projected without writes or provider requests. Unknown/orphan state
remains manual-review or quarantine only; manual rollback must never replace
a newer valid canonical generation.

The real-NAS follow-up checklist remains: independently review descriptor and
mount authorization, perform a controlled destination initialization, run one
bounded claim under both gates, capture descriptor/readback/audit evidence,
and verify rollback/manual unknown-orphan handling. No such operation is part
of X6/Y6. Status remains Candidate / NO-GO pending the next independent audit.
