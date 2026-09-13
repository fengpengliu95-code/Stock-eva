# R2-F4.3 RC acceptance evidence

Status: Candidate / NO-GO pending the next independent SPEC and QUALITY audit.
No independent GO is claimed. The boundary is LOCAL_CHAIN_ONLY: all commands
used local temporary fixtures and fakes; no real NAS/SMB, provider, credential,
LaunchAgent installation, or production operation occurred.

## Reviewed commits

reviewed_implementation_spec_commit: 438fe4625f213fee37c0dca7f491671e4d3617a4
evidence_base_commit: 048ba6be27184acda1e15a45c4b6cf5b8e1a496e

X4 is the implementation/spec/test closure commit. It adds direct NFR-11
evidence using one immutable sidecar generation with 10,000 terminal
dead-letter heads/intents/events, public status warmup and five monotonic
samples with p95 below 500 ms. It expands the read-only matrix to missing,
corrupt, locked, empty, healthy, retry-wait, and dead-lettered states with
exact status/reason/count assertions and zero writes/provider requests. It
adds direct post-commit dual enqueue/journal failure reconciliation, five
manifest/binding/pointer fault points, and exhaustive mutable-root
descendant/ancestor/symlink dry-run checks for both CLI commands. The central
coordinator now catches a post-commit enqueue exception and emits a typed
degraded observation while preserving the ready pointer. Both normative specs
remain exactly equal to the reviewed 129-row matrix projection.

Y4 is this evidence-only successor. Its own hash is deliberately not embedded;
after committing Y4, `git diff 438fe4625f213fee37c0dca7f491671e4d3617a4..Y4
--name-only` is the proof that only this document changed.

## Traceability evidence at X4

The authoritative matrix contains 129 rows (42 FR, 16 NFR, 31 AC, 40 EC),
90 distinct direct anchors, 154 requirement-to-anchor mappings, and maximum
anchor reuse 4. Every anchor resolves in the AST catalog. Every AC has exact
known FR/NFR parents and nonblank Required tests; both normative documents
match the matrix row-for-row. The semantic digest is
`b8f147c5ff45568a241f7c36047b28ca0d73a0e4d3cc9a6922cf07d8cb0b5bb3`.
That digest is a drift guard, not semantic proof: the matrix's requirement
summaries, reviewed test bodies, direct rationales and manual review remain
the evidence. The validator's body-token check is explicitly heuristic.

Generated from the matrix and AST catalog on X4:

    ./.venv/bin/pytest -q $(awk -F'::' '{split($1,a,":"); print a[1] "::" $2}' /tmp/r2mapped_nodes_x4.txt)

This collected 90 concrete mapped pytest nodes and all 90 passed. The focused
replication/restore/CLI/spec command collected 219 nodes and all 219 passed.

## Full and static verification at X4

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

Offline CLI probes at X4 cover typed `dry_run`, explicit execute dispatch to
the injected central worker with `operation_day`, rejected mutable-root/path
aliases across every configured mutable root, acknowledged init semantics,
and sanitized status JSON. The status matrix asserts seven public states are
exactly projected without writes or provider requests. Unknown/orphan state
remains manual-review or quarantine only; manual rollback must never replace
a newer valid canonical generation.

The real-NAS follow-up checklist remains: independently review descriptor and
mount authorization, perform a controlled destination initialization, run one
bounded claim under both gates, capture descriptor/readback/audit evidence,
and verify rollback/manual unknown-orphan handling. No such operation is part
of X4/Y4. Status remains Candidate / NO-GO pending the next independent audit.
