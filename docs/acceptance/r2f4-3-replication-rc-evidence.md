# R2-F4.3 RC acceptance evidence

Status: Candidate / NO-GO pending the next independent SPEC and QUALITY audit.
No independent GO is claimed. The boundary is LOCAL_CHAIN_ONLY: all commands
used local temporary fixtures and fakes; no real NAS/SMB, provider, credential,
LaunchAgent installation, or production operation occurred.

## Reviewed commits

reviewed_implementation_spec_commit: 2a65ba477a0425554bf03caf409b178a49030ba2
evidence_base_commit: 2a65ba477a0425554bf03caf409b178a49030ba2

X3 is the implementation/spec/test closure commit. It adds direct NFR-11
10,000-row performance evidence, production writer AST inventory, LaunchAgent
and TCC asset checks, explicit config/layout and plain-store byte checks,
post-commit enqueue fault/reconciliation evidence, a parameterized read-only
status matrix, and an injected CLI execute-path test. It also makes both
normative specs exactly equal to the reviewed 129-row matrix projection,
including nonblank AC FR/NFR parents and Required tests.

Y3 is this evidence-only successor. Its own hash is deliberately not embedded;
after committing Y3, `git diff 2a65ba477a0425554bf03caf409b178a49030ba2..Y3 --name-only` is the proof that only
this document changed.

## Traceability evidence at X3

The authoritative matrix contains 129 rows (42 FR, 16 NFR, 31 AC, 40 EC),
88 distinct direct anchors, 155 requirement-to-anchor mappings, and maximum
anchor reuse 4. Every anchor resolves in the AST catalog. Every AC has exact
known FR/NFR parents and nonblank Required tests; both normative documents
match the matrix row-for-row. The semantic digest is
`83df3978d39c0142529199f3f1b72a6f0aec63779421fa802ff20941e58d19dc`.
That digest is a drift guard, not semantic proof: the matrix's requirement
summaries, reviewed test bodies, direct rationales and manual review remain
the evidence. The validator's body-token check is explicitly heuristic.

Generated from the matrix and AST catalog on X3:

    ./.venv/bin/pytest -q $(tr '\n' ' ' < /tmp/r2mapped_nodes_x3.txt)

This collected 95 concrete mapped pytest nodes and all 95 passed. The focused
replication/restore/CLI/spec command collected 212 nodes and all 212 passed.

## Full and static verification at X3

    ./.venv/bin/pytest -q -rA 2>&1 | rg '^PASSED ' | wc -l
    2954

The full suite passed all 2954 collected/executed tests. Only existing
deprecation warnings were emitted; there were no test failures. The focused
suite had no concurrency flake. The existing multiprocess tests still emit
the known Python 3.12 fork deprecation warning; no rerun was needed for a
failure.

    ./.venv/bin/ruff check backend tests
    All checks passed; exit 0
    ./.venv/bin/ruff format --check backend tests
    221 files already formatted; exit 0
    ./.venv/bin/python -m compileall -q backend tests
    exit 0
    git diff --check
    exit 0
    ./.venv/bin/pytest -q tests/test_r2f4_3_spec_crosswalk.py
    7 passed; exit 0

## CLI and safety boundary

Offline CLI probes at X3 cover typed `dry_run`, explicit execute dispatch to
the injected central worker with `operation_day`, rejected mutable-root/path
aliases, acknowledged init semantics, and sanitized status JSON. The status
matrix asserts missing, corrupt, locked and healthy states are zero-write and
provider-free. Unknown/orphan state remains manual-review or quarantine only;
manual rollback must never replace a newer valid canonical generation.

The real-NAS follow-up checklist remains: independently review descriptor and
mount authorization, perform a controlled destination initialization, run one
bounded claim under both gates, capture descriptor/readback/audit evidence,
and verify rollback/manual unknown-orphan handling. No such operation is part
of X3/Y3.
