# R2-F4.3 RC acceptance evidence (draft)

Status: Candidate / NO-GO pending independent audit; no independent QUALITY
GO is claimed. Execution boundary: `LOCAL_CHAIN_ONLY`, disabled replication,
no real NAS/provider/production operation.

## Commit and commands

The audited implementation commit is
`implementation_commit: 46bde46d3effc19db11c955b45706423439bd00d`.
The evidence-base commit is
`evidence_base_commit: bbd96e642a76da11f0c1e5f0d1d9e0f58e60f7a7`.
The evidence commit is a documentation/test-only successor, so the report
does not make an impossible self-referential commit claim.

Commands and results to record:

```text
focused pytest at evidence base: PASS, 196 tests
full pytest at evidence base: PASS, 2938 tests
current closure focused RC scope: PASS, 98 tests collected
current closure full suite: PASS, 2942 tests; an earlier spawn-contention flake in
test_eight_processes_same_bundle_identity_are_idempotent was rerun successfully.
ruff baseline at parent HEAD `0f90c3a8820995d32b5bdde6ae31c7c1322e65e0`: 13 existing
errors in `replace_markdown_kline.py` and `scripts/markdown_to_pdf.py` (the
untracked RC validator is excluded from that baseline). These unrelated
legacy errors are a documented non-regression waiver; changed backend/tests
files must still be clean.
ruff backend/tests and changed files: PASS (scoped changed-file run; 13 unrelated baseline errors waived)
ruff format --check: PASS
compileall: PASS
crosswalk/spec validator: PASS (129 IDs, 129 real anchors, exact docs equality)
git diff --check: PASS
```

Audited command forms were `./.venv/bin/pytest -q` (2938 passed at the
evidence base) and the focused replication/restore/static command set (196
passed). The current closure reruns the same gates plus the new RC tests; all
focused RC tests pass, and the final current `./.venv/bin/pytest -q` completed
with 2942 passed.

The release record must include `git rev-parse HEAD`, `git status --short`,
and `git diff --stat`, plus the sanitized CLI dry-run payloads showing typed
`dry_run`, no writes, and zero provider requests. It must explicitly retain
the disabled/no-real-NAS boundary and the `LOCAL_CHAIN_ONLY` residual.

## Residual and rollback

Unknown/orphan destination state is a manual-review condition. Preserve the
canonical ready pointer, immutable binding, and journal; quarantine only under
an approved recovery operation. No automatic rollback may replace a newer
valid canonical generation. A later real-NAS window must follow the runbook
checklist and obtain a new explicit approval.
