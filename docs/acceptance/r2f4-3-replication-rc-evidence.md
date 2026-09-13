# R2-F4.3 RC acceptance evidence (draft)

Status: Candidate / NO-GO pending independent audit; no independent QUALITY
GO is claimed. Execution boundary: `LOCAL_CHAIN_ONLY`, disabled replication,
no real NAS/provider/production operation.

## Commit and commands

The exact implementation commit recorded after the implementation commit is
`reviewed_commit: 46bde46d3effc19db11c955b45706423439bd00d`.

Commands and results to record:

```text
focused pytest: PASS/FAIL (actual output below)
full pytest: PASS/FAIL (actual output below)
ruff baseline at parent HEAD `0f90c3a8820995d32b5bdde6ae31c7c1322e65e0`: 13 existing
errors in `replace_markdown_kline.py` and `scripts/markdown_to_pdf.py` (the
untracked RC validator is excluded from that baseline). These unrelated
legacy errors are a documented non-regression waiver; changed backend/tests
files must still be clean.
ruff backend/tests and changed files: PASS/FAIL
ruff format --check: PASS/FAIL
compileall: PASS/FAIL
crosswalk/spec validator: PASS/FAIL
git diff --check: PASS/FAIL
```

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
