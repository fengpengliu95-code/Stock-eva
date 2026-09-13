# R2-F4.3 RC acceptance evidence

Status: Candidate / NO-GO pending the next independent SPEC and QUALITY audit.
No independent GO is claimed. The evidence boundary is LOCAL_CHAIN_ONLY:
replication is disabled and this record claims no real NAS/SMB, provider,
LaunchAgent, credential, or production operation.

## Reviewed commits and scope

reviewed_implementation_spec_commit: b1a5492de25e29d6bc3d4a5b807cefa864568a6c

Commit X above is the clean semantic-spec/test implementation commit reviewed by
the commands below. It restores the effective 42 FR, 16 NFR, 31 AC, and 40 EC
matrix, adds the semantic-matrix digest guard, and tests the Asia/Shanghai
operation-day cutoff immediately before, at, and after the boundary.

evidence_base_commit: b1a5492de25e29d6bc3d4a5b807cefa864568a6c

Commit Y is this metadata-only evidence successor. Its own hash is intentionally
not embedded in its content: after committing Y, git diff X..Y --name-only is the
non-self-referential proof that only this acceptance evidence file changed.
The historical implementation/evidence pair remains
46bde46d3effc19db11c955b45706423439bd00d /
bbd96e642a76da11f0c1e5f0d1d9e0f58e60f7a7; it is retained only as prior audit
context, not as the reviewed spec commit for this record.

Commit X diff scope was exactly:

backend/app/storage/replication.py
docs/plans/2026-09-09-stock-eva-r2f4-3-replication-restore-design.md
docs/plans/2026-09-09-stock-eva-r2f4-3-replication-restore-implementation.md
tests/test_dataset_replication.py
tests/test_r2f4_3_spec_crosswalk.py

## Exact commands and observed results at X

./.venv/bin/pytest -q
2942 tests passed; exit 0. Warnings were deprecations only.

./.venv/bin/pytest -q tests/test_r2f4_3_spec_crosswalk.py tests/test_replication_cli_contract.py tests/test_replication_integration_batch5.py tests/test_dataset_replication.py
98 tests collected and passed; exit 0.

./.venv/bin/pytest -q tests/test_dataset_replication.py::test_claim_due_operation_day_filters_source_publication_with_fake_clock
1 test passed; before-cutoff and exact-cutoff claims were eligible, after-cutoff was not.

./.venv/bin/ruff check backend tests
All checks passed; exit 0.
./.venv/bin/ruff check backend/app/storage/replication.py tests/test_dataset_replication.py tests/test_r2f4_3_spec_crosswalk.py
All checks passed; exit 0.
./.venv/bin/ruff format --check backend/app/storage/replication.py tests/test_dataset_replication.py tests/test_r2f4_3_spec_crosswalk.py
3 files already formatted; exit 0.
./.venv/bin/python -m compileall -q backend tests
exit 0.
git diff --check
exit 0.

The pre-RC parent audit recorded a 13-error unrelated Ruff baseline and a
non-regression waiver for it. A fresh direct ruff check backend tests at X is
clean, so no current changed-file waiver is needed; the historical note remains
for audit traceability.

The crosswalk validator observed 129 unique IDs, 129 real AST anchors, identical
rows in both normative documents, at least 120 distinct descriptions, and the
approved semantic matrix SHA-256
5aac85dbc263abf4dae42a27856579c1b5c117d743cde6e37b5a81613eb6db59.

## Sanitized CLI probes

market-replicate --destination /tmp/r2f4-opday --operation-day 2026-09-14 --json
returned status=dry_run, operation_day=2026-09-14, writes=false,
destination_writes=false, and provider_requests=0 with exit 0.

market-replication-init --destination <tmp-child> --json returned
status=dry_run, writes=false, and provider_requests=0 with exit 0; the
destination child remained absent.

market-replication-status --json returned sanitized mode=status,
status=disabled, reason_code=DISABLED, provider_requests=0, all effects false,
paths_exposed=false, and trust_scope=LOCAL_CHAIN_ONLY with exit 0.

At X, git rev-parse HEAD returned the reviewed commit above, git status --short
was empty, and git diff --stat was empty. After Y, git diff X..Y --name-only must
contain only this evidence file and git status --short must again be empty.

## Residual and rollback

Unknown/orphan destination state is a manual-review or quarantine condition.
Preserve the canonical ready pointer, immutable binding, and journal. Manual
rollback must never replace a newer valid canonical generation. A later real-NAS
window requires a separate approval, exact-commit recheck, controlled empty-child
destination, descriptor/mount validation, one bounded operation, and post-run
manifest/pointer/inode/byte readback. This record is not authorization for that
window.
