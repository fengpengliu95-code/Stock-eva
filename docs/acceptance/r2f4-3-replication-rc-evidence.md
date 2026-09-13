# R2-F4.3 RC acceptance evidence

Status: Candidate / NO-GO pending the next independent SPEC and QUALITY audit.
No independent GO is claimed. The boundary is LOCAL_CHAIN_ONLY: replication is
disabled and this worktree performed no real NAS/SMB, provider, LaunchAgent,
credential, or production operation.

## Reviewed commits

reviewed_implementation_spec_commit: 337058bf026b942963979208f0080059b2400c86
evidence_base_commit: 337058bf026b942963979208f0080059b2400c86

Commit X2 is the semantic traceability implementation commit. It adds the
authoritative 129-row requirement evidence matrix, direct AST-discovered test
anchors, exact AC FR/NFR references, assertion rationales, bounded reuse checks,
and cross-document semantic drift validation.

Commit Y2 is this acceptance-evidence-only successor. Its own hash is deliberately
not embedded; after committing Y2, git diff X2..Y2 --name-only is the
non-self-referential proof that only this evidence document changed.

X2 changed exactly:

docs/plans/2026-09-09-stock-eva-r2f4-3-replication-restore-design.md
docs/plans/2026-09-09-stock-eva-r2f4-3-replication-restore-implementation.md
docs/acceptance/r2f4-3-requirement-evidence-matrix.md
tests/test_r2f4_3_spec_crosswalk.py

## Traceability evidence at X2

Matrix inventory: 129 rows (42 FR, 16 NFR, 31 AC, 40 EC), 84 distinct direct
test anchors, maximum anchor reuse 4, every anchor resolves to an AST test
function, every AC has nonblank known FR/NFR references, and every AC reference
set overlaps the direct evidence anchors for at least one referenced requirement.

Generated mapped-node command:

  ./.venv/bin/pytest -q $(tr '\n' ' ' < /tmp/r2mapped_nodes.txt)

The command was generated from the matrix and AST catalog, collected 91 concrete
pytest nodes, and all 91 passed. The broader replication/restore focused command
collected 203 tests and all 203 passed.

The validator also confirmed identical normative rows in both specs, 129 unique
IDs, 129 real anchors in the specs, at least 120 distinct requirement
descriptions, and semantic matrix digest
2f7d16fbdb385682cb64a1eb813fbd5b23c772a810ebfd383e65b5d981ad0505.
The digest is only a drift guard; the matrix's requirement summaries, exact
references, reviewed test bodies, and assertion rationales are the semantic proof.

## Full and static verification at X2

  ./.venv/bin/pytest -q
2942 tests passed; exit 0. Only deprecation warnings were emitted.

  ./.venv/bin/ruff check backend tests
All checks passed; exit 0.

  ./.venv/bin/ruff format --check backend tests
221 files already formatted; exit 0.

  ./.venv/bin/python -m compileall -q backend tests
exit 0.

  git diff --check
exit 0.

At X2, git rev-parse HEAD returned the reviewed X2 commit, git status --short
was empty, and git diff --stat was empty.

## CLI and safety boundary

Existing sanitized probes remain valid: market-replicate with an explicit
operation day returns typed dry_run with writes=false and provider_requests=0;
market-replication-init dry_run leaves its destination child absent; and
market-replication-status returns mode=status, paths_exposed=false,
provider_requests=0, and trust_scope=LOCAL_CHAIN_ONLY.

No provider request, NAS/SMB mount, production path, or credential was used.
Unknown/orphan state remains manual-review or quarantine only. Preserve canonical
ready pointers, immutable bindings, and journals; manual rollback must never
replace a newer valid canonical generation. A later real-NAS window needs a
separate approval and controlled change-window evidence.
