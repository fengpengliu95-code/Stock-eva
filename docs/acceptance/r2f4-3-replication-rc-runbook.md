# R2-F4.3 RC runbook

Status: Candidate / NO-GO pending independent audit. Scope is
`LOCAL_CHAIN_ONLY`; never run against a real NAS, SMB mount, provider, launch
agent, or production path from this worktree.

## Verification

Run from the repository root:

```text
./.venv/bin/pytest -q tests/test_replication_integration_batch5.py tests/test_replication_cli_contract.py tests/test_r2f4_3_spec_crosswalk.py
./.venv/bin/pytest -q
./.venv/bin/ruff check backend tests
./.venv/bin/ruff format --check backend tests
./.venv/bin/python -m compileall -q backend tests
git diff --check
```

Record stdout, exit status, `git rev-parse HEAD`, and clean/diff output in the
acceptance evidence. The CLI probes must show `dry_run`, `writes: false`, and
`provider_requests: 0`; init dry-run must leave its destination absent.

## Recovery and unknown state

Do not overwrite a valid pointer or immutable binding. For an unknown or
orphan destination object, stop the operation, preserve evidence, and perform
manual quarantine/removal only after independent review. A post-pointer
close/journal failure is degraded but ready state remains canonical; reconcile
from the durable journal before a later publication. Rollback is manual and
must not replace a newer valid generation.

## Later controlled real-NAS window

Obtain a separate approval window; verify the exact commit and disabled
defaults; validate an empty child root, mount identity, descriptor hash,
single-writer host, and lock; run init with the exact acknowledgement; record
pre/post manifest, pointer, inode, and byte hashes; run one bounded operation;
verify no provider requests and no canonical mutation; preserve journal and
audit evidence; then close and review the window. This checklist is not
authorization to perform that operation now.
