# Continuous integration

The CI workflow runs for pull requests targeting `main` and pushes to `main`.
Both Ubuntu and macOS use Python 3.12 and the committed uv lockfile:

```sh
uv sync --extra dev --frozen
uv run pytest -q
uv run ruff check backend tests
uv run ruff format --check backend tests
git diff --check
```

All commands are quality gates. A failed test is not ignored: lint, formatting and
whitespace checks still run to give a complete diagnosis, and the job stays failed.
The matrix does not cancel the other platform after one failure. JUnit reports
are retained as per-platform artifacts for 14 days. Actions are pinned to reviewed
release commit SHAs; update the SHA and version comment together.

## Platform coverage and safety

Canonical candidate and shadow bundle publication use native atomic no-clobber
operations: Linux `renameat2(RENAME_NOREPLACE)` and macOS
`renameatx_np(RENAME_EXCL)`. There is no check-then-rename fallback.
Concurrent publication and existing-file/directory/symlink collision tests run on
both platforms.

Secure control-store writer migration remains macOS-only under its existing
reviewed `UF_APPEND` / `renameatx_np` contract. Unsupported platforms reject
migration before creating target or staging directories. Linux does not silently
replace the directory protection with a no-op.

Only native macOS migration integrations have explicit, reasoned platform skips
on Ubuntu; their original assertions run in the macOS job. They are not deleted
or replaced by fake success. Ubuntu separately tests unsupported-platform
rejection and preservation of existing bytes and directory trees.

LaunchAgent installer tests still run on both platforms. macOS exercises native
`plutil`, `ditto` and BSD `mv`; Linux fixtures inject real plist parsing,
filesystem copying and atomic symlink replacement implementations using the same
test-only executable override pattern as the existing launchctl/uv fixtures.
Production defaults and the macOS-only installer guard are unchanged.
CI does not connect to a real NAS or install real LaunchAgents.

## Diagnosing a red run

1. If frozen sync fails, inspect Python, uv and dependency/wheel availability first.
2. Read pytest logs and the JUnit artifact; distinguish platform-specific native
   integrations from portable code, fixture ordering, and genuine assertion failures.
3. Read every quality-gate result even when pytest failed. Do not weaken assertions,
   disable quality gates, or add blanket skips to turn the run green.
4. Verify both platform jobs before considering a merge. CI success does not prove
   live-provider credentials, NAS deployment, or production data correctness.

The first Ubuntu run exposed a Darwin-only candidate publish operation, implicit
filesystem iteration ordering, macOS-only race injection, immediate inode reuse
in a replacement fixture, and a SQLite corruption oracle that never read the
database catalog. The bootstrap repair keeps the contracts and makes those
fixtures deterministic rather than relaxing their expected outcomes.
