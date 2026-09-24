# Stock EVA R2-F5.1 Preserve-Refresh Deployment Contract

**Author:** Codex R2-F delivery lead

**Date:** 2026-09-24 (Asia/Shanghai)

**Status:** APPROVED FOR IMPLEMENTATION AND CONTROLLED LOCAL INSTALLATION

## Context

R2-F5.1 adds offline secondary-provider capability and qualification contracts. Its acceptance
boundary requires the scheduled refresh LaunchAgent to remain disabled. The existing installer
always bootstraps every installed plist, so upgrading an installation whose refresh agent was
deliberately unloaded would silently re-enable provider activity.

The deployment contract must preserve that explicit disabled state while still installing the new
runtime and restarting services that were active. This change does not enable a provider, create
qualification evidence, publish canonical data, or change any data-quality gate.

## Functional Requirements

- FR-1: On upgrade, the installer MUST snapshot whether the refresh plist existed and whether the
  refresh service was loaded before any mutation.
- FR-2: If the refresh plist existed and refresh was unloaded, installation MUST install the new
  plist but MUST NOT bootstrap refresh.
- FR-3: If refresh was loaded before installation, installation MUST restart and verify it under
  the new runtime using the existing handoff behavior.
- FR-4: A first installation with no previous refresh plist MAY bootstrap refresh according to the
  existing first-install contract.
- FR-5: Failure rollback MUST restore the exact prior loaded/unloaded state for every agent.
- FR-6: Successful installation MUST report when disabled refresh state was preserved.

## Non-Functional Requirements

- NFR-1: State detection MUST use `launchctl print`; plist presence alone MUST NOT imply loaded.
- NFR-2: The change MUST NOT modify provider configuration, canonical datasets, manifests,
  pointers, repair queues, or qualification evidence.
- NFR-3: Synthetic install tests MUST cover loaded, unloaded, first-install and rollback behavior.
- NFR-4: The controlled production install MUST use verified local dataset reuse and perform zero
  provider requests.

## Acceptance Criteria

### AC-1: Preserve disabled refresh (FR-1, FR-2, FR-6)

Given an existing refresh plist with no loaded refresh job, when an upgrade succeeds, then the new
plist is installed, refresh remains unloaded, no refresh bootstrap occurs, and the result reports
the preserved state.

### AC-2: Preserve enabled refresh (FR-3)

Given a loaded refresh job, when an upgrade succeeds, then refresh is stopped before migration and
bootstrapped once against the new runtime.

### AC-3: First installation (FR-4)

Given no prior refresh plist, when installation succeeds, then existing first-install behavior is
unchanged.

### AC-4: Exact rollback (FR-5, NFR-3)

Given either prior loaded state, when installation fails after mutation, then rollback restores the
previous runtime, plists and loaded/unloaded agent set.

### AC-5: Controlled production install (NFR-2, NFR-4)

Given the R2-F5.1 reviewed commit and verified local canonical dataset, when production installation
runs, then the runtime points to that commit, refresh remains unloaded and canonical fingerprints do
not change.

## Edge Cases

- EC-1: Refresh plist exists but `launchctl print` fails: treat refresh as intentionally unloaded.
- EC-2: Refresh was loaded but bootstrap fails: fail installation and restore the prior runtime.
- EC-3: Disabled refresh plist replacement fails: fail installation and restore prior state.
- EC-4: Runtime handoff succeeds but API/Web readiness fails: rollback without loading refresh.

## API Contracts

HTTP API: N/A — this is a local deployment-state contract.

```text
upgrade input: previous plist existence + previous launchctl loaded state
upgrade output: runtime commit + installed plists + preserved loaded/unloaded state
```

## Data Models

| Entity | Fields | Constraints |
| --- | --- | --- |
| PreviousAgentState | label, plist_existed, loaded | captured before mutation; read-only |
| InstalledAgentState | label, plist_installed, loaded | must preserve disabled refresh on upgrade |

No database schema is added.

## Out of Scope

- OS-1: Enabling scheduled refresh or changing its calendar.
- OS-2: Provider canaries, retries, backfills or qualification observations.
- OS-3: Canonical publication, pointer changes or data repair.
- OS-4: General per-agent enable/disable policy; this contract is limited to the R2-F5.1 refresh
  safety boundary.
