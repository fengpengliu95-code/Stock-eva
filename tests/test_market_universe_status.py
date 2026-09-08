from __future__ import annotations

import json
import sqlite3
import sys
from datetime import UTC, date, datetime

from fastapi.testclient import TestClient

from backend.app import cli
from backend.app.api.market import get_settings
from backend.app.config import Settings
from backend.app.main import app
from backend.app.market.universe import (
    UniverseAttemptPlanV1,
    UniverseAttemptResultV1,
    UniverseSidecarStore,
    UniverseSourceStateV1,
    _source_state_identity,
    canonical_json_bytes,
    domain_sha256,
    source_version_digest,
)
from backend.app.market.universe_status import build_universe_status
from tests.test_market_universe import _authority, _bundle, _calendar_authority, _contract


def _record_changed_source_failure(store, contract, evidence, *, reason: str):
    refs = contract.source_refs.model_copy(update={"classification_source_version": "v2"})
    digest = source_version_digest(refs, evidence)
    refs = refs.model_copy(update={"source_version_digest": digest})
    source_id, source_sha = _source_state_identity(refs)
    observed_at = datetime(2026, 9, 2, 8, tzinfo=UTC)
    source = UniverseSourceStateV1(
        source_state_id=source_id,
        source_state_sha256=source_sha,
        trade_date=contract.trade_date,
        provider_id="baostock",
        source_version_digest=digest,
        source_refs_json=canonical_json_bytes(refs.model_dump(mode="json")).decode(),
        verified_at=observed_at,
    )
    operation_day = date(2026, 9, 2)
    created_at = datetime(2026, 9, 2, 9, tzinfo=UTC)
    preimage = {
        "trade_date": contract.trade_date.isoformat(),
        "operation_day": operation_day.isoformat(),
        "canonical_run_id": "status-test-run",
        "source_version_digest": digest,
    }
    dedup_key = domain_sha256("stock-eva/r2f4.2/universe-attempt-dedup/v1", preimage)
    plan_preimage = {
        **preimage,
        "hook_kind": "universe_post_success",
        "refresh_id": "status-test-refresh",
        "request_budget": 1,
        "classification_max_attempts": 1,
        "created_at": created_at.isoformat(),
        "attempt_status": "RUNNING",
    }
    plan = UniverseAttemptPlanV1(
        attempt_id="status-test-attempt",
        dedup_key=dedup_key,
        hook_kind="universe_post_success",
        refresh_id="status-test-refresh",
        canonical_run_id="status-test-run",
        source_version_digest=digest,
        trade_date=contract.trade_date,
        operation_day=operation_day,
        attempt_status="RUNNING",
        request_budget=1,
        classification_max_attempts=1,
        created_at=created_at,
        planned_sha256=domain_sha256("stock-eva/r2f4.2/universe-attempt-plan/v1", plan_preimage),
    )
    store.claim_attempt(plan)
    finished_at = datetime(2026, 9, 2, 10, tzinfo=UTC)
    result_preimage = {
        "attempt_id": plan.attempt_id,
        "terminal_status": "failed",
        "classification_request_count": 0,
        "reason_code": reason,
        "source_state_id": source_id,
        "finished_at": finished_at.isoformat(),
    }
    result = UniverseAttemptResultV1(
        attempt_id=plan.attempt_id,
        terminal_status="failed",
        classification_request_count=0,
        reason_code=reason,
        source_state_id=source_id,
        finished_at=finished_at,
        result_sha256=domain_sha256("stock-eva/r2f4.2/universe-attempt-result/v1", result_preimage),
    )
    store.record_source_state_and_result(source, result)


def test_market_universe_cli_parser_and_invalid_date_are_read_only(monkeypatch, capsys):
    parser = cli.build_parser()
    args = parser.parse_args(["market-universe", "--date", "not-a-date"])
    assert args.command == "market-universe"
    assert args.trade_date == "not-a-date"
    monkeypatch.setattr(sys, "argv", ["stock-eva", "market-universe", "--date", "not-a-date"])
    assert cli.main() == 2
    payload = json.loads(capsys.readouterr().out)
    assert payload["reason_code"] == "PIT_VISIBILITY_INVALID"
    assert payload["provider_requests"] == 0
    assert payload["writes"] is False


def test_market_universe_api_exists_and_invalid_date_is_422(monkeypatch):
    response = TestClient(app).get("/api/v1/market/universe?trade_date=not-a-date")
    assert response.status_code == 422
    assert response.json()["detail"]["reason_code"] == "PIT_VISIBILITY_INVALID"
    assert response.json()["detail"]["provider_requests"] == 0


def test_empty_or_missing_sidecar_is_control_error_without_initialization(
    tmp_path, monkeypatch, capsys
):
    settings = Settings(_env_file=None, local_control_dir=tmp_path / "control")
    monkeypatch.setattr(cli, "get_settings", lambda: settings)
    before = sorted(path.relative_to(tmp_path).as_posix() for path in tmp_path.rglob("*"))
    monkeypatch.setattr(
        sys,
        "argv",
        ["stock-eva", "market-universe", "--date", "2026-09-01"],
    )
    assert cli.main() == 3
    payload = json.loads(capsys.readouterr().out)
    assert payload == {"code": "universe_unavailable", "reason_code": "CONTROL_STATE_UNAVAILABLE"}
    assert sorted(path.relative_to(tmp_path).as_posix() for path in tmp_path.rglob("*")) == before

    sidecar = UniverseSidecarStore(
        settings.local_control_dir / settings.universe_contract_database_name
    )
    sidecar.initialize()
    app.dependency_overrides[get_settings] = lambda: settings
    try:
        response = TestClient(app).get("/api/v1/market/universe?trade_date=2026-09-01")
    finally:
        app.dependency_overrides.clear()
    assert response.status_code == 200
    assert response.json()["status"] == "unavailable"
    assert response.json()["reason_code"] == "CONTROL_STATE_UNAVAILABLE"


def test_promoted_sidecar_status_is_bounded_and_zero_request(tmp_path):
    path = tmp_path / "market_universe.sqlite3"
    store = UniverseSidecarStore(path)
    store.initialize()
    contract = _contract(trade_date=date(2026, 9, 1))
    mapping, evidence, snapshot = _bundle(contract)
    store.promote(
        contract,
        expected_sequence=0,
        expected_head_sha256=None,
        mapping=mapping,
        evidence=evidence,
        authority_bundle=_authority(mapping, evidence),
        required_snapshot=snapshot,
        calendar_authority=_calendar_authority(contract.trade_date),
    )
    payload = build_universe_status(
        contract.trade_date,
        store,
        now=datetime(2026, 9, 2, tzinfo=UTC),
    )
    assert payload["status"] == "ready"
    assert payload["reason_code"] == "NONE"
    assert payload["provider_requests"] == 0
    assert payload["writes"] is False
    assert payload["counts"]["total"] == len(contract.members)
    assert set(payload) == {
        "trade_date",
        "universe_id",
        "schema_version",
        "scope",
        "provider_requests",
        "writes",
        "status",
        "verified_head_trade_date",
        "contract_id",
        "contract_sha256",
        "source_state_id",
        "source_state_sha256",
        "source_version_digest",
        "classification_generation_id",
        "calendar_generation_id",
        "calendar_sha256",
        "counts",
        "layers",
        "required_indexes",
        "required_user_symbol_count",
        "publication_eligible",
        "reason_code",
    }


def test_future_date_checks_sidecar_proof_before_semantic_date(tmp_path):
    missing = UniverseSidecarStore(tmp_path / "missing.sqlite3")
    try:
        build_universe_status(date(2999, 1, 1), missing, now=datetime(2026, 9, 2, tzinfo=UTC))
    except Exception as exc:
        assert exc.__class__.__name__ == "UniverseStatusControlError"
    else:
        raise AssertionError("missing sidecar must win over future-date semantics")

    empty_path = tmp_path / "empty.sqlite3"
    empty = UniverseSidecarStore(empty_path)
    empty.initialize()
    try:
        build_universe_status(date(2999, 1, 1), empty, now=datetime(2026, 9, 2, tzinfo=UTC))
    except Exception as exc:
        assert exc.__class__.__name__ == "UniverseStatusDateError"
    else:
        raise AssertionError("proven sidecar must then reject a future date")


def test_no_migration_rehearsal_preserves_canonical_files_and_pointer(tmp_path):
    canonical = tmp_path / "canonical"
    canonical.mkdir()
    parquet = canonical / "2026-09-01.parquet"
    manifest = canonical / "manifest.json"
    pointer = canonical / "CURRENT"
    parquet.write_bytes(b"immutable parquet fixture")
    manifest.write_bytes(b'{"sha256":"fixture"}\n')
    pointer.write_bytes(b"2026-09-01\n")
    before = {
        path: (path.stat().st_ino, path.read_bytes()) for path in (parquet, manifest, pointer)
    }

    sidecar = UniverseSidecarStore(tmp_path / "control" / "market_universe.sqlite3")
    sidecar.initialize()
    contract = _contract(trade_date=date(2026, 9, 1))
    mapping, evidence, snapshot = _bundle(contract)
    sidecar.promote(
        contract,
        expected_sequence=0,
        expected_head_sha256=None,
        mapping=mapping,
        evidence=evidence,
        authority_bundle=_authority(mapping, evidence),
        required_snapshot=snapshot,
        calendar_authority=_calendar_authority(contract.trade_date),
    )
    assert (
        build_universe_status(
            contract.trade_date,
            sidecar,
            now=datetime(2026, 9, 2, tzinfo=UTC),
        )["status"]
        == "ready"
    )
    after = {path: (path.stat().st_ino, path.read_bytes()) for path in (parquet, manifest, pointer)}
    assert after == before


def test_source_version_and_terminal_attempt_status_matrix(tmp_path):
    store = UniverseSidecarStore(tmp_path / "universe.sqlite3")
    store.initialize()
    contract = _contract(trade_date=date(2026, 9, 1))
    mapping, evidence, snapshot = _bundle(contract)
    store.promote(
        contract,
        expected_sequence=0,
        expected_head_sha256=None,
        mapping=mapping,
        evidence=evidence,
        authority_bundle=_authority(mapping, evidence),
        required_snapshot=snapshot,
        calendar_authority=_calendar_authority(contract.trade_date),
    )
    # A newer source row without a corresponding attempt is stale.  Insert it only through the
    # sidecar's existing immutable writer tables; the status reader itself remains read-only.
    refs = contract.source_refs.model_copy(update={"classification_source_version": "v2"})
    digest = source_version_digest(refs, evidence)
    refs = refs.model_copy(update={"source_version_digest": digest})
    source_id, source_sha = _source_state_identity(refs)
    with sqlite3.connect(store.path) as connection:
        connection.execute(
            "INSERT INTO universe_source_state VALUES (?,?,?,?,?,?,?)",
            (
                source_id,
                source_sha,
                contract.trade_date.isoformat(),
                "baostock",
                digest,
                canonical_json_bytes(refs.model_dump(mode="json")).decode(),
                "2026-09-02T08:00:00+00:00",
            ),
        )
        connection.commit()
    stale = build_universe_status(contract.trade_date, store, now=datetime(2026, 9, 2, tzinfo=UTC))
    assert stale["status"] == "stale"
    assert stale["reason_code"] == "UNIVERSE_SOURCE_VERSION_CHANGED"

    # A matching failed attempt takes precedence over the stale projection and hides old head
    # fields, as required by the terminal matrix.
    store2 = UniverseSidecarStore(tmp_path / "universe-blocked.sqlite3")
    store2.initialize()
    store2.promote(
        contract,
        expected_sequence=0,
        expected_head_sha256=None,
        mapping=mapping,
        evidence=evidence,
        authority_bundle=_authority(mapping, evidence),
        required_snapshot=snapshot,
        calendar_authority=_calendar_authority(contract.trade_date),
    )
    _record_changed_source_failure(store2, contract, evidence, reason="CLASSIFICATION_UNAVAILABLE")
    blocked = build_universe_status(
        contract.trade_date, store2, now=datetime(2026, 9, 2, tzinfo=UTC)
    )
    assert blocked["status"] == "blocked"
    assert blocked["reason_code"] == "CLASSIFICATION_UNAVAILABLE"
    assert blocked["contract_id"] is None


def test_api_and_cli_project_the_same_promoted_snapshot(tmp_path, monkeypatch, capsys):
    settings = Settings(_env_file=None, local_control_dir=tmp_path / "control")
    store = UniverseSidecarStore(
        settings.local_control_dir / settings.universe_contract_database_name
    )
    store.initialize()
    contract = _contract(trade_date=date(2026, 9, 1))
    mapping, evidence, snapshot = _bundle(contract)
    store.promote(
        contract,
        expected_sequence=0,
        expected_head_sha256=None,
        mapping=mapping,
        evidence=evidence,
        authority_bundle=_authority(mapping, evidence),
        required_snapshot=snapshot,
        calendar_authority=_calendar_authority(contract.trade_date),
    )
    app.dependency_overrides[get_settings] = lambda: settings
    try:
        response = TestClient(app).get("/api/v1/market/universe?trade_date=2026-09-01")
    finally:
        app.dependency_overrides.clear()
    assert response.status_code == 200
    api_payload = response.json()
    assert api_payload["status"] == "ready"
    assert api_payload["reason_code"] == "NONE"
    assert api_payload["provider_requests"] == 0
    assert api_payload["writes"] is False

    monkeypatch.setattr(cli, "get_settings", lambda: settings)
    monkeypatch.setattr(sys, "argv", ["stock-eva", "market-universe", "--date", "2026-09-01"])
    assert cli.main() == 0
    cli_payload = json.loads(capsys.readouterr().out)
    assert cli_payload == api_payload
