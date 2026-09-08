from __future__ import annotations

import inspect
import json
from datetime import date

import pytest

from backend.app.market.universe import (
    RequiredSymbolSnapshotV1,
    UniverseCountsV1,
    UniverseInstrumentEvidenceV1,
    UniverseSemanticMappingV1,
    UniverseSidecarStore,
    UniverseStoreUnavailable,
    build_universe_contract,
    canonical_json_bytes,
    classification_snapshot_sha256,
    domain_sha256,
    source_version_digest,
    validate_calendar_authority,
    validate_provider_raw_batch,
)
from backend.app.user.models import PositionCreate
from backend.app.user.store import UserDataError, UserStore
from tests.test_market_universe import _authority, _bundle, _contract, _evidence, _mapping


def _rehash_evidence(values: dict) -> UniverseInstrumentEvidenceV1:
    values = dict(values)
    values.pop("evidence_sha256", None)
    draft = UniverseInstrumentEvidenceV1.model_construct(**values, evidence_sha256="0" * 64)
    values["evidence_sha256"] = domain_sha256(
        "stock-eva/r2f4.2/instrument-evidence/v1", draft.preimage()
    )
    return UniverseInstrumentEvidenceV1.model_validate(values)


def test_missing_listing_window_evidence_is_unknown_and_unpublishable():
    contract = _contract()
    values = _evidence(contract.members[0], contract.trade_date).model_dump()
    values.update(
        list_date=None,
        delist_date=None,
        source_schema="classification.instrument.v1",
        artifact_origin="production_reviewed_artifact",
    )
    with pytest.raises(ValueError):
        _rehash_evidence(values)


def test_local_reviewed_fixture_cannot_enter_production_evidence():
    contract = _contract()
    values = _evidence(contract.members[0], contract.trade_date).model_dump()
    values["artifact_origin"] = "local_reviewed_fixture"
    with pytest.raises(ValueError):
        _rehash_evidence(values)


def test_required_index_evidence_requires_closed_identity_and_state():
    contract = _contract()
    values = _evidence(
        next(m for m in contract.members if m.member_kind == "index"), contract.trade_date
    ).model_dump()
    values.update(
        board="index",
        security_type="stock",
        expected_trading_state="trading",
        source_schema="classification.instrument.v2",
        suspension_state="trading",
        st_state="not_applicable",
    )
    with pytest.raises(ValueError):
        _rehash_evidence(values)


def test_reviewed_mapping_requires_closed_schema_payload_identity():
    mapping = _mapping()
    payload = {"tradestatus": {"unexpected": "1"}}
    values = mapping.model_dump()
    values["source_schema"] = "classification.instrument.v9"
    values["payload_json"] = canonical_json_bytes(payload).decode()
    values["mapping_sha256"] = domain_sha256(
        "stock-eva/r2f4.2/semantic-mapping/v1",
        {
            "mapping_id": values["mapping_id"],
            "mapping_version": values["mapping_version"],
            "provider_id": values["provider_id"],
            "source_schema": values["source_schema"],
            "payload": payload,
        },
    )
    with pytest.raises(ValueError):
        UniverseSemanticMappingV1.model_validate(values)


def test_legacy_reviewed_mapping_schema_cannot_establish_authority():
    mapping = _mapping()
    values = mapping.model_dump()
    values["source_schema"] = "classification.instrument.v1"
    payload = {"tradestatus": {"trading": "1", "suspended": "0"}}
    values["payload_json"] = canonical_json_bytes(payload).decode()
    values["mapping_sha256"] = domain_sha256(
        "stock-eva/r2f4.2/semantic-mapping/v1",
        {
            "mapping_id": values["mapping_id"],
            "mapping_version": values["mapping_version"],
            "provider_id": values["provider_id"],
            "source_schema": values["source_schema"],
            "payload": payload,
        },
    )
    with pytest.raises(ValueError):
        UniverseSemanticMappingV1.model_validate(values)


def test_counts_cannot_be_constructed_outside_member_state_recount():
    contract = _contract()
    counts = UniverseCountsV1(
        total=len(contract.members),
        trading=0,
        suspended=len(contract.members),
        not_yet_listed=0,
        delisted=0,
        unknown=0,
        session_expected=len(contract.members),
        loaded=None,
        critical_attribute_unknown_count=0,
    )
    payload = json.loads(contract.payload_json)
    payload["counts"] = counts.model_dump(mode="json")
    digest = domain_sha256("stock-eva/r2f4.2/universe-contract/v1", payload)
    values = contract.model_dump()
    values.update(
        counts=counts,
        contract_id=digest[:32],
        contract_sha256=digest,
        payload_json=canonical_json_bytes(payload).decode(),
    )
    with pytest.raises(ValueError):
        type(contract).model_validate(values)


def test_raw_gate_binds_request_scope_and_requires_all_endpoints():
    from tests.test_market_provider_contract import _provider_raw_batch

    batch = _provider_raw_batch()
    contract = _contract(trade_date=batch.request.trade_date)
    bad_request = batch.request.model_copy(update={"universe_id": "other-universe"})
    bad_batch = batch.model_copy(update={"request": bad_request})
    bad_result = validate_provider_raw_batch(bad_batch, contract)
    assert bad_result.reason_code == "UNIVERSE_SESSION_DRIFT"

    drift = validate_provider_raw_batch(batch, contract)
    assert drift.reason_code == "UNIVERSE_SESSION_DRIFT"


def test_cas_rejects_bad_snapshot_roles_before_head_change(tmp_path):
    source = inspect.getsource(UniverseSidecarStore.promote)
    assert source.index("RequiredSymbolSnapshotV1.model_validate") < source.index("BEGIN IMMEDIATE")


def test_promote_rejects_caller_crafted_required_snapshot(tmp_path):
    contract = _contract()
    mapping, evidence, trusted = _bundle(contract)
    crafted = RequiredSymbolSnapshotV1.model_validate(trusted.model_dump(mode="python"))
    store = UniverseSidecarStore(tmp_path / "market_universe.sqlite3")
    store.initialize()
    with pytest.raises(UniverseStoreUnavailable):
        store.promote(
            contract,
            expected_sequence=0,
            expected_head_sha256=None,
            mapping=mapping,
            evidence=evidence,
            authority_bundle=_authority(mapping, evidence),
            required_snapshot=crafted,
        )


def test_classification_projection_is_bound_field_by_field_to_evidence(tmp_path):
    contract = _contract()
    values = contract.model_dump()
    projection = list(values["classification_evidence_projection"])
    projection[0]["symbol"] = "sz.000001"
    values["classification_evidence_projection"] = tuple(projection)
    payload = json.loads(contract.payload_json)
    payload["classification_evidence_projection"] = projection
    digest = domain_sha256("stock-eva/r2f4.2/universe-contract/v1", payload)
    values.update(
        contract_id=digest[:32],
        contract_sha256=digest,
        payload_json=canonical_json_bytes(payload).decode(),
        classification_evidence_partition_sha256=domain_sha256(
            "stock-eva/r2f4.2/universe-partition/classification-evidence/v1", projection
        ),
        classification_evidence_sha256=domain_sha256(
            "stock-eva/r2f4.2/universe-partition/classification-evidence/v1", projection
        ),
    )
    bad_contract = type(contract).model_validate(values)
    store = UniverseSidecarStore(tmp_path / "market_universe.sqlite3")
    store.initialize()
    mapping, evidence, snapshot = _bundle(contract)
    with pytest.raises(UniverseStoreUnavailable):
        store.promote(
            bad_contract,
            expected_sequence=0,
            expected_head_sha256=None,
            mapping=mapping,
            evidence=evidence,
            authority_bundle=_authority(mapping, evidence),
            required_snapshot=snapshot,
        )


def test_read_methods_explicitly_open_one_deferred_snapshot():
    assert "BEGIN DEFERRED" in inspect.getsource(UniverseSidecarStore.read_head)
    source = inspect.getsource(UniverseSidecarStore.read_verified_snapshot)
    assert "BEGIN DEFERRED" in source


def test_cross_generation_new_evidence_identity_is_allowed(tmp_path):
    store = UniverseSidecarStore(tmp_path / "market_universe.sqlite3")
    store.initialize()
    first = _contract()
    mapping, evidence, snapshot = _bundle(first)
    first_head = store.promote(
        first,
        expected_sequence=0,
        expected_head_sha256=None,
        mapping=mapping,
        evidence=evidence,
        authority_bundle=_authority(mapping, evidence),
        required_snapshot=snapshot,
    )
    members = tuple(
        _rehash_member(member, f"{member.instrument_evidence_id}-g2") for member in first.members
    )
    refs = first.source_refs.model_copy(
        update={
            "classification_generation_id": "cls-2",
            "classification_generation_sequence": 2,
            "instrument_evidence_ids": tuple(
                sorted(member.instrument_evidence_id for member in members)
            ),
        }
    )
    second_evidence = tuple(_evidence(member, first.trade_date) for member in members)
    refs = refs.model_copy(
        update={
            "classification_snapshot_sha256": classification_snapshot_sha256(second_evidence),
        }
    )
    refs = refs.model_copy(
        update={"source_version_digest": source_version_digest(refs, second_evidence)}
    )
    second_authority = _authority(mapping, second_evidence)
    second = build_universe_contract(
        trade_date=first.trade_date,
        calendar_generation_id=first.calendar_generation_id,
        calendar_sha256=first.calendar_sha256,
        classification_generation_id="cls-2",
        classification_generation_sequence=2,
        classification_source=refs.classification_source,
        classification_source_version=refs.classification_source_version,
        classification_source_snapshot_date=refs.classification_source_snapshot_date,
        classification_observed_at=refs.classification_observed_at,
        exact_pit_cutoff=refs.exact_pit_cutoff,
        source_refs=refs,
        members=members,
        classification_evidence=second_evidence,
        authority_bundle=second_authority,
        calendar_authority=__import__(
            "tests.test_market_universe", fromlist=["_calendar_authority"]
        )._calendar_authority(first.trade_date),
        sequence=2,
        parent_contract_id=first.contract_id,
    )
    second_snapshot = snapshot
    second_head = store.promote(
        second,
        expected_sequence=first_head.sequence,
        expected_head_sha256=first_head.head_sha256,
        mapping=mapping,
        evidence=second_evidence,
        authority_bundle=_authority(mapping, second_evidence),
        required_snapshot=second_snapshot,
    )
    assert second_head.sequence == 2


def test_required_index_role_cannot_be_assigned_to_stock_snapshot():
    token = "f" * 64
    symbols = (("sh.600000", ("required_index",)),)
    digest = domain_sha256(
        "stock-eva/r2f4.2/required-symbol-snapshot/v1",
        {
            "schema_version": 1,
            "snapshot_token": token,
            "symbols": [{"symbol": symbols[0][0], "roles": list(symbols[0][1])}],
        },
    )
    with pytest.raises(ValueError):
        RequiredSymbolSnapshotV1(
            snapshot_id="snap-bad",
            snapshot_sha256=digest,
            snapshot_token_digest=token,
            symbols=symbols,
        )


def test_publication_context_lineage_refs_are_bidirectionally_bound():
    lineage = {
        "provider_id": "baostock",
        "universe_id": "all-main-board-plus-required-symbols",
        "evidence_id": "a" * 64,
        "evidence_sha256": "b" * 64,
        "candidate_id": "c" * 64,
        "candidate_manifest_sha256": "d" * 64,
        "gate_report_sha256": "e" * 64,
        "adapter_version": "r2f2.v1",
        "source_schema_version": "daily_astock.v1",
    }
    lineage_json = canonical_json_bytes(lineage).decode()
    evidence_refs = tuple(
        sorted(
            {
                lineage["evidence_id"],
                lineage["evidence_sha256"],
                lineage["candidate_id"],
                lineage["gate_report_sha256"],
            }
        )
    )
    created_at = "2026-09-01T18:00:00+00:00"
    values = {
        "run_id": "run-1",
        "trade_date": "2026-09-01",
        "manifest_ref": lineage["candidate_manifest_sha256"],
        "evidence_refs": list(evidence_refs),
        "publication_lineage_json": lineage_json,
        "publication_lineage_sha256": domain_sha256(
            "stock-eva/r2f4.2/publication-lineage/v2", lineage
        ),
        "status": "ready",
        "created_at": created_at,
    }
    digest = domain_sha256("stock-eva/r2f4.2/universe-publication-context/v1", values)
    values.update(context_sha256=digest, context_id=digest[:32])
    values["evidence_refs"] = [lineage["evidence_id"]]
    digest = domain_sha256(
        "stock-eva/r2f4.2/universe-publication-context/v1",
        {
            **{
                key: value
                for key, value in values.items()
                if key not in {"context_id", "context_sha256", "evidence_refs"}
            },
            "evidence_refs": values["evidence_refs"],
        },
    )
    values.update(context_sha256=digest, context_id=digest[:32])
    with pytest.raises(ValueError):
        from backend.app.market.universe import UniversePublicationContextV1

        UniversePublicationContextV1.model_validate(values)


def test_reader_rejects_unallowlisted_view(tmp_path):
    path = tmp_path / "market_universe.sqlite3"
    store = UniverseSidecarStore(path)
    store.initialize()
    import sqlite3

    with sqlite3.connect(path) as connection:
        connection.execute("CREATE VIEW unexpected_view AS SELECT 1 AS value")
        connection.commit()
    with pytest.raises(UniverseStoreUnavailable):
        store.read_head()


def test_read_latest_source_state_uses_explicit_deferred_snapshot():
    source = inspect.getsource(UniverseSidecarStore.read_latest_source_state)
    assert "BEGIN DEFERRED" in source


def test_empty_head_still_rejects_orphan_evidence(tmp_path):
    import sqlite3

    path = tmp_path / "market_universe.sqlite3"
    store = UniverseSidecarStore(path)
    store.initialize()
    contract = _contract()
    mapping, evidence, _ = _bundle(contract)
    with sqlite3.connect(path) as connection:
        connection.execute(
            "INSERT INTO universe_semantic_mapping VALUES (?,?,?,?,?,?,?)",
            (
                mapping.mapping_id,
                mapping.mapping_version,
                mapping.provider_id,
                mapping.source_schema,
                mapping.payload_json,
                mapping.mapping_sha256,
                mapping.authority_status,
            ),
        )
        item = evidence[0]
        connection.execute(
            "INSERT INTO universe_instrument_evidence VALUES "
            "(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            tuple(item.model_dump(mode="json").values()),
        )
        connection.commit()
    with pytest.raises(UniverseStoreUnavailable):
        store.read_head()


def test_raw_gate_rejects_cross_page_duplicate_identity_and_bad_shard_projection():
    from tests.test_market_provider_contract import _provider_raw_batch

    batch = _provider_raw_batch()
    endpoint = batch.endpoint_batches[0]
    bad_endpoint = endpoint.model_copy(update={"shard_id": "different-shard"})
    with pytest.raises(ValueError):
        batch.model_copy(update={"endpoint_batches": (bad_endpoint,)})


def test_builder_without_verified_authority_bundle_is_not_publishable():
    legacy = _contract()
    refs = legacy.source_refs.model_copy(update={"classification_source": "production"})
    refs = refs.model_copy(
        update={"classification_snapshot_sha256": classification_snapshot_sha256(())}
    )
    refs = refs.model_copy(update={"source_version_digest": source_version_digest(refs, ())})
    contract = build_universe_contract(
        trade_date=legacy.trade_date,
        calendar_generation_id=legacy.calendar_generation_id,
        calendar_sha256=legacy.calendar_sha256,
        classification_generation_id=legacy.classification_generation_id,
        classification_generation_sequence=refs.classification_generation_sequence,
        classification_source="production",
        classification_source_version=refs.classification_source_version,
        classification_source_snapshot_date=refs.classification_source_snapshot_date,
        classification_observed_at=refs.classification_observed_at,
        exact_pit_cutoff=refs.exact_pit_cutoff,
        source_refs=refs,
        members=legacy.members,
    )
    assert contract.publication_eligible is False


def _rehash_member(member, evidence_id: str):
    values = member.model_dump()
    values["instrument_evidence_id"] = evidence_id
    values["member_sha256"] = None
    draft = type(member).model_construct(**values)
    values["member_sha256"] = domain_sha256("stock-eva/r2f4.2/universe-member/v1", draft.preimage())
    return type(member).model_validate(values)


def test_non_main_board_cannot_enter_effective_base_from_caller_role():
    """Fourth-review RED: scope_role must not override derived identity."""
    from tests.test_market_universe import _member

    with pytest.raises(ValueError):
        _member("sh.688000", role="effective_main_board")


def test_reviewed_evidence_requires_observed_at_at_or_before_cutoff():
    contract = _contract()
    values = _evidence(contract.members[0], contract.trade_date).model_dump()
    values["observed_at"] = None
    with pytest.raises(ValueError):
        _rehash_evidence(values)


def test_source_version_refs_are_derived_not_caller_selected():
    contract = _contract()
    refs = contract.source_refs.model_copy(update={"classification_snapshot_sha256": "c" * 64})
    evidence = tuple(_evidence(item, contract.trade_date) for item in contract.members)
    with pytest.raises(ValueError):
        build_universe_contract(
            trade_date=contract.trade_date,
            calendar_generation_id=contract.calendar_generation_id,
            calendar_sha256=contract.calendar_sha256,
            classification_generation_id=refs.classification_generation_id,
            classification_generation_sequence=refs.classification_generation_sequence,
            classification_source=refs.classification_source,
            classification_source_version=refs.classification_source_version,
            classification_source_snapshot_date=refs.classification_source_snapshot_date,
            classification_observed_at=refs.classification_observed_at,
            exact_pit_cutoff=refs.exact_pit_cutoff,
            source_refs=refs,
            members=contract.members,
            classification_evidence=evidence,
            authority_bundle=_authority(_mapping(), evidence),
        )


def test_user_snapshot_capture_is_existing_descriptor_bound_and_atomic(tmp_path):
    store = UserStore(tmp_path / "user.sqlite3")
    position = store.create_position(
        PositionCreate(
            symbol="sh.600000",
            quantity=1,
            avg_cost=1,
            as_of_date=date(2026, 9, 1),
        )
    )
    result = store.capture_required_symbol_snapshot_existing()
    assert result.snapshot.symbols == (position.symbol,)
    assert result.snapshot.roles_by_symbol == ((position.symbol, ("position",)),)
    assert len(result.snapshot_token) == 64


def test_user_snapshot_capture_does_not_initialize_missing_database(tmp_path):
    path = tmp_path / "missing.sqlite3"
    with pytest.raises(UserDataError):
        UserStore(path).capture_required_symbol_snapshot_existing()
    assert not path.exists()


def test_calendar_authority_rejects_weekend_and_unavailable_read_result():
    from backend.app.market.calendar_generation import CalendarReadResult
    from tests.test_market_universe import _calendar_authority

    authority = _calendar_authority(date(2026, 9, 5))
    with pytest.raises(ValueError):
        validate_calendar_authority(
            authority,
            trade_date=date(2026, 9, 5),
            calendar_generation_id="a" * 32,
            calendar_sha256="a" * 64,
        )
    unavailable = CalendarReadResult(status="unavailable")
    with pytest.raises(ValueError):
        validate_calendar_authority(
            unavailable,
            trade_date=date(2026, 9, 4),
            calendar_generation_id="a" * 32,
            calendar_sha256="a" * 64,
        )


def test_reviewed_not_yet_listed_and_delisted_states_have_explicit_mapping():
    from tests.test_market_universe import _member

    delisted = _member("sh.600010", state="delisted", role="required_user").model_copy(
        update={"delist_date": date(2026, 9, 1), "member_sha256": None}
    )
    contract = _contract((delisted,))
    evidence = next(item for item in _bundle(contract)[1] if item.symbol == delisted.symbol)
    assert evidence.listing_status == "delisted"
    assert evidence.daily_trade_status == "0"
    assert evidence.suspension_state == "not_supplied"
