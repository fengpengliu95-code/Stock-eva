from __future__ import annotations

import sqlite3
import tempfile
from datetime import UTC, date, datetime, time

import pytest

from backend.app.market.calendar_generation import (
    CalendarAuthorityAdmission,
    CalendarConfigSnapshot,
    CalendarGenerationV1,
    CalendarMachineDayV1,
    CalendarReadResult,
    ImmutableCalendarSnapshot,
    build_generation_sha256,
    build_observation,
)
from backend.app.market.universe import (
    RequiredSymbolSnapshotV1,
    SourceRefsV1,
    UniverseAuthorityBundleV1,
    UniverseCountsV1,
    UniverseInstrumentEvidenceV1,
    UniverseMemberV1,
    UniverseSemanticMappingV1,
    UniverseSidecarStore,
    UniverseStoreUnavailable,
    build_universe_contract,
    canonical_json_bytes,
    classification_snapshot_sha256,
    domain_sha256,
    source_version_digest,
    validate_provider_raw_batch,
)
from backend.app.user.store import UserStore


def _member(symbol: str, *, state: str = "trading", role: str = "effective_main_board"):
    is_index = role == "required_index"
    return UniverseMemberV1(
        symbol=symbol,
        security_id=f"sec-{symbol.replace('.', '-')}",
        member_kind="index" if is_index else "stock",
        scope_roles=(role,),
        exchange="SSE" if symbol.startswith("sh.") else "SZSE",
        board="index" if is_index else "main",
        list_date=date(2020, 1, 1),
        delist_date=None,
        expected_trading_state=state,
        st_state="not_applicable" if is_index else "no",
        state_source="fixture-state-v1",
        effective_from=date(2020, 1, 1),
        effective_to=None,
        instrument_evidence_id=f"ev-{symbol.replace('.', '-')}",
    )


def _contract(
    members: tuple[UniverseMemberV1, ...] = (),
    *,
    sequence: int = 1,
    parent_contract_id: str | None = None,
    source_date_semantics: str = "source_observed",
    trade_date: date = date(2026, 9, 1),
    include_user_snapshot: bool = True,
):
    members = members or (
        _member("sh.600000"),
        _member("sh.000001", role="required_index"),
        _member("sz.399001", role="required_index"),
    )
    if not any(item.symbol == "sh.000001" for item in members):
        members += (_member("sh.000001", role="required_index"),)
    if not any(item.symbol == "sz.399001" for item in members):
        members += (_member("sz.399001", role="required_index"),)
    mapping = _mapping()
    user_capture = _user_capture(members)
    snapshot = RequiredSymbolSnapshotV1.from_user_capture(user_capture)
    calendar_authority = _calendar_authority(trade_date)
    calendar_hash = calendar_authority.read_result.generation.generation_sha256
    calendar_id = calendar_hash[:32]
    evidence = (
        tuple(_evidence(item, trade_date) for item in members)
        if all(
            item.expected_trading_state in {"trading", "suspended", "not_yet_listed", "delisted"}
            for item in members
        )
        else ()
    )
    authority_projection = [
        {
            "evidence_id": item.evidence_id,
            "evidence_sha256": item.evidence_sha256,
            "mapping_id": item.mapping_id,
            "mapping_version": item.mapping_version,
        }
        for item in sorted(evidence, key=lambda value: value.evidence_id)
    ]
    authority = (
        UniverseAuthorityBundleV1(
            mapping=mapping,
            evidence=evidence,
            source_date_semantics="source_observed",
            authority_sha256=domain_sha256(
                "stock-eva/r2f4.2/universe-authority-bundle/v1",
                {
                    "mapping_sha256": mapping.mapping_sha256,
                    "source_date_semantics": "source_observed",
                    "evidence": authority_projection,
                },
            ),
        )
        if evidence
        else None
    )
    snapshot_digest = classification_snapshot_sha256(evidence)
    refs_for_digest = SourceRefsV1.model_validate(
        {
            "calendar_generation_id": calendar_id,
            "calendar_sha256": calendar_hash,
            "classification_generation_id": "cls-1",
            "classification_generation_sequence": 1,
            "classification_source": "fixture",
            "classification_source_version": "v1",
            "classification_source_snapshot_date": min(trade_date, date(2026, 8, 31)),
            "classification_observed_at": datetime.combine(trade_date, time(8), tzinfo=UTC),
            "classification_snapshot_sha256": snapshot_digest,
            "required_symbol_snapshot_id": snapshot.snapshot_id,
            "required_symbol_snapshot_sha256": snapshot.snapshot_sha256,
            "instrument_evidence_ids": tuple(
                sorted(item.instrument_evidence_id for item in members)
            ),
            "semantic_mapping_sha256": mapping.mapping_sha256,
            "source_version_digest": "e" * 64,
            "provider_id": "baostock",
            "trade_date": trade_date,
            "exact_pit_cutoff": datetime.combine(trade_date, time(18), tzinfo=UTC),
            "source_date_semantics": source_date_semantics,
        }
    )
    source_digest = source_version_digest(refs_for_digest, evidence)
    return build_universe_contract(
        trade_date=trade_date,
        calendar_generation_id=calendar_id,
        calendar_sha256=calendar_hash,
        classification_generation_id="cls-1",
        classification_generation_sequence=1,
        classification_source="fixture",
        classification_source_version="v1",
        classification_source_snapshot_date=min(trade_date, date(2026, 8, 31)),
        classification_observed_at=f"{trade_date.isoformat()}T08:00:00Z",
        exact_pit_cutoff=f"{trade_date.isoformat()}T18:00:00Z",
        source_refs={
            "calendar_generation_id": calendar_id,
            "calendar_sha256": calendar_hash,
            "classification_generation_id": "cls-1",
            "classification_generation_sequence": 1,
            "classification_source": "fixture",
            "classification_source_version": "v1",
            "classification_source_snapshot_date": min(trade_date, date(2026, 8, 31)).isoformat(),
            "classification_observed_at": f"{trade_date.isoformat()}T08:00:00Z",
            "classification_snapshot_sha256": snapshot_digest,
            "required_symbol_snapshot_id": snapshot.snapshot_id,
            "required_symbol_snapshot_sha256": snapshot.snapshot_sha256,
            "instrument_evidence_ids": sorted(item.instrument_evidence_id for item in members),
            "semantic_mapping_sha256": mapping.mapping_sha256,
            "source_version_digest": source_digest,
            "provider_id": "baostock",
            "trade_date": trade_date.isoformat(),
            "exact_pit_cutoff": f"{trade_date.isoformat()}T18:00:00Z",
            "source_date_semantics": source_date_semantics,
        },
        members=members,
        classification_evidence=evidence,
        authority_bundle=authority if include_user_snapshot else None,
        calendar_authority=calendar_authority,
        required_user_snapshot=user_capture if include_user_snapshot else None,
        sequence=sequence,
        parent_contract_id=parent_contract_id,
    )


def _calendar_authority(trade_date: date) -> CalendarAuthorityAdmission:
    start = date(trade_date.year, 1, 1)
    end = date(trade_date.year, 12, 31)
    machine = build_observation(
        provider="baostock",
        contract_version="r2f4.1-baostock-calendar-days-v1",
        range_start=start,
        range_end=end,
        observed_at=datetime.combine(trade_date, time(0), tzinfo=UTC),
        days=tuple(
            CalendarMachineDayV1(
                date=start + __import__("datetime").timedelta(days=i), is_open=True
            )
            for i in range((end - start).days + 1)
        ),
    )
    calendar = ImmutableCalendarSnapshot(
        (
            CalendarConfigSnapshot(
                year=trade_date.year,
                status="confirmed",
                published_on=trade_date,
                sources=(),
                closed_dates=(),
            ),
        ),
        generation_sha256="0" * 64,
    )
    generation0 = CalendarGenerationV1(
        sequence=1,
        parent_sha256=None,
        bundled_sha256="a" * 64,
        source_sha256="a" * 64,
        attempt_target_year=trade_date.year,
        attempt_slot_date=trade_date,
        official_body_hashes=("a" * 64, "a" * 64),
        machine=machine,
        promoted_at=datetime.combine(trade_date, time(1), tzinfo=UTC),
        generation_sha256="0" * 64,
    )
    generation = generation0.model_copy(
        update={"generation_sha256": build_generation_sha256(generation0)}
    )
    calendar = ImmutableCalendarSnapshot(
        (
            CalendarConfigSnapshot(
                year=trade_date.year,
                status="confirmed",
                published_on=trade_date,
                sources=(),
                closed_dates=(),
            ),
        ),
        generation_sha256=generation.generation_sha256,
    )
    result = CalendarReadResult(status="ready", generation=generation, calendar=calendar)
    admission = object.__new__(CalendarAuthorityAdmission)
    object.__setattr__(admission, "read_result", result)
    object.__setattr__(admission, "head_generation_sha256", generation.generation_sha256)
    return admission


def _mapping() -> UniverseSemanticMappingV1:
    payload = {
        "mapping_id": "baostock-instrument-v2",
        "mapping_version": "v2",
        "security_type": "stock",
        "exchange": "SSE",
        "board": "main",
        "list_date": "source",
        "delist_date": "source",
        "listing_status": "listed",
        "daily_trade_status": "1",
        "suspension_state": "trading",
        "st_state": "no",
        "expected_trading_state": "trading",
    }
    preimage = {
        "mapping_id": "baostock-instrument-v2",
        "mapping_version": "v2",
        "provider_id": "baostock",
        "source_schema": "classification.instrument.v2",
        "payload": payload,
    }
    return UniverseSemanticMappingV1(
        mapping_id="baostock-instrument-v2",
        mapping_version="v2",
        provider_id="baostock",
        source_schema="classification.instrument.v2",
        payload_json=canonical_json_bytes(payload).decode(),
        mapping_sha256=domain_sha256("stock-eva/r2f4.2/semantic-mapping/v1", preimage),
        authority_status="reviewed",
    )


def _snapshot(members: tuple[UniverseMemberV1, ...]) -> RequiredSymbolSnapshotV1:
    return RequiredSymbolSnapshotV1.from_user_capture(_user_capture(members))


def _user_capture(members: tuple[UniverseMemberV1, ...]):
    with tempfile.TemporaryDirectory() as directory:
        path = __import__("pathlib").Path(directory) / "user.sqlite3"
        store = UserStore(path)
        connection = store._connect()
        try:
            required = sorted(
                item.symbol for item in members if "required_user" in item.scope_roles
            )
            for ordinal, symbol in enumerate(required, start=1):
                connection.execute(
                    "INSERT INTO positions VALUES (?,?,?,?,?,?,?,?,?)",
                    (
                        f"fixture-position-{ordinal}",
                        symbol,
                        "1",
                        "1",
                        "2026-01-01",
                        "0",
                        1,
                        "2026-01-01T00:00:00+00:00",
                        "2026-01-01T00:00:00+00:00",
                    ),
                )
            connection.commit()
        finally:
            connection.close()
        return store.capture_required_symbol_snapshot_existing()


def _evidence(member: UniverseMemberV1, trade_date: date) -> UniverseInstrumentEvidenceV1:
    values = {
        "provider_id": "baostock",
        "authority_status": "reviewed",
        "artifact_origin": "production_reviewed_artifact",
        "security_id": member.security_id,
        "symbol": member.symbol,
        "mapping_id": "baostock-instrument-v2",
        "mapping_version": "v2",
        "mapping_sha256": _mapping().mapping_sha256,
        "source_snapshot_date": min(trade_date, date(2026, 8, 31)),
        "evidence_trade_date": trade_date,
        "exclusion_reason": "",
        "index_role": "required_index" if member.member_kind == "index" else "not_applicable",
        "source_date_semantics": "source_observed",
        "adapter_version": "r2f2.v1",
        "source_schema": "classification.instrument.v2",
        "security_type": "index" if member.member_kind == "index" else "stock",
        "exchange": member.exchange,
        "board": member.board,
        "list_date": member.list_date,
        "delist_date": member.delist_date,
        "listing_status": (
            member.expected_trading_state
            if member.expected_trading_state in {"not_yet_listed", "delisted"}
            else "listed"
        ),
        "daily_trade_status": ("1" if member.expected_trading_state == "trading" else "0"),
        "suspension_state": (
            member.expected_trading_state
            if member.expected_trading_state in {"trading", "suspended"}
            else "not_supplied"
        ),
        "st_state": member.st_state,
        "expected_trading_state": member.expected_trading_state,
        "observed_at": datetime.combine(trade_date, time(8), tzinfo=UTC),
        "evidence_json": canonical_json_bytes(
            {
                "security_type": "index" if member.member_kind == "index" else "stock",
                "exchange": member.exchange,
                "board": member.board,
                "list_date": member.list_date.isoformat() if member.list_date else None,
                "delist_date": member.delist_date.isoformat() if member.delist_date else None,
                "listing_status": (
                    member.expected_trading_state
                    if member.expected_trading_state in {"not_yet_listed", "delisted"}
                    else "listed"
                ),
                "daily_trade_status": ("1" if member.expected_trading_state == "trading" else "0"),
                "suspension_state": (
                    member.expected_trading_state
                    if member.expected_trading_state in {"trading", "suspended"}
                    else "not_supplied"
                ),
                "st_state": member.st_state,
                "expected_trading_state": member.expected_trading_state,
            }
        ).decode(),
    }
    values["lineage_hash"] = domain_sha256(
        "stock-eva/r2f4.2/instrument-lineage/v1",
        {
            "provider_id": values["provider_id"],
            "security_id": values["security_id"],
            "symbol": values["symbol"],
            "mapping_id": values["mapping_id"],
            "mapping_version": values["mapping_version"],
            "mapping_sha256": values["mapping_sha256"],
            "source_schema": values["source_schema"],
            "source_snapshot_date": values["source_snapshot_date"].isoformat(),
            "evidence_trade_date": values["evidence_trade_date"].isoformat(),
            "listing_status": values["listing_status"],
            "daily_trade_status": values["daily_trade_status"],
            "suspension_state": values["suspension_state"],
            "st_state": values["st_state"],
            "expected_trading_state": values["expected_trading_state"],
            "observed_at": values["observed_at"].isoformat(),
        },
    )
    draft = UniverseInstrumentEvidenceV1.model_construct(
        evidence_id=member.instrument_evidence_id, evidence_sha256="0" * 64, **values
    )
    return UniverseInstrumentEvidenceV1(
        evidence_id=member.instrument_evidence_id,
        evidence_sha256=domain_sha256("stock-eva/r2f4.2/instrument-evidence/v1", draft.preimage()),
        **values,
    )


def _bundle(contract):
    return (
        _mapping(),
        tuple(_evidence(item, contract.trade_date) for item in contract.members),
        _snapshot(contract.members),
    )


def _authority(mapping, evidence):
    projection = [
        {
            "evidence_id": item.evidence_id,
            "evidence_sha256": item.evidence_sha256,
            "mapping_id": item.mapping_id,
            "mapping_version": item.mapping_version,
        }
        for item in sorted(evidence, key=lambda value: value.evidence_id)
    ]
    return UniverseAuthorityBundleV1(
        mapping=mapping,
        evidence=evidence,
        source_date_semantics="source_observed",
        authority_sha256=domain_sha256(
            "stock-eva/r2f4.2/universe-authority-bundle/v1",
            {
                "mapping_sha256": mapping.mapping_sha256,
                "source_date_semantics": "source_observed",
                "evidence": projection,
            },
        ),
    )


def test_member_and_count_hashes_are_deterministic_and_equation_is_fail_closed():
    left = _member("sh.600000")
    right = _member("sh.600001")
    assert left.member_sha256 != right.member_sha256
    assert UniverseCountsV1(
        total=2,
        trading=1,
        suspended=1,
        not_yet_listed=0,
        delisted=0,
        unknown=0,
        session_expected=2,
        loaded=None,
        critical_attribute_unknown_count=0,
    )
    with pytest.raises(ValueError):
        UniverseCountsV1(
            total=2,
            trading=2,
            suspended=0,
            not_yet_listed=0,
            delisted=0,
            unknown=0,
            session_expected=1,
            loaded=None,
            critical_attribute_unknown_count=0,
        )


def test_unknown_one_loaded_match_rejects_contract_publication():
    contract = _contract((_member("sh.600000", state="unknown"),))
    assert contract.publication_eligible is False
    assert contract.counts.unknown == 1


def test_requested_unverified_source_is_never_publishable():
    contract = _contract(source_date_semantics="requested_unverified")
    assert contract.publication_eligible is False


def test_required_index_identity_and_state_are_closed():
    with pytest.raises(ValueError):
        _member("sh.000001", role="required_index", state="suspended")
    with pytest.raises(ValueError):
        build_universe_contract(
            trade_date=date(2026, 9, 1),
            calendar_generation_id="cal-1",
            calendar_sha256="a" * 64,
            classification_generation_id="cls-1",
            classification_generation_sequence=1,
            classification_source="fixture",
            classification_source_version="v1",
            classification_source_snapshot_date=date(2026, 8, 31),
            classification_observed_at="2026-09-01T08:00:00Z",
            exact_pit_cutoff="2026-09-01T18:00:00Z",
            source_refs=_contract().source_refs,
            members=(_member("sh.600000"),),
            calendar_authority=_calendar_authority(date(2026, 9, 1)),
        )


def test_sidecar_refuses_promotion_without_persisted_evidence_bundle(tmp_path):
    path = tmp_path / "market_universe.sqlite3"
    store = UniverseSidecarStore(path)
    store.initialize()
    with pytest.raises(UniverseStoreUnavailable):
        store.promote(
            _contract(),
            expected_sequence=0,
            expected_head_sha256=None,
            calendar_authority=_calendar_authority(date(2026, 9, 1)),
        )
    with sqlite3.connect(path) as connection:
        assert connection.execute("SELECT COUNT(*) FROM universe_head").fetchone()[0] == 0
        assert connection.execute("SELECT COUNT(*) FROM universe_contract").fetchone()[0] == 0


def test_raw_gate_unknown_and_state_mismatch_fail_closed():
    from tests.test_market_provider_contract import _provider_raw_batch

    batch = _provider_raw_batch()
    unknown_contract = _contract(
        (_member("sh.600000", state="unknown"),), trade_date=batch.request.trade_date
    )
    unknown_result = validate_provider_raw_batch(batch, unknown_contract)
    assert unknown_result.result == "reject"
    assert unknown_result.reason_code == "UNIVERSE_UNKNOWN_NONZERO"

    suspended_contract = _contract(
        (_member("sh.600000", state="suspended"),), trade_date=batch.request.trade_date
    )
    state_result = validate_provider_raw_batch(batch, suspended_contract)
    assert state_result.result == "reject"
    assert state_result.reason_code == "UNIVERSE_STATE_MISMATCH"


def test_sidecar_reader_rejects_missing_bidirectional_role_link(tmp_path):
    path = tmp_path / "market_universe.sqlite3"
    store = UniverseSidecarStore(path)
    store.initialize()
    contract = _contract()
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
    with sqlite3.connect(path) as connection:
        connection.execute("DROP TRIGGER contract_evidence_no_delete")
        connection.execute(
            "DELETE FROM contract_evidence WHERE contract_id=? AND evidence_role='member' "
            "AND symbol='sh.600000'",
            (contract.contract_id,),
        )
        connection.commit()
    with pytest.raises(UniverseStoreUnavailable):
        store.read_head()


def test_sidecar_initializes_without_head_and_cas_promotes_then_rejects_tamper(tmp_path):
    path = tmp_path / "market_universe.sqlite3"
    store = UniverseSidecarStore(path)
    store.initialize()
    assert store.read_head() is None
    contract = _contract()
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
    head = store.read_head()
    assert head is not None and head.sequence == 1
    with sqlite3.connect(path) as connection:
        connection.execute("DROP TRIGGER universe_contract_no_update")
        connection.execute("UPDATE universe_contract SET payload_json='{}'")
        connection.commit()
    with pytest.raises(UniverseStoreUnavailable):
        store.read_head()


def test_sidecar_cas_conflict_does_not_change_head(tmp_path):
    store = UniverseSidecarStore(tmp_path / "market_universe.sqlite3")
    store.initialize()
    first = _contract()
    mapping, evidence, snapshot = _bundle(first)
    store.promote(
        first,
        expected_sequence=0,
        expected_head_sha256=None,
        mapping=mapping,
        evidence=evidence,
        authority_bundle=_authority(mapping, evidence),
        required_snapshot=snapshot,
        calendar_authority=_calendar_authority(first.trade_date),
    )
    with pytest.raises(UniverseStoreUnavailable):
        candidate = _contract((_member("sh.600002"),))
        mapping, evidence, snapshot = _bundle(candidate)
        store.promote(
            candidate,
            expected_sequence=0,
            expected_head_sha256=None,
            mapping=mapping,
            evidence=evidence,
            authority_bundle=_authority(mapping, evidence),
            required_snapshot=snapshot,
            calendar_authority=_calendar_authority(candidate.trade_date),
        )
    assert store.read_head().sequence == 1


def test_sidecar_schema_is_exact_and_parent_cas_is_atomic(tmp_path):
    path = tmp_path / "market_universe.sqlite3"
    store = UniverseSidecarStore(path)
    store.initialize()
    with sqlite3.connect(path) as connection:
        tables = {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
            )
        }
        assert "universe_source_state" in tables
        assert "universe_attempt_result" in tables
        assert connection.execute("PRAGMA user_version").fetchone()[0] == 1
        assert connection.execute("PRAGMA journal_mode").fetchone()[0].lower() == "wal"

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
        calendar_authority=_calendar_authority(first.trade_date),
    )
    second = _contract(
        (_member("sh.600002"), _member("sh.000001", role="required_index")),
        sequence=2,
        parent_contract_id=first.contract_id,
    )
    mapping, evidence, snapshot = _bundle(second)
    second_head = store.promote(
        second,
        expected_sequence=first_head.sequence,
        expected_head_sha256=first_head.head_sha256,
        mapping=mapping,
        evidence=evidence,
        authority_bundle=_authority(mapping, evidence),
        required_snapshot=snapshot,
        calendar_authority=_calendar_authority(second.trade_date),
    )
    assert second_head.sequence == 2
    with pytest.raises(UniverseStoreUnavailable):
        candidate = _contract(
            (_member("sh.600003"), _member("sh.000001", role="required_index")),
            sequence=3,
            parent_contract_id=second.contract_id,
        )
        mapping, evidence, snapshot = _bundle(candidate)
        store.promote(
            candidate,
            expected_sequence=1,
            expected_head_sha256=first_head.head_sha256,
            mapping=mapping,
            evidence=evidence,
            required_snapshot=snapshot,
            calendar_authority=_calendar_authority(candidate.trade_date),
        )
    assert store.read_head().head_sha256 == second_head.head_sha256
    with sqlite3.connect(path) as connection:
        assert connection.execute("SELECT COUNT(*) FROM universe_contract").fetchone()[0] == 2


def test_raw_batch_validator_rejects_partial_candidate_before_normalize():
    contract = _contract()
    with pytest.raises((ValueError, UniverseStoreUnavailable)):
        validate_provider_raw_batch(object(), contract)
