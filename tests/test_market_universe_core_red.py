from __future__ import annotations

import sqlite3

import pytest

from backend.app.market.universe import (
    UniverseInstrumentEvidenceV1,
    UniverseSidecarStore,
    UniverseStoreUnavailable,
    domain_sha256,
)
from tests.test_market_universe import _authority, _bundle, _contract, _evidence


def test_partition_digest_excludes_member_digest_field():
    contract = _contract()
    projection = [
        member.preimage()
        for member in contract.members
        if "effective_main_board" in member.scope_roles
    ]
    expected = domain_sha256(
        "stock-eva/r2f4.2/universe-partition/effective-main-board/v1", projection
    )
    assert contract.effective_main_board_partition_sha256 == expected


def test_unpublishable_contract_cannot_be_promoted(tmp_path):
    store = UniverseSidecarStore(tmp_path / "market_universe.sqlite3")
    store.initialize()
    contract = _contract(source_date_semantics="requested_unverified")
    mapping, evidence, snapshot = _bundle(contract)
    with pytest.raises(UniverseStoreUnavailable):
        store.promote(
            contract,
            expected_sequence=0,
            expected_head_sha256=None,
            mapping=mapping,
            evidence=evidence,
            required_snapshot=snapshot,
            calendar_authority=__import__(
                "tests.test_market_universe", fromlist=["_calendar_authority"]
            )._calendar_authority(contract.trade_date),
        )


def test_index_evidence_must_bind_required_index_role(tmp_path):
    store = UniverseSidecarStore(tmp_path / "market_universe.sqlite3")
    store.initialize()
    contract = _contract()
    mapping, evidence, snapshot = _bundle(contract)
    first = next(item for item in evidence if item.symbol == "sh.600000")
    values = first.model_dump()
    values["index_role"] = "required_index"
    draft = UniverseInstrumentEvidenceV1.model_construct(**values)
    values["evidence_sha256"] = domain_sha256(
        "stock-eva/r2f4.2/instrument-evidence/v1", draft.preimage()
    )
    with pytest.raises(ValueError):
        UniverseInstrumentEvidenceV1.model_validate(values)
    bad = tuple(
        UniverseInstrumentEvidenceV1.model_construct(**values) if item is first else item
        for item in evidence
    )
    with pytest.raises(UniverseStoreUnavailable):
        store.promote(
            contract,
            expected_sequence=0,
            expected_head_sha256=None,
            mapping=mapping,
            evidence=bad,
            required_snapshot=snapshot,
            calendar_authority=__import__(
                "tests.test_market_universe", fromlist=["_calendar_authority"]
            )._calendar_authority(contract.trade_date),
        )


def test_orphan_evidence_invalidates_global_sidecar(tmp_path):
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
        calendar_authority=__import__(
            "tests.test_market_universe", fromlist=["_calendar_authority"]
        )._calendar_authority(contract.trade_date),
    )
    orphan = _evidence(contract.members[0], contract.trade_date).model_dump()
    orphan["evidence_id"] = "orphan-evidence"
    draft = UniverseInstrumentEvidenceV1.model_construct(**orphan)
    orphan["evidence_sha256"] = domain_sha256(
        "stock-eva/r2f4.2/instrument-evidence/v1", draft.preimage()
    )
    with sqlite3.connect(path) as connection:
        columns = [
            row[1] for row in connection.execute("PRAGMA table_info(universe_instrument_evidence)")
        ]
        placeholders = ",".join("?" for _ in columns)
        connection.execute(
            f"INSERT INTO universe_instrument_evidence ({','.join(columns)}) "
            f"VALUES ({placeholders})",
            tuple(orphan[column] for column in columns),
        )
        connection.commit()
    with pytest.raises(UniverseStoreUnavailable):
        store.read_head()
