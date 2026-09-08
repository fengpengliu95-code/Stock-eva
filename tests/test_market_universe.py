from __future__ import annotations

import sqlite3
from datetime import date

import pytest

from backend.app.market.universe import (
    UniverseCountsV1,
    UniverseMemberV1,
    UniverseSidecarStore,
    UniverseStoreUnavailable,
    build_universe_contract,
    validate_provider_raw_batch,
)


def _member(symbol: str, *, state: str = "trading", role: str = "effective_main_board"):
    return UniverseMemberV1(
        symbol=symbol,
        security_id=f"sec-{symbol.replace('.', '-')}",
        member_kind="stock",
        scope_roles=(role,),
        exchange="SSE" if symbol.startswith("sh.") else "SZSE",
        board="main",
        list_date=date(2020, 1, 1),
        delist_date=None,
        expected_trading_state=state,
        st_state="no",
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
):
    members = members or (_member("sh.600000"), _member("sh.000001", role="required_index"))
    return build_universe_contract(
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
        source_refs={
            "calendar_generation_id": "cal-1",
            "calendar_sha256": "a" * 64,
            "classification_generation_id": "cls-1",
            "classification_generation_sequence": 1,
            "classification_source": "fixture",
            "classification_source_version": "v1",
            "classification_source_snapshot_date": "2026-08-31",
            "classification_observed_at": "2026-09-01T08:00:00Z",
            "classification_snapshot_sha256": "b" * 64,
            "required_symbol_snapshot_id": "snap-1",
            "required_symbol_snapshot_sha256": "c" * 64,
            "instrument_evidence_ids": ["ev-sh-600000", "ev-sh-000001"],
            "semantic_mapping_sha256": "d" * 64,
            "source_version_digest": "e" * 64,
            "provider_id": "baostock",
            "trade_date": "2026-09-01",
            "exact_pit_cutoff": "2026-09-01T18:00:00Z",
            "source_date_semantics": "source_observed",
        },
        members=members,
        sequence=sequence,
        parent_contract_id=parent_contract_id,
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


def test_sidecar_initializes_without_head_and_cas_promotes_then_rejects_tamper(tmp_path):
    path = tmp_path / "market_universe.sqlite3"
    store = UniverseSidecarStore(path)
    store.initialize()
    assert store.read_head() is None
    contract = _contract()
    store.promote(contract, expected_sequence=0, expected_head_sha256=None)
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
    store.promote(first, expected_sequence=0, expected_head_sha256=None)
    with pytest.raises(UniverseStoreUnavailable):
        store.promote(
            _contract((_member("sh.600002"),)), expected_sequence=0, expected_head_sha256=None
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
    first_head = store.promote(first, expected_sequence=0, expected_head_sha256=None)
    second = _contract(
        (_member("sh.600002"), _member("sh.000001", role="required_index")),
        sequence=2,
        parent_contract_id=first.contract_id,
    )
    second_head = store.promote(
        second,
        expected_sequence=first_head.sequence,
        expected_head_sha256=first_head.head_sha256,
    )
    assert second_head.sequence == 2
    with pytest.raises(UniverseStoreUnavailable):
        store.promote(
            _contract(
                (_member("sh.600003"), _member("sh.000001", role="required_index")),
                sequence=3,
                parent_contract_id=second.contract_id,
            ),
            expected_sequence=1,
            expected_head_sha256=first_head.head_sha256,
        )
    assert store.read_head().head_sha256 == second_head.head_sha256
    with sqlite3.connect(path) as connection:
        assert connection.execute("SELECT COUNT(*) FROM universe_contract").fetchone()[0] == 2


def test_raw_batch_validator_rejects_partial_candidate_before_normalize():
    contract = _contract()
    with pytest.raises((ValueError, UniverseStoreUnavailable)):
        validate_provider_raw_batch(object(), contract)
