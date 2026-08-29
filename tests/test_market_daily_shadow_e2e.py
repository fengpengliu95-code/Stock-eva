from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

from backend.app.market.daily_shadow_candidates import DailyCandidateStore
from backend.app.market.daily_shadow_canonical import PublishedDailyCanonicalProjection
from backend.app.market.daily_shadow_models import (
    CanonicalDailyOhlcRow,
    DailyCanonicalLineageState,
    DailyCanonicalReadResult,
    DailyCanonicalSnapshot,
    DailyShadowFetchObservation,
    DailyShadowFetchResult,
    DailyShadowSourceRow,
    canonical_to_tickflow_daily_symbol,
    domain_sha256,
)
from backend.app.market.daily_shadow_registry import (
    DAILY_SHADOW_TERMS_CONTRACT_VERSION,
    DailyShadowContract,
    DailyShadowRegistry,
)
from backend.app.market.daily_shadow_worker import DailyShadowWorker
from backend.app.market.providers.shadow_contracts import TermsEvidence
from backend.app.market.shadow_calendar import ConfirmedSessionSnapshot
from backend.app.market.shadow_evidence import ShadowEvidenceReader, ShadowEvidenceStore

START = date(2026, 7, 14)
NOW = datetime(2026, 8, 28, tzinfo=UTC)
SYMBOLS = ("sh.600000", "sz.000001")
EXTRA_SYMBOL = "sh.600001"
UNIVERSE_POLICY_SHA256 = domain_sha256(
    "stock-eva/r2f3/daily-canonical-universe-policy/v1",
    {
        "eligible": "active-ready-sh-sz-stock-positive-legal-ohlc",
        "excluded": "index-or-suspended-nontrading-hashed",
        "mapping": "sh-sz-six-digit-reversible-v1",
    },
)


class _Reader:
    def __init__(self, snapshot: DailyCanonicalSnapshot):
        self.snapshot = snapshot

    def verify(self) -> DailyCanonicalReadResult:
        return DailyCanonicalReadResult(status="ready", snapshot=self.snapshot)


def _tree(root: Path) -> dict[str, str]:
    return {
        str(path.relative_to(root)): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in sorted(root.rglob("*"))
        if path.is_file() and not path.is_symlink()
    }


def _snapshot(trade_date: date, *, symbols: tuple[str, ...] = SYMBOLS) -> DailyCanonicalSnapshot:
    price_rows = {
        "sh.600000": (10.0, 10.4, 9.9, 10.2),
        "sz.000001": (11.0, 11.4, 10.9, 11.2),
        EXTRA_SYMBOL: (12.0, 12.4, 11.9, 12.2),
    }
    rows = tuple(
        CanonicalDailyOhlcRow(
            trade_date=trade_date,
            symbol=symbol,
            open=Decimal(str(open_value)),
            high=Decimal(str(high)),
            low=Decimal(str(low)),
            close=Decimal(str(close)),
        )
        for symbol in symbols
        for open_value, high, low, close in (price_rows[symbol],)
    )
    mapping = tuple((symbol, canonical_to_tickflow_daily_symbol(symbol)) for symbol in symbols)
    return DailyCanonicalSnapshot(
        trade_date=trade_date,
        lineage_state=DailyCanonicalLineageState.LEGACY_UNAVAILABLE,
        manifest_generation=f"generation-{trade_date.isoformat()}",
        manifest_sha256=domain_sha256("stock-eva/test/r2f3/manifest/v1", trade_date.isoformat()),
        partition_relative_path=(f"bars/source=baostock/date={trade_date.isoformat()}.parquet"),
        partition_sha256=domain_sha256("stock-eva/test/r2f3/partition/v1", trade_date.isoformat()),
        partition_row_count=len(symbols),
        eligible_symbol_count=len(symbols),
        excluded_symbol_count=0,
        canonical_universe_sha256="0" * 64,
        canonical_exclusion_sha256=domain_sha256("stock-eva/test/r2f3/exclusions/v1", ()),
        symbol_mapping_sha256=domain_sha256("stock-eva/r2f3/daily-symbol-mapping/v1", mapping),
        ohlc_sha256=domain_sha256(
            "stock-eva/test/r2f3/ohlc/v1",
            tuple(row.model_dump(mode="json") for row in rows),
        ),
        rows=rows,
    )


def _calendar(snapshots: dict[date, DailyCanonicalSnapshot]) -> ConfirmedSessionSnapshot:
    dates = tuple(snapshots)
    values = {
        "snapshot_id": "daily-calendar-clean-window",
        "provider_id": "tickflow",
        "window_id": "daily-window-clean",
        "calendar_generation": "calendar-generation-clean",
        "calendar_sha256": "5" * 64,
        "confirmed_next_sessions": [item.isoformat() for item in dates],
        "universe_id": "daily-canonical-active-universe",
        "universe_sha256": UNIVERSE_POLICY_SHA256,
        "captured_at": NOW.isoformat(),
    }
    payload = (json.dumps(values, sort_keys=True, separators=(",", ":")) + "\n").encode()
    return ConfirmedSessionSnapshot(
        **values,
        snapshot_sha256=hashlib.sha256(payload).hexdigest(),
    )


def _terms() -> TermsEvidence:
    return TermsEvidence.build(
        content_bytes=b"reviewed TickFlow Free Daily Bar terms",
        official_url_allowlist=("https://free-api.tickflow.org",),
        terms_evidence_id="tickflow-free-daily-bar-20260828-e2e",
        provider_id="tickflow",
        content_object_relpath="terms/tickflow-free-daily-bar-20260828-e2e.txt",
        contract_version=DAILY_SHADOW_TERMS_CONTRACT_VERSION,
        as_of_date="2026-08-28",
        reviewer="stock-eva-owner",
        review_id="r2f3-free-daily-bar-e2e-review-20260828",
        approved_intended_use="free-historical-daily-ohlc-shadow-only",
        approved_retention="final-success-source-evidence-only",
        approved_credential_mode="credentialless-free",
        approved_quota_decision="unqualified-max-40-sequential-one-attempt",
    )


def _fetch(plan, *, mismatch: bool = False) -> DailyShadowFetchResult:
    timestamp = int(
        datetime.combine(plan.trade_date, datetime.min.time(), tzinfo=UTC).timestamp() * 1000
    )
    prices = {
        "sh.600000": (10.0, 10.4, 9.9, 10.22 if mismatch else 10.2),
        "sz.000001": (11.0, 11.4, 10.9, 11.2),
        EXTRA_SYMBOL: (12.0, 12.4, 11.9, 12.2),
    }
    rows = tuple(
        DailyShadowSourceRow(
            trade_date=plan.trade_date,
            timestamp=timestamp,
            provider_symbol=canonical_to_tickflow_daily_symbol(symbol),
            symbol=symbol,
            open=open_value,
            high=high,
            low=low,
            close=close,
            volume=1000,
            amount=10000.0,
        )
        for shard in plan.shards
        for symbol in shard.canonical_symbols
        for open_value, high, low, close in (prices[symbol],)
    )
    return DailyShadowFetchResult(
        status="ready",
        trade_date=plan.trade_date,
        request_plan_sha256=plan.request_plan_sha256,
        request_count=1,
        rows=rows,
        observations=(
            DailyShadowFetchObservation(
                ordinal=0,
                outcome="SUCCESS",
                elapsed_ms=1,
                response_bytes=100,
                expected_rows=len(plan.shards[0].canonical_symbols),
                observed_rows=len(plan.shards[0].canonical_symbols),
            ),
        ),
    )


def test_one_reset_then_clean_twenty_session_epoch_is_only_qualified_epoch(tmp_path):
    canonical_root = tmp_path / "canonical"
    canonical_root.mkdir()
    (canonical_root / "manifest.json").write_text("canonical-manifest", encoding="utf-8")
    partition = canonical_root / "bars" / "immutable.parquet"
    partition.parent.mkdir()
    partition.write_bytes(b"immutable-canonical-partition")
    canonical_before = _tree(canonical_root)

    dates = tuple(START + timedelta(days=ordinal) for ordinal in range(20))
    snapshots = {
        trade_date: _snapshot(
            trade_date,
            symbols=SYMBOLS if ordinal % 3 else tuple(sorted((*SYMBOLS, EXTRA_SYMBOL))),
        )
        for ordinal, trade_date in enumerate(dates)
    }
    terms = _terms()
    control = tmp_path / "control"
    control.mkdir(mode=0o700)
    registry = DailyShadowRegistry(control / "daily_bar_shadow.sqlite3", clock=lambda: NOW)
    registry.initialize(DailyShadowContract(terms_evidence_sha256=terms.manifest_sha256), terms)
    evidence_root = tmp_path / "evidence"
    candidate_root = tmp_path / "candidates"
    provider_calls = []
    first_call = True

    def canonical_factory(trade_date):
        snapshot = snapshots[trade_date]
        return PublishedDailyCanonicalProjection(_Reader(snapshot), snapshot)

    def fetcher(plan):
        nonlocal first_call
        provider_calls.append(plan.trade_date)
        result = _fetch(plan, mismatch=first_call)
        first_call = False
        return result

    worker = DailyShadowWorker(
        registry=registry,
        canonical_factory=canonical_factory,
        confirmed_calendar=_calendar(snapshots),
        fetcher=fetcher,
        evidence_store=ShadowEvidenceStore(evidence_root),
        evidence_reader=ShadowEvidenceReader(evidence_root),
        candidate_store=DailyCandidateStore(candidate_root),
        terms_evidence_sha256=terms.manifest_sha256,
        owner="pytest-e2e-worker",
        clock=lambda: NOW,
    )

    reset = worker.run_one(dates[0])
    clean = [worker.run_one(trade_date) for trade_date in dates]

    status = registry.read()
    assert reset.outcome == "MISMATCH"
    assert [result.outcome for result in clean] == ["SUCCESS"] * 20
    assert clean[-1].window_state == "SHADOW_QUALIFIED"
    assert clean[-1].consecutive_sessions == 20
    assert status.window.state == "SHADOW_QUALIFIED"
    assert status.window.consecutive_sessions == 20
    assert status.epoch_count == 2
    assert status.session_report_count == 21
    assert provider_calls == (list(dates[:1]) + list(dates))
    assert len(tuple((evidence_root / "bundles").iterdir())) == 21
    assert len(tuple((candidate_root / "daily-candidates").iterdir())) == 20
    assert status.publication_enabled is False
    assert status.failover_enabled is False
    assert not (control / "provider_registry.sqlite3").exists()
    assert _tree(canonical_root) == canonical_before

    with sqlite3.connect(f"file:{registry.path}?mode=ro", uri=True) as connection:
        epochs = connection.execute(
            "SELECT epoch_state,version_vector_sha256 FROM daily_shadow_epoch "
            "ORDER BY epoch_ordinal"
        ).fetchall()
    assert [item[0] for item in epochs] == ["RESET", "SHADOW_QUALIFIED"]
    assert len({item[1] for item in epochs}) == 1
