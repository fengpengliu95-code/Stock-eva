import json
import sys
from datetime import UTC, date, datetime, timedelta
from types import SimpleNamespace

import pytest

import backend.app.cli as cli
from backend.app.classification.models import (
    TAXONOMY_BAOSTOCK_INDUSTRY,
    ClassificationSnapshot,
    SectorMembershipRecord,
    SecurityMasterRecord,
)
from backend.app.classification.store import ClassificationStore
from backend.app.config import Settings
from backend.app.regime.acceptance import ReleaseOneAcceptanceService
from backend.app.regime.service import MarketRegimeService
from backend.app.regime.snapshots import (
    RegimeSnapshotStore,
    SnapshotCaptureRequest,
)
from backend.app.regime.store import MarketRegimeStore
from backend.app.storage.dataset import PublishedReadSnapshot


class EmptyBoundReader:
    def __init__(self, dates: tuple[date, ...]) -> None:
        self.snapshot = PublishedReadSnapshot(
            identity="/private/verified-dataset:fixture",
            paths=(),
            fingerprints=(),
            generation="generation-fixture",
            trade_dates=dates,
        )
        self.capture_calls = 0

    def cache_snapshot(self) -> PublishedReadSnapshot:
        self.capture_calls += 1
        return self.snapshot

    def bars_through_snapshot(self, *_args, **_kwargs):
        return []


class ClassificationReader:
    def __init__(
        self,
        dates: tuple[date, ...],
        *,
        mapped: int = 20,
        declared_eligible: int = 20,
        future_membership: bool = False,
    ) -> None:
        self.dates = dates
        self.mapped = mapped
        self.declared_eligible = declared_eligible
        self.future_membership = future_membership
        self.cutoffs: list[datetime] = []

    def read_snapshot(self, as_of: date, *, known_at, **_kwargs):
        self.cutoffs.append(known_at)
        observed_at = datetime.combine(self.dates[-1] + timedelta(days=1), datetime.min.time(), UTC)
        securities = [
            SimpleNamespace(
                security_id=f"security-{index}",
                symbol=f"sh.{600000 + index:06d}",
                security_type="stock",
                exchange="sh",
                board="main",
                list_date=self.dates[0],
                delist_date=None,
            )
            for index in range(20)
        ]
        memberships = [
            SimpleNamespace(
                security_id=f"security-{index}",
                source_snapshot_date=(
                    as_of + timedelta(days=1)
                    if self.future_membership and as_of == self.dates[-1] and index == 0
                    else self.dates[0]
                ),
                observed_at=observed_at,
                effective_from=(
                    as_of + timedelta(days=1)
                    if self.future_membership and as_of == self.dates[-1] and index == 0
                    else self.dates[0]
                ),
                effective_to=None,
            )
            for index in range(self.mapped)
        ]
        unmapped = [row.symbol for row in securities[self.mapped :]]
        audit = SimpleNamespace(
            taxonomy_id=TAXONOMY_BAOSTOCK_INDUSTRY,
            eligible_count=self.declared_eligible,
            mapped_count=self.mapped,
            coverage_ratio=self.mapped / self.declared_eligible,
            unmapped_symbols=unmapped,
        )
        generation = SimpleNamespace(
            source_snapshot_date=self.dates[0],
            observed_at=observed_at,
            coverage_audits=[audit],
        )
        return SimpleNamespace(
            generation=generation,
            securities=securities,
            sector_memberships=memberships,
        )


def _dates(count: int = 20) -> tuple[date, ...]:
    start = date(2026, 7, 1)
    return tuple(start + timedelta(days=index) for index in range(count))


def _real_classification_store(tmp_path, dates: tuple[date, ...]) -> ClassificationStore:
    observed_at = datetime(2026, 8, 1, tzinfo=UTC)
    securities = []
    memberships = []
    for index in range(20):
        symbol = f"sh.{600000 + index:06d}"
        security_id = f"baostock:{symbol}"
        securities.append(
            SecurityMasterRecord(
                record_id=f"security:{symbol}",
                security_id=security_id,
                symbol=symbol,
                name=f"Fixture {symbol}",
                exchange="sh",
                board="main",
                security_type="stock",
                list_date=date(2000, 1, 1),
                delist_date=None,
                is_tradable=True,
                listing_status="1",
                daily_trade_status="1",
                source="baostock",
                source_version="0.9.3",
                source_snapshot_date=dates[0],
                effective_from=date(2000, 1, 1),
                effective_to=None,
                observed_at=observed_at,
                source_record_id=f"query_all_stock:{symbol}",
                lineage_hash=f"security-lineage:{symbol}",
                price_available=True,
            )
        )
        memberships.append(
            SectorMembershipRecord(
                record_id=f"sector:{symbol}",
                taxonomy_id=TAXONOMY_BAOSTOCK_INDUSTRY,
                taxonomy_name="BaoStock industryClassification",
                sector_id="bank",
                sector_name="Bank",
                security_id=security_id,
                symbol=symbol,
                raw_industry="Bank",
                raw_classification="证监会行业分类",
                source="baostock",
                source_version="0.9.3",
                source_snapshot_date=dates[0],
                effective_from=dates[0],
                effective_to=None,
                observed_at=observed_at,
                source_record_id=f"query_stock_industry:{symbol}",
                lineage_hash=f"sector-lineage:{symbol}",
            )
        )
    store = ClassificationStore(
        tmp_path / "classification.duckdb",
        temp_directory=tmp_path / "classification-temp",
    )
    store.publish(
        ClassificationSnapshot(
            source="baostock",
            source_version="0.9.3",
            source_snapshot_date=dates[0],
            observed_at=observed_at,
            securities=securities,
            sector_memberships=memberships,
        )
    )
    return store


def _service(tmp_path, *, classification=None, capture_count: int = 20):
    dates = _dates()
    reader = EmptyBoundReader(dates)
    regime_store = MarketRegimeStore(reader)
    snapshots = RegimeSnapshotStore(tmp_path / "derived" / "snapshots.sqlite3")
    evaluator = MarketRegimeService()
    for as_of in dates[:capture_count]:
        result = evaluator.evaluate(regime_store.read_bound(as_of, reader.snapshot))
        snapshots.capture(
            SnapshotCaptureRequest(
                result=result,
                capture_mode="post_hoc_backfill",
                evidence_cutoff_at=datetime(2026, 8, 1, tzinfo=UTC),
                dataset_generation=reader.snapshot.generation,
                dataset_identity=reader.snapshot.identity,
            )
        )
    classifications = classification or ClassificationReader(dates)
    return (
        ReleaseOneAcceptanceService(
            regime_store=regime_store,
            snapshot_store=snapshots,
            classification_store=classifications,
        ),
        reader,
        classifications,
        snapshots,
    )


def test_release_one_audit_accepts_post_hoc_replay_but_reports_late_classification(
    tmp_path,
) -> None:
    service, reader, classifications, snapshots = _service(tmp_path)
    before = snapshots.path.read_bytes()

    report = service.run(
        sessions=20,
        audit_cutoff_at=datetime(2026, 8, 3, tzinfo=UTC),
    )

    assert report.status == "ready"
    assert report.session_count == report.snapshot_count == report.matching_result_count == 20
    assert report.post_hoc_verified_sessions == 20
    assert report.contemporaneous_classification_sessions == 0
    assert report.contemporaneous_unverified_sessions == 20
    assert report.coverage.coverage_ratio == 1
    assert report.coverage.unmapped_symbols == []
    assert report.future_price_reads == 0
    assert report.future_lineage_reads == 0
    assert report.future_evidence_reads == 0
    assert report.future_membership_reads == 0
    assert "classification_history_observed_late" in report.quality_issues
    assert reader.capture_calls == 1
    assert len(set(classifications.cutoffs)) == 1
    assert snapshots.path.read_bytes() == before


def test_release_one_audit_reads_real_classification_store_without_mutation(tmp_path) -> None:
    dates = _dates()
    classification = _real_classification_store(tmp_path / "classification", dates)
    service, *_ = _service(tmp_path / "audit", classification=classification)
    before = classification.path.read_bytes()
    before_mtime = classification.path.stat().st_mtime_ns

    report = service.run(
        sessions=20,
        audit_cutoff_at=datetime(2026, 8, 3, tzinfo=UTC),
    )

    assert report.status == "ready"
    assert report.classification_available_sessions == 20
    assert report.contemporaneous_classification_sessions == 0
    assert report.coverage.eligible_count == 20
    assert report.coverage.mapped_count == 20
    assert classification.path.read_bytes() == before
    assert classification.path.stat().st_mtime_ns == before_mtime


@pytest.mark.parametrize(
    ("capture_count", "mapped", "declared", "expected_issue"),
    [
        (19, 20, 20, "missing_regime_snapshots"),
        (20, 18, 20, "classification_coverage_below_target"),
        (20, 20, 21, "classification_counts_unreconciled"),
    ],
)
def test_release_one_audit_fails_closed_on_missing_or_bad_coverage(
    tmp_path,
    capture_count: int,
    mapped: int,
    declared: int,
    expected_issue: str,
) -> None:
    dates = _dates()
    service, *_ = _service(
        tmp_path,
        capture_count=capture_count,
        classification=ClassificationReader(
            dates,
            mapped=mapped,
            declared_eligible=declared,
        ),
    )

    report = service.run(
        sessions=20,
        audit_cutoff_at=datetime(2026, 8, 3, tzinfo=UTC),
    )

    assert report.status == "not_ready"
    assert expected_issue in report.quality_issues


def test_release_one_audit_rejects_future_membership_and_effective_window(tmp_path) -> None:
    dates = _dates()
    service, *_ = _service(
        tmp_path,
        classification=ClassificationReader(dates, future_membership=True),
    )

    report = service.run(
        sessions=20,
        audit_cutoff_at=datetime(2026, 8, 3, tzinfo=UTC),
    )

    assert report.status == "not_ready"
    assert report.future_membership_reads == 1
    assert report.future_effective_window_reads == 1
    assert "future_membership_reads" in report.quality_issues
    assert "future_effective_window_reads" in report.quality_issues


def test_release_one_audit_requires_at_least_twenty_sessions(tmp_path) -> None:
    service, *_ = _service(tmp_path)

    with pytest.raises(ValueError, match="at least 20"):
        service.run(
            sessions=19,
            audit_cutoff_at=datetime(2026, 8, 3, tzinfo=UTC),
        )


def test_release_one_audit_fails_closed_on_recompute_drift(tmp_path, monkeypatch) -> None:
    service, *_ = _service(tmp_path)

    def changed_read(*_args, **_kwargs):
        raise RuntimeError("/private/dataset changed during query")

    monkeypatch.setattr(service.regime_store, "read_bound", changed_read)

    report = service.run(
        sessions=20,
        audit_cutoff_at=datetime(2026, 8, 3, tzinfo=UTC),
    )

    assert report.status == "not_ready"
    assert report.matching_result_count == 0
    assert report.quality_issues.count("dataset_unavailable") == 1
    assert "/private" not in report.model_dump_json()


def test_release_one_cli_rejects_small_session_count_before_settings(
    monkeypatch,
    capsys,
) -> None:
    monkeypatch.setattr(
        cli,
        "get_settings",
        lambda: (_ for _ in ()).throw(AssertionError("invalid request reached settings")),
    )
    monkeypatch.setattr(
        sys,
        "argv",
        ["stock-eva", "release-one-audit", "--sessions", "19"],
    )

    assert cli.main() == 1
    captured = capsys.readouterr()
    assert captured.err == ""
    assert json.loads(captured.out) == {
        "error_code": "release_one_audit_unavailable",
        "status": "not_ready",
        "writes_snapshot_data": False,
    }


def test_release_one_cli_is_one_sanitized_read_only_json_line(
    tmp_path,
    monkeypatch,
    capsys,
) -> None:
    settings = Settings(
        local_control_dir=tmp_path / "control",
        market_data_dir=tmp_path / "market",
        local_temp_dir=tmp_path / "temp",
    )

    class FakeAudit:
        def __init__(self, **_kwargs) -> None:
            print("/private/audit cli-secret")

        def run(self, **_kwargs):
            print("/private/query cli-secret")
            return SimpleNamespace(
                status="ready",
                model_dump=lambda **_kwargs: {"status": "ready", "session_count": 20},
            )

    monkeypatch.setattr(cli, "get_settings", lambda: settings)
    monkeypatch.setattr(cli, "market_regime_store_from_settings", lambda _settings: object())
    monkeypatch.setattr(cli, "RegimeSnapshotStore", lambda _path: object())
    monkeypatch.setattr(cli, "ClassificationStore", lambda *_args, **_kwargs: object())
    monkeypatch.setattr(cli, "ReleaseOneAcceptanceService", FakeAudit)
    monkeypatch.setattr(
        sys,
        "argv",
        ["stock-eva", "release-one-audit", "--sessions", "20"],
    )

    assert cli.main() == 0
    captured = capsys.readouterr()
    assert captured.err == ""
    assert captured.out.count("\n") == 1
    assert "/private" not in captured.out
    assert "cli-secret" not in captured.out
    assert json.loads(captured.out) == {
        "session_count": 20,
        "status": "ready",
        "writes_snapshot_data": False,
    }
    assert not settings.local_control_dir.exists()
