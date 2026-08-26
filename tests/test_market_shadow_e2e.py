"""Production-store end-to-end coverage for the isolated shadow lane."""

import json
import os
from datetime import date

import pytest

from backend.app.market.canonical_comparison import (
    CanonicalCandidateReader,
    PublishedCanonicalComparison,
)
from backend.app.market.providers.registry import ShadowRegistry
from backend.app.market.shadow_calendar import ConfirmedCalendarReader
from backend.app.market.shadow_candidates import ShadowCandidateReader, ShadowCandidateStore
from backend.app.market.shadow_evidence import (
    ShadowAttempt,
    ShadowCompletion,
    ShadowEvidenceControlSink,
    ShadowEvidenceReader,
    ShadowEvidenceStore,
    ShadowLogicalRequest,
    ShadowLogicalRequestPlan,
    ShadowRequestCompletion,
)
from backend.app.market.shadow_jobs import (
    CanonicalOutcomeScanner,
    ShadowBundlePublisher,
    ShadowJobStore,
    ShadowOutcomeReporter,
)
from backend.app.market.shadow_normalize import REVIEWED_TUSHARE_UNIT_CONTRACT, normalize_tushare
from backend.app.market.shadow_reconciliation import (
    CanonicalSessionCandidateReader,
    reconcile,
)
from backend.app.market.shadow_scheduler import ShadowScheduler
from backend.app.market.shadow_terminal import (
    ShadowTerminalUnavailable,
    ShadowTerminalWriter,
    TerminalGraphIdentity,
)
from tests.test_market_canonical_comparison import write_canonical_fixture
from tests.test_market_provider_registry import _record, _sha, _terms
from tests.test_market_shadow_jobs import _calendar_root, _write_canonical_dataset


def test_real_shadow_stores_reconcile_attach_and_terminal_success(tmp_path):
    comparison_root = tmp_path / "comparison"
    comparison_root.mkdir()
    capability_dataset, canonical_candidate, canonical_evidence = write_canonical_fixture(
        comparison_root
    )
    canonical_capability = PublishedCanonicalComparison.open(
        CanonicalCandidateReader(
            capability_dataset,
            canonical_candidate,
            evidence_root=canonical_evidence,
        )
    )
    capability_snapshot = canonical_capability.verify().snapshot
    assert capability_snapshot is not None
    dataset = tmp_path / "canonical-scan"
    _write_canonical_dataset(dataset, trade_date=date(2026, 8, 20))
    manifest_path = dataset / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    manifest["files"][0]["universe_id"] = "main-board-v1"
    manifest_path.write_text(json.dumps(manifest, sort_keys=True, separators=(",", ":")) + "\n")
    canonical_side = CanonicalSessionCandidateReader(dataset, trade_date=date(2026, 8, 20)).read()

    calendar_parent = tmp_path / "calendar-root"
    calendar_parent.mkdir()
    calendar_root = _calendar_root(calendar_parent)
    calendar = ConfirmedCalendarReader(
        calendar_root.resolve(),
        provider_id="tushare",
        window_id="tushare-window-1",
        universe_id="main-board-v1",
        universe_sha256="a" * 64,
    )
    snapshot = calendar.read(date(2026, 8, 20), date(2026, 8, 21))
    version_vector = _sha("e2e-version")

    registry = ShadowRegistry.in_memory()
    registry.initialize()
    registry.put_provider(_record("tushare"))
    terms = _terms("tushare")
    registry.put_terms_evidence(terms)
    registry.attach_terms_to_provider("tushare", terms)
    state = registry.transition("tushare", "canary", expected_state_version=1)
    registry.transition("tushare", "shadow", expected_state_version=state.state_version)
    window = registry.ensure_window(
        "tushare", version_vector, snapshot.calendar_generation, snapshot.calendar_sha256
    )

    job_id = "job-e2e"
    session_id = "session-e2e"
    request = ShadowLogicalRequest(
        job_id=job_id,
        provider_id="tushare",
        window_id=window.window_id,
        ordinal=0,
        request_id="request-e2e",
        endpoint="daily",
        endpoint_class="daily",
        role="bars",
        trade_date=date(2026, 8, 20),
        symbol_or_index_shard="all",
        schema_contract_hash=_sha("schema"),
        unit_contract_hash=_sha("units"),
    )
    plan = ShadowLogicalRequestPlan(
        job_id=job_id, provider_id="tushare", window_id=window.window_id, requests=(request,)
    )
    job = ShadowJobStore(registry).enqueue(
        job_id=job_id,
        provider_id="tushare",
        window_id=window.window_id,
        trade_date="2026-08-20",
        universe_id="main-board-v1",
        canonical_manifest_generation=capability_snapshot.dataset_manifest_generation,
        canonical_manifest_sha256=capability_snapshot.dataset_manifest_sha256,
        version_vector_sha256=version_vector,
    )
    ShadowJobStore(registry).lease(job.job_id, owner="e2e-worker")

    page = {
        "page_identity": "request-e2e:page-000001",
        "rows": [{"ts_code": "600000.SH", "trade_date": "20260820"}],
    }
    completion = ShadowCompletion(
        job_id=job_id,
        provider_id="tushare",
        window_id=window.window_id,
        session_id=session_id,
        evidence_id="evidence-e2e",
        request_plan_sha256=plan.request_plan_sha256,
        requests=(
            ShadowRequestCompletion(
                ordinal=0,
                request_id=request.request_id,
                endpoint="daily",
                endpoint_class="daily",
                final_attempt_id="attempt-e2e",
                pages=(page,),
            ),
        ),
    )
    attempt = ShadowAttempt(
        attempt_id="attempt-e2e",
        job_id=job_id,
        provider_id="tushare",
        ordinal=0,
        outcome="success",
        rows=tuple(page["rows"]),
        pages=(page,),
        started_at="2026-08-20T08:00:00Z",
        completed_at="2026-08-20T08:00:01Z",
    )
    evidence_store = ShadowEvidenceStore(tmp_path / "shadow-evidence")
    evidence_store.publish(plan=plan, completion=completion, attempts=(attempt,))
    evidence_reader = ShadowEvidenceReader(tmp_path / "shadow-evidence")
    evidence = evidence_reader.read("evidence-e2e")
    evidence_result = ShadowEvidenceControlSink(registry).persist_evidence_ready(
        evidence=evidence,
        reader=evidence_reader,
        plan=plan,
        completion=completion,
        attempts=(attempt,),
        session_id=session_id,
        trade_date=date(2026, 8, 20),
        calendar_generation=snapshot.calendar_generation,
        calendar_sha256=snapshot.calendar_sha256,
        universe_sha256="a" * 64,
        version_vector_sha256=version_vector,
    )
    assert evidence_result.outcome == "evidence_ready"
    evidence_ready_job = ShadowJobStore(registry).get(job_id)
    assert evidence_ready_job is not None

    secondary = normalize_tushare(
        {
            "daily": [
                {
                    "ts_code": "000001.SH",
                    "trade_date": "20260820",
                    "open": 1,
                    "high": 1.1,
                    "low": 0.9,
                    "close": 1,
                    "pre_close": 1,
                    "vol": 1,
                    "amount": 0.1,
                }
            ],
            "universe": [{"ts_code": "000001.SH", "list_status": "L"}],
            "indexes": [],
            "adj_factor": [
                {
                    "ts_code": "000001.SH",
                    "trade_date": "20260820",
                    "anchor_date": "2026-08-20",
                    "adj_factor": 1,
                    "prev_adj_factor": 1,
                    "factor_semantics": "multiplicative_back_adjust",
                }
            ],
            "suspend_d": [],
            "stock_basic": [{"ts_code": "000001.SH", "list_status": "L"}],
            "units": {
                "vol": "lots",
                "amount": "thousand_cny",
                "factor_semantics": "multiplicative_back_adjust",
                "factor_anchor": "trade_date",
                "factor_direction": "back_adjust",
            },
            "job_id": job_id,
            "window_id": window.window_id,
            "session_id": session_id,
            "version_vector_sha256": version_vector,
        },
        trade_date=date(2026, 8, 20),
        universe_id="main-board-v1",
        contract=REVIEWED_TUSHARE_UNIT_CONTRACT,
    )
    mismatch_secondary = secondary.model_copy(
        update={
            "rows": (secondary.rows[0].model_copy(update={"close": 2}),),
            "normalized_sha256": "0" * 64,
        }
    )
    assert reconcile(canonical_side, mismatch_secondary).status == "material_mismatch"
    reconciliation = reconcile(canonical_side, secondary)
    assert reconciliation.status == "ready", reconciliation.model_dump()
    candidate_store = ShadowCandidateStore(tmp_path / "shadow-candidate")
    manifest = candidate_store.publish(
        secondary,
        evidence_reader=evidence_reader,
        evidence_id=evidence.evidence_id,
        reconciliation_report=reconciliation,
        canonical_comparison=canonical_capability,
        registry=registry,
    )
    candidate_reader = ShadowCandidateReader(
        tmp_path / "shadow-candidate", evidence_reader=evidence_reader, registry=registry
    )
    attached = candidate_store.attach(
        candidate_reader=candidate_reader,
        candidate_id=manifest.candidate_id,
        canonical_comparison=canonical_capability,
        registry=registry,
    )
    assert attached.manifest_sha256 == manifest.manifest_sha256

    terminal = ShadowTerminalWriter(ShadowBundlePublisher(tmp_path / "terminal-bundles"))
    identity = TerminalGraphIdentity(
        "tushare",
        job_id,
        window.window_id,
        session_id,
        evidence.evidence_id,
        manifest.candidate_id,
        "terminal-e2e",
    )
    attestation = terminal.write_success(
        registry,
        plan=plan,
        evidence_reader=evidence_reader,
        candidate_reader=candidate_reader,
        identity=identity,
        calendar_generation=snapshot.calendar_generation,
        calendar_sha256=snapshot.calendar_sha256,
        universe_sha256="a" * 64,
        version_vector_sha256=version_vector,
        expected_job_state_version=evidence_ready_job.state_version,
        expected_window_state_version=window.state_version,
        snapshot=snapshot,
    )
    assert attestation.terminal_outcome == "success"


def test_production_scheduler_run_once_uses_one_canonical_root_end_to_end(tmp_path):
    """The scanner and comparison must consume the same immutable canonical root."""
    published_root = tmp_path / "published"
    published_root.mkdir()
    _unused_dataset, canonical_candidate, canonical_evidence = write_canonical_fixture(
        published_root
    )
    dataset = tmp_path / "canonical"
    _write_canonical_dataset(
        dataset,
        trade_date=date(2026, 8, 20),
        symbol="sh.600000",
        open_=10,
        high=11,
        low=9,
        close=10.5,
        preclose=10,
        volume=1000,
        amount=20000,
        turnover_rate=1,
        pct_change=5,
    )
    candidate_values = json.loads(
        (canonical_candidate / "bundles/candidate-canonical-0001/candidate.json").read_text()
    )
    gate_values = json.loads(
        (canonical_candidate / "bundles/candidate-canonical-0001/gate.json").read_text()
    )
    manifest_path = dataset / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    manifest["files"][0].update(
        {
            "universe_id": candidate_values["universe_id"],
            "evidence_id": candidate_values["evidence_id"],
            "evidence_sha256": candidate_values["evidence_sha256"],
            "candidate_id": candidate_values["candidate_id"],
            "candidate_manifest_sha256": candidate_values["manifest_sha256"],
            "gate_report_sha256": gate_values["aggregate_sha256"],
            "adapter_version": candidate_values["adapter_version"],
            "source_schema_version": candidate_values["source_schema_version"],
        }
    )
    manifest_path.write_text(json.dumps(manifest, sort_keys=True, separators=(",", ":")) + "\n")

    canonical_comparison = PublishedCanonicalComparison.open(
        CanonicalCandidateReader(dataset, canonical_candidate, evidence_root=canonical_evidence)
    )
    assert isinstance(canonical_comparison, PublishedCanonicalComparison)
    capability = canonical_comparison.verify().snapshot
    assert capability is not None
    canonical_side = CanonicalSessionCandidateReader(dataset, trade_date=date(2026, 8, 20)).read()
    scanner = CanonicalOutcomeScanner(
        dataset,
        shadow_start_date=date(2026, 8, 20),
        shadow_end_date=date(2026, 8, 20),
        provider_id="tushare",
        window_id="tushare-window-1",
    )
    descriptor = scanner.scan()
    assert len(descriptor) == 1
    assert descriptor[0]["canonical_manifest_sha256"] == capability.dataset_manifest_sha256

    calendar_parent = tmp_path / "calendar"
    calendar_parent.mkdir()
    calendar = ConfirmedCalendarReader(
        _calendar_root(calendar_parent).resolve(),
        provider_id="tushare",
        window_id="tushare-window-1",
        universe_id="main-board-v1",
        universe_sha256="a" * 64,
    )
    snapshot = calendar.read(date(2026, 8, 20), date(2026, 8, 21))
    registry = ShadowRegistry.in_memory()
    registry.initialize()
    registry.put_provider(_record("tushare"))
    terms = _terms("tushare")
    registry.put_terms_evidence(terms)
    registry.attach_terms_to_provider("tushare", terms)
    canary = registry.transition("tushare", "canary", expected_state_version=1)
    registry.transition("tushare", "shadow", expected_state_version=canary.state_version)
    window = registry.ensure_window(
        "tushare",
        descriptor[0]["version_vector_sha256"],
        snapshot.calendar_generation,
        snapshot.calendar_sha256,
    )
    shadow_root = tmp_path / "shadow"
    jobs = ShadowJobStore(registry, shadow_root)
    publisher = ShadowBundlePublisher(shadow_root)
    evidence_root = tmp_path / "evidence"
    candidate_root = tmp_path / "candidate"
    evidence_store = ShadowEvidenceStore(evidence_root)
    evidence_reader = ShadowEvidenceReader(evidence_root)
    candidate_store = ShadowCandidateStore(candidate_root)
    captured = {}

    def worker(leased, *, deadline, max_requests, cancelled):
        del deadline, max_requests, cancelled
        request = ShadowLogicalRequest(
            job_id=leased.job_id,
            provider_id=leased.provider_id,
            window_id=leased.window_id,
            ordinal=0,
            request_id=f"request-{leased.job_id}",
            endpoint="daily",
            endpoint_class="daily",
            role="bars",
            trade_date=date.fromisoformat(leased.trade_date),
            symbol_or_index_shard="all",
            schema_contract_hash=_sha("schema"),
            unit_contract_hash=_sha("units"),
        )
        plan = ShadowLogicalRequestPlan(
            job_id=leased.job_id,
            provider_id=leased.provider_id,
            window_id=leased.window_id,
            requests=(request,),
        )
        session_id = f"session-{leased.job_id}"
        page = {
            "page_identity": f"{request.request_id}:page-000001",
            "rows": [{"ts_code": "600000.SH", "trade_date": "20260820"}],
        }
        completion = ShadowCompletion(
            job_id=leased.job_id,
            provider_id=leased.provider_id,
            window_id=leased.window_id,
            session_id=session_id,
            evidence_id=f"evidence-{leased.job_id}",
            request_plan_sha256=plan.request_plan_sha256,
            requests=(
                ShadowRequestCompletion(
                    ordinal=0,
                    request_id=request.request_id,
                    endpoint=request.endpoint,
                    endpoint_class=request.endpoint_class,
                    final_attempt_id=f"attempt-{leased.job_id}",
                    pages=(page,),
                ),
            ),
        )
        attempt = ShadowAttempt(
            attempt_id=f"attempt-{leased.job_id}",
            job_id=leased.job_id,
            provider_id=leased.provider_id,
            ordinal=0,
            outcome="success",
            rows=tuple(page["rows"]),
            pages=(page,),
            started_at="2026-08-20T08:00:00Z",
            completed_at="2026-08-20T08:00:01Z",
        )
        evidence_store.publish(plan=plan, completion=completion, attempts=(attempt,))
        evidence = evidence_reader.read(completion.evidence_id)
        ShadowEvidenceControlSink(registry).persist_evidence_ready(
            evidence=evidence,
            reader=evidence_reader,
            plan=plan,
            completion=completion,
            attempts=(attempt,),
            session_id=session_id,
            trade_date=date.fromisoformat(leased.trade_date),
            calendar_generation=snapshot.calendar_generation,
            calendar_sha256=snapshot.calendar_sha256,
            universe_sha256="a" * 64,
            version_vector_sha256=leased.version_vector_sha256,
        )
        secondary = normalize_tushare(
            {
                "daily": [
                    {
                        "ts_code": "600000.SH",
                        "trade_date": "20260820",
                        "open": 10,
                        "high": 11,
                        "low": 9,
                        "close": 10.5,
                        "pre_close": 10,
                        "vol": 10,
                        "amount": 20,
                    }
                ],
                "universe": [{"ts_code": "600000.SH", "list_status": "L"}],
                "indexes": [],
                "adj_factor": [
                    {
                        "ts_code": "600000.SH",
                        "trade_date": "20260820",
                        "anchor_date": "2026-08-20",
                        "adj_factor": 1,
                        "prev_adj_factor": 1,
                        "factor_semantics": "multiplicative_back_adjust",
                    }
                ],
                "suspend_d": [],
                "stock_basic": [{"ts_code": "600000.SH", "list_status": "L"}],
                "units": {
                    "vol": "lots",
                    "amount": "thousand_cny",
                    "factor_semantics": "multiplicative_back_adjust",
                    "factor_anchor": "trade_date",
                    "factor_direction": "back_adjust",
                },
                "job_id": leased.job_id,
                "window_id": leased.window_id,
                "session_id": session_id,
                "version_vector_sha256": leased.version_vector_sha256,
            },
            trade_date=date(2026, 8, 20),
            universe_id="main-board-v1",
            contract=REVIEWED_TUSHARE_UNIT_CONTRACT,
        )
        reconciliation = reconcile(canonical_side, secondary)
        assert reconciliation.status == "ready", reconciliation.model_dump()
        published = candidate_store.publish(
            secondary,
            evidence_reader=evidence_reader,
            evidence_id=evidence.evidence_id,
            reconciliation_report=reconciliation,
            canonical_comparison=canonical_comparison,
            registry=registry,
        )
        candidate_reader = ShadowCandidateReader(
            candidate_root, evidence_reader=evidence_reader, registry=registry
        )
        candidate_store.attach(
            candidate_reader=candidate_reader,
            candidate_id=published.candidate_id,
            canonical_comparison=canonical_comparison,
            registry=registry,
        )
        ready_job = jobs.get(leased.job_id)
        assert ready_job is not None
        identity = TerminalGraphIdentity(
            leased.provider_id,
            leased.job_id,
            leased.window_id,
            session_id,
            evidence.evidence_id,
            published.candidate_id,
            f"terminal-{session_id}",
        )
        captured["identity"] = identity
        terminal_context = {
            "plan": plan,
            "evidence_reader": evidence_reader,
            "candidate_reader": candidate_reader,
            "identity": identity,
            "calendar_generation": snapshot.calendar_generation,
            "calendar_sha256": snapshot.calendar_sha256,
            "universe_sha256": "a" * 64,
            "version_vector_sha256": leased.version_vector_sha256,
            "expected_job_state_version": ready_job.state_version,
            "expected_window_state_version": window.state_version,
            "snapshot": snapshot,
        }
        captured["context"] = terminal_context
        return {"status": "completed", "terminal_context": terminal_context}

    assert scanner.enqueue(jobs) == 1

    scheduler = ShadowScheduler(
        jobs,
        worker=worker,
        owner="e2e-scheduler",
        canonical_scanner=scanner,
        outcome_reporter=ShadowOutcomeReporter(publisher),
        terminal_writer=ShadowTerminalWriter(publisher),
    )
    outcome = scheduler.run_once()
    assert outcome.status == "completed"
    assert captured["identity"]
    success_dir = shadow_root / "bundles" / captured["identity"].session_report_id
    pending_dir = shadow_root / "bundles" / f"{captured['identity'].session_report_id}-pending"
    assert success_dir.is_dir() and pending_dir.is_dir()
    success_before = (success_dir / "report.json").read_bytes()
    db_before = registry._memory_connection.execute(
        "SELECT COUNT(*) FROM session_report WHERE session_report_id=?",
        (captured["identity"].session_report_id,),
    ).fetchone()

    def restore_file(path, payload):
        path.write_bytes(payload)
        path.chmod(0o600)
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)

    for name in ("OWNER", "report.json", "COMMIT"):
        original = (pending_dir / name).read_bytes()
        (pending_dir / name).unlink()
        try:
            with pytest.raises(ShadowTerminalUnavailable):
                ShadowTerminalWriter(publisher).recover_success(
                    registry, report_id=captured["identity"].session_report_id
                )
        finally:
            restore_file(pending_dir / name, original)

        original = (pending_dir / name).read_bytes()
        (pending_dir / name).unlink()
        (pending_dir / name).symlink_to(success_dir / "report.json")
        try:
            with pytest.raises(ShadowTerminalUnavailable):
                ShadowTerminalWriter(publisher).recover_success(
                    registry, report_id=captured["identity"].session_report_id
                )
        finally:
            (pending_dir / name).unlink()
            restore_file(pending_dir / name, original)
        assert (success_dir / "report.json").read_bytes() == success_before
        assert (
            registry._memory_connection.execute(
                "SELECT COUNT(*) FROM session_report WHERE session_report_id=?",
                (captured["identity"].session_report_id,),
            ).fetchone()
            == db_before
        )

    assert (
        ShadowTerminalWriter(publisher)
        .recover_success(registry, report_id=captured["identity"].session_report_id)
        .is_dir()
    )

    for name in ("OWNER", "report.json", "COMMIT"):
        original = (pending_dir / name).read_bytes()
        if name == "report.json":
            changed_payload = json.loads(original)
            changed_payload["outcome"] = "tampered"
            changed = (
                json.dumps(changed_payload, sort_keys=True, separators=(",", ":")) + "\n"
            ).encode()
        elif name == "OWNER":
            changed_payload = json.loads(original)
            changed_payload["publisher_id"] = "0" * 32
            changed = (
                json.dumps(changed_payload, sort_keys=True, separators=(",", ":")) + "\n"
            ).encode()
        else:
            changed = b"0" + original[1:]
        with (pending_dir / name).open("wb") as handle:
            handle.write(changed)
            handle.flush()
        try:
            try:
                ShadowTerminalWriter(publisher).recover_success(
                    registry, report_id=captured["identity"].session_report_id
                )
            except ShadowTerminalUnavailable:
                pass
            else:
                pytest.fail(f"same-inode tamper was accepted for {name}")
        finally:
            restore_file(pending_dir / name, original)

    connection = registry._memory_connection
    original_bundle_ref = connection.execute(
        "SELECT bundle_ref FROM shadow_evidence_ref WHERE evidence_id=?",
        (captured["identity"].evidence_id,),
    ).fetchone()[0]
    connection.execute(
        "UPDATE shadow_evidence_ref SET bundle_ref=? WHERE evidence_id=?",
        ("tampered/evidence", captured["identity"].evidence_id),
    )
    try:
        with pytest.raises(ShadowTerminalUnavailable):
            ShadowTerminalWriter(publisher).recover_success(
                registry, report_id=captured["identity"].session_report_id
            )
    finally:
        connection.execute(
            "UPDATE shadow_evidence_ref SET bundle_ref=? WHERE evidence_id=?",
            (original_bundle_ref, captured["identity"].evidence_id),
        )

    pending_report = pending_dir / "report.json"
    original_report = pending_report.read_bytes()
    for digest_name in (
        "request_plan_sha256",
        "completion_sha256",
        "attempt_ordinal_closure_sha256",
        "report_digest_sha256",
    ):
        payload = json.loads(original_report)
        payload["digests"][digest_name] = "0" * 64
        pending_report.write_text(json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n")
        try:
            with pytest.raises(ShadowTerminalUnavailable):
                ShadowTerminalWriter(publisher).recover_success(
                    registry, report_id=captured["identity"].session_report_id
                )
        finally:
            restore_file(pending_report, original_report)
    for section in ("evidence_ref", "candidate_ref"):
        original_payload = json.loads(original_report)
        for field, value in original_payload[section].items():
            payload = json.loads(original_report)
            if isinstance(value, dict):
                changed = {**value, "tampered": 1}
            elif isinstance(value, str):
                changed = "tampered"
            elif isinstance(value, int):
                changed = value + 1
            else:
                changed = ["tampered"]
            payload[section][field] = changed
            pending_report.write_text(
                json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n"
            )
            try:
                with pytest.raises(ShadowTerminalUnavailable):
                    ShadowTerminalWriter(publisher).recover_success(
                        registry, report_id=captured["identity"].session_report_id
                    )
            finally:
                restore_file(pending_report, original_report)
    payload = json.loads(original_report)
    payload["attestation"]["attestation_id"] = "evil-attestation"
    pending_report.write_text(json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n")
    try:
        with pytest.raises(ShadowTerminalUnavailable):
            ShadowTerminalWriter(publisher).recover_success(
                registry, report_id=captured["identity"].session_report_id
            )
    finally:
        restore_file(pending_report, original_report)
    assert (success_dir / "report.json").read_bytes() == success_before
    alternate = ShadowBundlePublisher(tmp_path / "alternate-shadow")
    with pytest.raises(ShadowTerminalUnavailable):
        ShadowTerminalWriter(publisher).write_success(
            registry, **captured["context"], bundle_publisher=alternate
        )
    assert not alternate.root.exists()


def test_production_scheduler_no_candidate_is_failure_without_success_marker(tmp_path):
    dataset = tmp_path / "canonical"
    _write_canonical_dataset(dataset)
    scanner = CanonicalOutcomeScanner(
        dataset,
        shadow_start_date=date(2026, 1, 2),
        shadow_end_date=date(2026, 1, 2),
        provider_id="tickflow",
        window_id="tickflow-window-1",
    )
    descriptor = scanner.scan()
    assert len(descriptor) == 1
    registry = ShadowRegistry.in_memory()
    registry.initialize()
    registry.put_provider(_record())
    terms = _terms()
    registry.put_terms_evidence(terms)
    registry.attach_terms_to_provider("tickflow", terms)
    canary = registry.transition("tickflow", "canary", expected_state_version=1)
    registry.transition("tickflow", "shadow", expected_state_version=canary.state_version)
    registry.ensure_window(
        "tickflow",
        descriptor[0]["version_vector_sha256"],
        "calendar-v1",
        _sha("calendar"),
    )
    shadow_root = tmp_path / "shadow"
    jobs = ShadowJobStore(registry, shadow_root)
    assert scanner.enqueue(jobs) == 1
    scheduler = ShadowScheduler(
        jobs,
        worker=lambda leased, **kwargs: {"status": "completed"},
        canonical_scanner=scanner,
        outcome_reporter=ShadowOutcomeReporter(ShadowBundlePublisher(shadow_root)),
        terminal_writer=ShadowTerminalWriter(ShadowBundlePublisher(shadow_root)),
    )
    outcome = scheduler.run_once()
    assert outcome.status == "failed"
    assert jobs.get(descriptor[0]["job_id"]).run_status == "failed"
    assert not any(path.name.endswith("-success") for path in (shadow_root / "bundles").iterdir())
