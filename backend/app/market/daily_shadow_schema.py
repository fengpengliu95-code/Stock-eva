"""Frozen SQLite v1 schema for the isolated TickFlow Free Daily shadow lane."""

# The SQL is deliberately kept as one byte-reviewable source.
# ruff: noqa: E501

from __future__ import annotations

import hashlib
import json
import sqlite3
import unicodedata
from typing import Any

MIGRATION_ID = "r2f3-daily-shadow-0001"
SCHEMA_VERSION = 1
MAX_CANONICAL_BYTES = 1_048_576

DAILY_SHADOW_DDL = r"""
CREATE TABLE schema_migration (
 migration_id TEXT PRIMARY KEY CHECK(migration_id='r2f3-daily-shadow-0001'),
 schema_version INTEGER NOT NULL UNIQUE CHECK(schema_version=1),
 checksum TEXT NOT NULL CHECK(length(checksum)=64),
 applied_at TEXT NOT NULL
);
CREATE TABLE daily_shadow_terms_evidence (
 terms_evidence_sha256 TEXT PRIMARY KEY CHECK(length(terms_evidence_sha256)=64),
 terms_evidence_id TEXT NOT NULL UNIQUE, review_id TEXT NOT NULL UNIQUE,
 contract_version TEXT NOT NULL, content_bytes_sha256 TEXT NOT NULL CHECK(length(content_bytes_sha256)=64),
 canonical_json BLOB NOT NULL CHECK(length(canonical_json)<=1048576), installed_at TEXT NOT NULL
);
CREATE TABLE daily_shadow_contract (
 descriptor_sha256 TEXT PRIMARY KEY CHECK(length(descriptor_sha256)=64),
 provider TEXT NOT NULL CHECK(provider='tickflow'),
 profile TEXT NOT NULL CHECK(profile='TICKFLOW_FREE_DAILY_BAR_OHLC_V1'),
 descriptor_json BLOB NOT NULL CHECK(length(descriptor_json)<=1048576),
 terms_evidence_sha256 TEXT NOT NULL, installed_at TEXT NOT NULL,
 UNIQUE(provider,profile,terms_evidence_sha256),
 FOREIGN KEY(terms_evidence_sha256) REFERENCES daily_shadow_terms_evidence(terms_evidence_sha256)
);
CREATE TABLE daily_shadow_epoch (
 epoch_id TEXT PRIMARY KEY, epoch_ordinal INTEGER NOT NULL UNIQUE CHECK(epoch_ordinal>=1),
 prior_epoch_id TEXT, epoch_state TEXT NOT NULL CHECK(epoch_state IN ('ACTIVE','RESET','SHADOW_QUALIFIED')),
 reset_reason TEXT, calendar_generation TEXT NOT NULL, calendar_sha256 TEXT NOT NULL CHECK(length(calendar_sha256)=64),
 universe_sha256 TEXT NOT NULL CHECK(length(universe_sha256)=64), version_vector_sha256 TEXT NOT NULL CHECK(length(version_vector_sha256)=64),
 terms_evidence_sha256 TEXT NOT NULL, expected_dates_json BLOB NOT NULL CHECK(length(expected_dates_json)<=1048576),
 expected_dates_sha256 TEXT NOT NULL CHECK(length(expected_dates_sha256)=64), created_at TEXT NOT NULL,
 CHECK((epoch_state='RESET' AND reset_reason IS NOT NULL) OR (epoch_state<>'RESET' AND reset_reason IS NULL)),
 FOREIGN KEY(prior_epoch_id) REFERENCES daily_shadow_epoch(epoch_id),
 FOREIGN KEY(terms_evidence_sha256) REFERENCES daily_shadow_terms_evidence(terms_evidence_sha256)
);
CREATE TABLE daily_shadow_window (
 provider TEXT NOT NULL CHECK(provider='tickflow'), profile TEXT NOT NULL CHECK(profile='TICKFLOW_FREE_DAILY_BAR_OHLC_V1'),
 epoch_id TEXT NOT NULL UNIQUE, window_state TEXT NOT NULL CHECK(window_state IN ('OBSERVING','RESET','SHADOW_QUALIFIED')),
 first_trade_date TEXT, last_trade_date TEXT, next_trade_date TEXT,
 consecutive_sessions INTEGER NOT NULL CHECK(consecutive_sessions BETWEEN 0 AND 20),
 required_sessions INTEGER NOT NULL CHECK(required_sessions=20), last_session_report_id TEXT,
 state_version INTEGER NOT NULL CHECK(state_version>=0),
 PRIMARY KEY(provider,profile), FOREIGN KEY(epoch_id) REFERENCES daily_shadow_epoch(epoch_id),
 FOREIGN KEY(last_session_report_id) REFERENCES daily_shadow_session_report(session_report_id) DEFERRABLE INITIALLY DEFERRED
);
CREATE TABLE daily_shadow_job (
 job_id TEXT PRIMARY KEY, epoch_id TEXT NOT NULL, session_id TEXT NOT NULL UNIQUE, trade_date TEXT NOT NULL,
 request_plan_sha256 TEXT NOT NULL CHECK(length(request_plan_sha256)=64), canonical_snapshot_sha256 TEXT NOT NULL CHECK(length(canonical_snapshot_sha256)=64),
 run_status TEXT NOT NULL CHECK(run_status IN ('LEASED','COMPLETED','FAILURE','MISMATCH','UNAVAILABLE','SKIPPED')),
 lease_owner TEXT, lease_expires_at TEXT, terminal_attestation_id TEXT, state_version INTEGER NOT NULL CHECK(state_version>=0),
 UNIQUE(epoch_id,trade_date), UNIQUE(job_id,epoch_id,session_id),
 CHECK((run_status='LEASED' AND lease_owner IS NOT NULL AND lease_expires_at IS NOT NULL AND terminal_attestation_id IS NULL) OR
       (run_status<>'LEASED' AND lease_owner IS NULL AND lease_expires_at IS NULL)),
 CHECK((run_status='COMPLETED' AND terminal_attestation_id IS NOT NULL) OR
       (run_status<>'COMPLETED' AND terminal_attestation_id IS NULL)),
 FOREIGN KEY(epoch_id) REFERENCES daily_shadow_epoch(epoch_id),
 FOREIGN KEY(terminal_attestation_id) REFERENCES daily_shadow_terminal_attestation(attestation_id) DEFERRABLE INITIALLY DEFERRED
);
CREATE TABLE daily_shadow_attempt_audit (
 audit_id TEXT PRIMARY KEY, epoch_id TEXT NOT NULL, job_id TEXT NOT NULL, session_id TEXT NOT NULL,
 ordinal INTEGER NOT NULL CHECK(ordinal>=0 AND ordinal<40), request_id TEXT NOT NULL,
 endpoint TEXT NOT NULL CHECK(endpoint='historical_daily_1d'), attempt INTEGER NOT NULL CHECK(attempt=1),
 outcome TEXT NOT NULL CHECK(outcome IN ('SUCCESS','FAILURE','SKIPPED_CIRCUIT_OPEN','HALF_OPEN_PROBE')),
 elapsed_ms INTEGER NOT NULL CHECK(elapsed_ms>=0), response_bytes INTEGER NOT NULL CHECK(response_bytes BETWEEN 0 AND 8388608),
 expected_rows INTEGER NOT NULL CHECK(expected_rows BETWEEN 0 AND 100), observed_rows INTEGER NOT NULL CHECK(observed_rows BETWEEN 0 AND 100),
 failure_class TEXT, audit_sha256 TEXT NOT NULL CHECK(length(audit_sha256)=64),
 UNIQUE(job_id,ordinal), FOREIGN KEY(job_id,epoch_id,session_id) REFERENCES daily_shadow_job(job_id,epoch_id,session_id)
);
CREATE TABLE daily_shadow_evidence_ref (
 evidence_id TEXT PRIMARY KEY, epoch_id TEXT NOT NULL, job_id TEXT NOT NULL, session_id TEXT NOT NULL,
 completion_sha256 TEXT NOT NULL CHECK(length(completion_sha256)=64), evidence_sha256 TEXT NOT NULL CHECK(length(evidence_sha256)=64),
 bundle_ref TEXT NOT NULL, bundle_sha256 TEXT NOT NULL CHECK(length(bundle_sha256)=64),
 UNIQUE(evidence_id,epoch_id,job_id,session_id), UNIQUE(evidence_id,epoch_id,job_id,session_id,evidence_sha256),
 FOREIGN KEY(job_id,epoch_id,session_id) REFERENCES daily_shadow_job(job_id,epoch_id,session_id)
);
CREATE TABLE daily_shadow_candidate_ref (
 candidate_id TEXT PRIMARY KEY, evidence_id TEXT NOT NULL UNIQUE, epoch_id TEXT NOT NULL, job_id TEXT NOT NULL, session_id TEXT NOT NULL,
 candidate_sha256 TEXT NOT NULL CHECK(length(candidate_sha256)=64), bundle_ref TEXT NOT NULL,
 bundle_sha256 TEXT NOT NULL CHECK(length(bundle_sha256)=64), quality_report_sha256 TEXT NOT NULL CHECK(length(quality_report_sha256)=64),
 reconciliation_report_sha256 TEXT NOT NULL CHECK(length(reconciliation_report_sha256)=64),
 UNIQUE(candidate_id,epoch_id,job_id,session_id), UNIQUE(candidate_id,epoch_id,job_id,session_id,candidate_sha256),
 FOREIGN KEY(evidence_id,epoch_id,job_id,session_id) REFERENCES daily_shadow_evidence_ref(evidence_id,epoch_id,job_id,session_id),
 FOREIGN KEY(job_id,epoch_id,session_id) REFERENCES daily_shadow_job(job_id,epoch_id,session_id)
);
CREATE TABLE daily_shadow_session_report (
 session_report_id TEXT PRIMARY KEY, epoch_id TEXT NOT NULL, job_id TEXT NOT NULL, session_id TEXT NOT NULL, trade_date TEXT NOT NULL,
 outcome TEXT NOT NULL CHECK(outcome IN ('SUCCESS','FAILURE','MISMATCH','UNAVAILABLE','SKIPPED_CIRCUIT_OPEN')),
 canonical_snapshot_sha256 TEXT NOT NULL CHECK(length(canonical_snapshot_sha256)=64), request_plan_sha256 TEXT NOT NULL CHECK(length(request_plan_sha256)=64),
 completion_sha256 TEXT, evidence_id TEXT, evidence_sha256 TEXT, candidate_id TEXT, candidate_sha256 TEXT,
 reconciliation_report_sha256 TEXT, terminal_attestation_id TEXT, failure_class TEXT,
 report_json BLOB NOT NULL CHECK(length(report_json)<=1048576), report_sha256 TEXT NOT NULL CHECK(length(report_sha256)=64), created_at TEXT NOT NULL,
 UNIQUE(epoch_id,trade_date), UNIQUE(session_report_id,epoch_id,job_id,session_id),
 CHECK((outcome='SUCCESS' AND completion_sha256 IS NOT NULL AND evidence_id IS NOT NULL AND evidence_sha256 IS NOT NULL AND candidate_id IS NOT NULL AND candidate_sha256 IS NOT NULL AND reconciliation_report_sha256 IS NOT NULL AND terminal_attestation_id IS NOT NULL AND failure_class IS NULL) OR
       (outcome<>'SUCCESS' AND completion_sha256 IS NULL AND evidence_id IS NULL AND evidence_sha256 IS NULL AND candidate_id IS NULL AND candidate_sha256 IS NULL AND reconciliation_report_sha256 IS NULL AND terminal_attestation_id IS NULL AND failure_class IS NOT NULL)),
 FOREIGN KEY(job_id,epoch_id,session_id) REFERENCES daily_shadow_job(job_id,epoch_id,session_id),
 FOREIGN KEY(evidence_id,epoch_id,job_id,session_id,evidence_sha256) REFERENCES daily_shadow_evidence_ref(evidence_id,epoch_id,job_id,session_id,evidence_sha256),
 FOREIGN KEY(candidate_id,epoch_id,job_id,session_id,candidate_sha256) REFERENCES daily_shadow_candidate_ref(candidate_id,epoch_id,job_id,session_id,candidate_sha256),
 FOREIGN KEY(terminal_attestation_id) REFERENCES daily_shadow_terminal_attestation(attestation_id) DEFERRABLE INITIALLY DEFERRED
);
CREATE TABLE daily_shadow_terminal_attestation (
 attestation_id TEXT PRIMARY KEY, epoch_id TEXT NOT NULL, job_id TEXT NOT NULL, session_id TEXT NOT NULL,
 session_report_id TEXT NOT NULL UNIQUE, evidence_id TEXT NOT NULL UNIQUE, candidate_id TEXT NOT NULL UNIQUE,
 request_plan_json BLOB NOT NULL CHECK(length(request_plan_json)<=1048576), request_plan_sha256 TEXT NOT NULL CHECK(length(request_plan_sha256)=64),
 completion_json BLOB NOT NULL CHECK(length(completion_json)<=1048576), completion_sha256 TEXT NOT NULL CHECK(length(completion_sha256)=64),
 attempt_closure_json BLOB NOT NULL CHECK(length(attempt_closure_json)<=1048576), attempt_closure_sha256 TEXT NOT NULL CHECK(length(attempt_closure_sha256)=64),
 report_graph_json BLOB NOT NULL CHECK(length(report_graph_json)<=1048576), report_graph_sha256 TEXT NOT NULL CHECK(length(report_graph_sha256)=64),
 attestation_sha256 TEXT NOT NULL UNIQUE CHECK(length(attestation_sha256)=64), immutable_version INTEGER NOT NULL CHECK(immutable_version=1),
 UNIQUE(attestation_id,epoch_id,job_id,session_id),
 FOREIGN KEY(session_report_id,epoch_id,job_id,session_id) REFERENCES daily_shadow_session_report(session_report_id,epoch_id,job_id,session_id),
 FOREIGN KEY(evidence_id,epoch_id,job_id,session_id) REFERENCES daily_shadow_evidence_ref(evidence_id,epoch_id,job_id,session_id),
 FOREIGN KEY(candidate_id,epoch_id,job_id,session_id) REFERENCES daily_shadow_candidate_ref(candidate_id,epoch_id,job_id,session_id)
);
CREATE TABLE daily_shadow_circuit (
 endpoint TEXT PRIMARY KEY CHECK(endpoint='historical_daily_1d'), state TEXT NOT NULL CHECK(state IN ('CLOSED','OPEN','HALF_OPEN')),
 consecutive_failures INTEGER NOT NULL CHECK(consecutive_failures>=0), opened_at TEXT, cooldown_until TEXT,
 probe_lease_id TEXT UNIQUE, probe_owner TEXT, probe_expires_at TEXT, state_version INTEGER NOT NULL CHECK(state_version>=0),
 CHECK((state='CLOSED' AND opened_at IS NULL AND cooldown_until IS NULL AND probe_lease_id IS NULL AND probe_owner IS NULL AND probe_expires_at IS NULL) OR
       (state='OPEN' AND opened_at IS NOT NULL AND cooldown_until IS NOT NULL AND probe_lease_id IS NULL AND probe_owner IS NULL AND probe_expires_at IS NULL) OR
       (state='HALF_OPEN' AND opened_at IS NOT NULL AND cooldown_until IS NOT NULL AND probe_lease_id IS NOT NULL AND probe_owner IS NOT NULL AND probe_expires_at IS NOT NULL))
);
CREATE TABLE daily_shadow_circuit_event (
 event_id TEXT PRIMARY KEY, endpoint TEXT NOT NULL, event_type TEXT NOT NULL,
 observed_at TEXT NOT NULL, state_before TEXT NOT NULL, state_after TEXT NOT NULL,
 failure_class TEXT, event_sha256 TEXT NOT NULL CHECK(length(event_sha256)=64),
 FOREIGN KEY(endpoint) REFERENCES daily_shadow_circuit(endpoint)
);
CREATE TRIGGER schema_migration_immutable_update BEFORE UPDATE ON schema_migration BEGIN SELECT RAISE(ABORT,'daily_schema_migration_append_only'); END;
CREATE TRIGGER schema_migration_immutable_delete BEFORE DELETE ON schema_migration BEGIN SELECT RAISE(ABORT,'daily_schema_migration_append_only'); END;
CREATE TRIGGER daily_terms_immutable_update BEFORE UPDATE ON daily_shadow_terms_evidence BEGIN SELECT RAISE(ABORT,'daily_terms_append_only'); END;
CREATE TRIGGER daily_terms_immutable_delete BEFORE DELETE ON daily_shadow_terms_evidence BEGIN SELECT RAISE(ABORT,'daily_terms_append_only'); END;
CREATE TRIGGER daily_contract_immutable_update BEFORE UPDATE ON daily_shadow_contract BEGIN SELECT RAISE(ABORT,'daily_contract_append_only'); END;
CREATE TRIGGER daily_contract_immutable_delete BEFORE DELETE ON daily_shadow_contract BEGIN SELECT RAISE(ABORT,'daily_contract_append_only'); END;
CREATE TRIGGER daily_attempt_immutable_update BEFORE UPDATE ON daily_shadow_attempt_audit BEGIN SELECT RAISE(ABORT,'daily_attempt_append_only'); END;
CREATE TRIGGER daily_attempt_immutable_delete BEFORE DELETE ON daily_shadow_attempt_audit BEGIN SELECT RAISE(ABORT,'daily_attempt_append_only'); END;
CREATE TRIGGER daily_evidence_immutable_update BEFORE UPDATE ON daily_shadow_evidence_ref BEGIN SELECT RAISE(ABORT,'daily_evidence_append_only'); END;
CREATE TRIGGER daily_evidence_immutable_delete BEFORE DELETE ON daily_shadow_evidence_ref BEGIN SELECT RAISE(ABORT,'daily_evidence_append_only'); END;
CREATE TRIGGER daily_candidate_immutable_update BEFORE UPDATE ON daily_shadow_candidate_ref BEGIN SELECT RAISE(ABORT,'daily_candidate_append_only'); END;
CREATE TRIGGER daily_candidate_immutable_delete BEFORE DELETE ON daily_shadow_candidate_ref BEGIN SELECT RAISE(ABORT,'daily_candidate_append_only'); END;
CREATE TRIGGER daily_session_immutable_update BEFORE UPDATE ON daily_shadow_session_report BEGIN SELECT RAISE(ABORT,'daily_session_append_only'); END;
CREATE TRIGGER daily_session_immutable_delete BEFORE DELETE ON daily_shadow_session_report BEGIN SELECT RAISE(ABORT,'daily_session_append_only'); END;
CREATE TRIGGER daily_attestation_immutable_update BEFORE UPDATE ON daily_shadow_terminal_attestation BEGIN SELECT RAISE(ABORT,'daily_attestation_append_only'); END;
CREATE TRIGGER daily_attestation_immutable_delete BEFORE DELETE ON daily_shadow_terminal_attestation BEGIN SELECT RAISE(ABORT,'daily_attestation_append_only'); END;
CREATE TRIGGER daily_circuit_event_immutable_update BEFORE UPDATE ON daily_shadow_circuit_event BEGIN SELECT RAISE(ABORT,'daily_circuit_event_append_only'); END;
CREATE TRIGGER daily_circuit_event_immutable_delete BEFORE DELETE ON daily_shadow_circuit_event BEGIN SELECT RAISE(ABORT,'daily_circuit_event_append_only'); END;
CREATE TRIGGER daily_terminal_graph_gate BEFORE INSERT ON daily_shadow_terminal_attestation
BEGIN
 SELECT CASE WHEN daily_validate_terminal_graph(
  NEW.request_plan_json,NEW.request_plan_sha256,NEW.completion_json,NEW.completion_sha256,
  NEW.attempt_closure_json,NEW.attempt_closure_sha256,NEW.report_graph_json,NEW.report_graph_sha256
 ) <> 1 THEN RAISE(ABORT,'daily_terminal_digest_mismatch') END;
 SELECT CASE WHEN NOT EXISTS (
  SELECT 1 FROM daily_shadow_session_report s
  JOIN daily_shadow_evidence_ref e ON e.evidence_id=s.evidence_id AND e.epoch_id=s.epoch_id AND e.job_id=s.job_id AND e.session_id=s.session_id
  JOIN daily_shadow_candidate_ref c ON c.candidate_id=s.candidate_id AND c.epoch_id=s.epoch_id AND c.job_id=s.job_id AND c.session_id=s.session_id
  WHERE s.session_report_id=NEW.session_report_id AND s.epoch_id=NEW.epoch_id AND s.job_id=NEW.job_id AND s.session_id=NEW.session_id
   AND s.outcome='SUCCESS' AND s.terminal_attestation_id=NEW.attestation_id
   AND e.evidence_id=NEW.evidence_id AND e.evidence_sha256=s.evidence_sha256
   AND c.candidate_id=NEW.candidate_id AND c.candidate_sha256=s.candidate_sha256
 ) THEN RAISE(ABORT,'daily_terminal_graph_unclosed') END;
END;
"""

DAILY_SHADOW_SCHEMA_SHA256 = hashlib.sha256(DAILY_SHADOW_DDL.encode("utf-8")).hexdigest()


def _schema_object_sha256(connection: sqlite3.Connection) -> str:
    rows = [
        tuple(row)
        for row in connection.execute(
            "SELECT type,name,tbl_name,sql FROM sqlite_master "
            "WHERE name NOT LIKE 'sqlite_%' AND sql IS NOT NULL "
            "ORDER BY type,name"
        )
    ]
    return hashlib.sha256(
        json.dumps(rows, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


_schema_probe = sqlite3.connect(":memory:")
_schema_probe.executescript(DAILY_SHADOW_DDL)
DAILY_SHADOW_SCHEMA_OBJECT_SHA256 = _schema_object_sha256(_schema_probe)
_schema_probe.close()


class DailyShadowSchemaError(RuntimeError):
    pass


def canonical_json_bytes(value: Any) -> bytes:
    text = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return (unicodedata.normalize("NFC", text) + "\n").encode("utf-8")


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def daily_validate_terminal_graph(*values: Any) -> int:
    if len(values) != 8:
        return 0
    for payload, expected in zip(values[::2], values[1::2], strict=True):
        if not isinstance(payload, bytes | str) or not isinstance(expected, str):
            return 0
        raw = payload.encode("utf-8") if isinstance(payload, str) else payload
        if len(raw) > MAX_CANONICAL_BYTES or sha256_bytes(raw) != expected:
            return 0
        try:
            decoded = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            return 0
        if canonical_json_bytes(decoded) != raw:
            return 0
    return 1


def configure_daily_connection(connection: sqlite3.Connection) -> None:
    connection.create_function(
        "daily_validate_terminal_graph", 8, daily_validate_terminal_graph, deterministic=True
    )
    connection.execute("PRAGMA foreign_keys=ON")
    connection.execute("PRAGMA journal_mode=DELETE")
    connection.execute("PRAGMA synchronous=FULL")
    connection.execute("PRAGMA busy_timeout=250")


def validate_daily_shadow_schema(connection: sqlite3.Connection) -> None:
    expected_tables = {
        "schema_migration",
        "daily_shadow_terms_evidence",
        "daily_shadow_contract",
        "daily_shadow_epoch",
        "daily_shadow_window",
        "daily_shadow_job",
        "daily_shadow_attempt_audit",
        "daily_shadow_evidence_ref",
        "daily_shadow_candidate_ref",
        "daily_shadow_session_report",
        "daily_shadow_terminal_attestation",
        "daily_shadow_circuit",
        "daily_shadow_circuit_event",
    }
    try:
        rows = connection.execute(
            "SELECT migration_id,schema_version,checksum FROM schema_migration"
        ).fetchall()
        tables = {
            item[0]
            for item in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
            )
        }
        foreign_keys = connection.execute("PRAGMA foreign_key_check").fetchall()
        integrity = connection.execute("PRAGMA integrity_check").fetchone()
        object_sha256 = _schema_object_sha256(connection)
    except sqlite3.DatabaseError as exc:
        raise DailyShadowSchemaError("daily shadow schema unavailable") from exc
    if (
        [tuple(row) for row in rows] != [(MIGRATION_ID, SCHEMA_VERSION, DAILY_SHADOW_SCHEMA_SHA256)]
        or tables != expected_tables
        or foreign_keys
        or tuple(integrity or ()) != ("ok",)
        or object_sha256 != DAILY_SHADOW_SCHEMA_OBJECT_SHA256
    ):
        raise DailyShadowSchemaError("daily shadow schema unavailable")


def initialize_daily_shadow_schema(connection: sqlite3.Connection, *, applied_at: str) -> None:
    configure_daily_connection(connection)
    tables = {
        row[0]
        for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
        )
    }
    if not tables:
        try:
            connection.executescript(DAILY_SHADOW_DDL)
            connection.execute(
                "INSERT INTO schema_migration VALUES (?,?,?,?)",
                (MIGRATION_ID, SCHEMA_VERSION, DAILY_SHADOW_SCHEMA_SHA256, applied_at),
            )
            connection.execute(
                "INSERT INTO daily_shadow_circuit VALUES "
                "('historical_daily_1d','CLOSED',0,NULL,NULL,NULL,NULL,NULL,0)"
            )
            connection.commit()
        except sqlite3.DatabaseError as exc:
            connection.rollback()
            raise DailyShadowSchemaError("daily shadow schema bootstrap unavailable") from exc
    validate_daily_shadow_schema(connection)
