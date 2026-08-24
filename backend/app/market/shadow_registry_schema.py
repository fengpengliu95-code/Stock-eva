"""Frozen SQLite schema and bootstrap primitives for the shadow registry."""

# The frozen SQL source is intentionally kept byte-readable for independent review.
# ruff: noqa: E501

from __future__ import annotations

import hashlib
import json
import sqlite3
import unicodedata
from typing import Any

MIGRATION_IDS = ("r2f3-registry-0001", "r2f3-registry-0002")
MIGRATION_ID = MIGRATION_IDS[-1]
SCHEMA_VERSION = 2
MAX_CANONICAL_BYTES = 1_048_576

REGISTRY_DDL = r"""
CREATE TABLE schema_migration (
 migration_id TEXT PRIMARY KEY, schema_version INTEGER NOT NULL UNIQUE CHECK(schema_version > 0), applied_at TEXT NOT NULL
);
CREATE TABLE terms_evidence (
 terms_evidence_id TEXT PRIMARY KEY, provider_id TEXT NOT NULL CHECK(provider_id IN ('tickflow','tushare')),
 official_url_allowlist_json TEXT NOT NULL, content_object_relpath TEXT NOT NULL,
 content_bytes_sha256 TEXT NOT NULL CHECK(length(content_bytes_sha256)=64), contract_version TEXT NOT NULL,
 as_of_date TEXT NOT NULL, reviewer TEXT NOT NULL, review_id TEXT NOT NULL,
 approved_intended_use TEXT NOT NULL, approved_retention TEXT NOT NULL, approved_credential_mode TEXT NOT NULL,
 approved_quota_decision TEXT NOT NULL, manifest_sha256 TEXT NOT NULL CHECK(length(manifest_sha256)=64),
 UNIQUE(provider_id,manifest_sha256,review_id), UNIQUE(provider_id,manifest_sha256)
);
CREATE TABLE provider_record (
 provider_id TEXT PRIMARY KEY CHECK(provider_id IN ('tickflow','tushare')),
 admission_state TEXT NOT NULL CHECK(admission_state IN ('discovered','canary','shadow','qualified','quarantined')),
 adapter_hash TEXT NOT NULL CHECK(length(adapter_hash)=64), endpoint_contract_hash TEXT NOT NULL CHECK(length(endpoint_contract_hash)=64),
 source_schema_hash TEXT NOT NULL CHECK(length(source_schema_hash)=64), normalizer_hash TEXT NOT NULL CHECK(length(normalizer_hash)=64),
 reconciliation_policy_hash TEXT NOT NULL CHECK(length(reconciliation_policy_hash)=64), terms_evidence_hash TEXT,
 terms_review_id TEXT, credential_env_name TEXT NOT NULL CHECK((provider_id='tickflow' AND credential_env_name='STOCK_EVA_TICKFLOW_TOKEN') OR (provider_id='tushare' AND credential_env_name='STOCK_EVA_TUSHARE_TOKEN')),
 intended_use TEXT NOT NULL, retention_decision TEXT NOT NULL, quota_contract TEXT NOT NULL,
 required_fields_json TEXT NOT NULL, unit_contract_json TEXT NOT NULL, state_version INTEGER NOT NULL CHECK(state_version>=0), quarantine_reason TEXT,
 CHECK(admission_state='discovered' OR (terms_evidence_hash IS NOT NULL AND terms_review_id IS NOT NULL)),
 FOREIGN KEY(provider_id,terms_evidence_hash,terms_review_id) REFERENCES terms_evidence(provider_id,manifest_sha256,review_id)
);
CREATE TABLE qualification_window (
 provider_id TEXT NOT NULL, window_id TEXT NOT NULL, window_start TEXT, window_end TEXT,
 consecutive_sessions INTEGER NOT NULL CHECK(consecutive_sessions>=0), version_vector_sha256 TEXT NOT NULL CHECK(length(version_vector_sha256)=64),
 calendar_generation TEXT NOT NULL, calendar_sha256 TEXT NOT NULL CHECK(length(calendar_sha256)=64),
 window_state TEXT NOT NULL CHECK(window_state IN ('observing','qualified','reset')), last_session_report_id TEXT,
 qualification_evidence_sha256 TEXT CHECK(qualification_evidence_sha256 IS NULL OR length(qualification_evidence_sha256)=64),
 qualification_candidate_sha256 TEXT CHECK(qualification_candidate_sha256 IS NULL OR length(qualification_candidate_sha256)=64), terminal_attestation_id TEXT,
 state_version INTEGER NOT NULL CHECK(state_version>=0),
 CHECK(window_state<>'qualified' OR (last_session_report_id IS NOT NULL AND qualification_evidence_sha256 IS NOT NULL AND qualification_candidate_sha256 IS NOT NULL AND terminal_attestation_id IS NOT NULL)),
 PRIMARY KEY(provider_id,window_id), UNIQUE(provider_id), FOREIGN KEY(provider_id) REFERENCES provider_record(provider_id),
 FOREIGN KEY(terminal_attestation_id,provider_id,window_id) REFERENCES shadow_terminal_attestation(attestation_id,provider_id,window_id)
);
CREATE TABLE shadow_job (
 job_id TEXT PRIMARY KEY, provider_id TEXT NOT NULL, window_id TEXT NOT NULL, trade_date TEXT NOT NULL, universe_id TEXT NOT NULL,
 canonical_manifest_generation TEXT NOT NULL, canonical_manifest_sha256 TEXT NOT NULL CHECK(length(canonical_manifest_sha256)=64), version_vector_sha256 TEXT NOT NULL CHECK(length(version_vector_sha256)=64),
 successful_evidence_sha256 TEXT CHECK(successful_evidence_sha256 IS NULL OR length(successful_evidence_sha256)=64), successful_candidate_sha256 TEXT CHECK(successful_candidate_sha256 IS NULL OR length(successful_candidate_sha256)=64), completion_sha256 TEXT CHECK(completion_sha256 IS NULL OR length(completion_sha256)=64), terminal_attestation_id TEXT,
 run_status TEXT NOT NULL CHECK(run_status IN ('pending','leased','pending_normalization','completed','failed','cancelled','unavailable')), lease_owner TEXT, lease_expires_at TEXT,
 attempt_count INTEGER NOT NULL CHECK(attempt_count>=0), state_version INTEGER NOT NULL CHECK(state_version>=0),
 UNIQUE(provider_id,window_id,trade_date,universe_id), UNIQUE(job_id,provider_id,window_id),
 CHECK(run_status<>'completed' OR (successful_evidence_sha256 IS NOT NULL AND successful_candidate_sha256 IS NOT NULL AND completion_sha256 IS NOT NULL AND terminal_attestation_id IS NOT NULL)),
 FOREIGN KEY(provider_id,window_id) REFERENCES qualification_window(provider_id,window_id),
 FOREIGN KEY(terminal_attestation_id,provider_id,job_id,window_id) REFERENCES shadow_terminal_attestation(attestation_id,provider_id,job_id,window_id)
);
CREATE TABLE shadow_evidence_ref (
 evidence_id TEXT PRIMARY KEY, job_id TEXT NOT NULL, provider_id TEXT NOT NULL, window_id TEXT NOT NULL, session_id TEXT NOT NULL,
 completion_sha256 TEXT NOT NULL CHECK(length(completion_sha256)=64), evidence_sha256 TEXT NOT NULL CHECK(length(evidence_sha256)=64), bundle_ref TEXT NOT NULL, bundle_sha256 TEXT NOT NULL CHECK(length(bundle_sha256)=64), attached_session_report_id TEXT,
 UNIQUE(evidence_id,provider_id,job_id,window_id,session_id), UNIQUE(evidence_id,provider_id,job_id,window_id,session_id,evidence_sha256),
 FOREIGN KEY(job_id,provider_id,window_id) REFERENCES shadow_job(job_id,provider_id,window_id), FOREIGN KEY(provider_id,window_id) REFERENCES qualification_window(provider_id,window_id),
 FOREIGN KEY(attached_session_report_id,provider_id,job_id,window_id,session_id) REFERENCES session_report(session_report_id,provider_id,job_id,window_id,session_id)
);
CREATE TABLE shadow_candidate_ref (
 candidate_id TEXT PRIMARY KEY, evidence_id TEXT NOT NULL UNIQUE, job_id TEXT NOT NULL, provider_id TEXT NOT NULL, window_id TEXT NOT NULL, session_id TEXT NOT NULL, candidate_ref TEXT NOT NULL, candidate_sha256 TEXT NOT NULL CHECK(length(candidate_sha256)=64), quality_report_ref TEXT NOT NULL, quality_report_sha256 TEXT NOT NULL CHECK(length(quality_report_sha256)=64),
 FOREIGN KEY(evidence_id,provider_id,job_id,window_id,session_id) REFERENCES shadow_evidence_ref(evidence_id,provider_id,job_id,window_id,session_id), FOREIGN KEY(job_id,provider_id,window_id) REFERENCES shadow_job(job_id,provider_id,window_id), UNIQUE(candidate_id,provider_id,job_id,window_id,session_id), UNIQUE(candidate_id,provider_id,job_id,window_id,session_id,candidate_sha256)
);
CREATE TABLE session_report (
 session_report_id TEXT PRIMARY KEY, provider_id TEXT NOT NULL, job_id TEXT NOT NULL, window_id TEXT NOT NULL, session_id TEXT NOT NULL, successful_attempt_id TEXT, evidence_id TEXT, candidate_id TEXT, terminal_attestation_id TEXT,
 report_version INTEGER NOT NULL CHECK(report_version>=1), trade_date TEXT NOT NULL, outcome TEXT NOT NULL CHECK(outcome IN ('evidence_ready','success','failure','skip','unavailable','mismatch')),
 calendar_generation TEXT NOT NULL, calendar_sha256 TEXT NOT NULL CHECK(length(calendar_sha256)=64), universe_sha256 TEXT NOT NULL CHECK(length(universe_sha256)=64), version_vector_sha256 TEXT NOT NULL CHECK(length(version_vector_sha256)=64), evidence_sha256 TEXT CHECK(evidence_sha256 IS NULL OR length(evidence_sha256)=64), candidate_sha256 TEXT CHECK(candidate_sha256 IS NULL OR length(candidate_sha256)=64), report_ref TEXT NOT NULL, report_sha256 TEXT NOT NULL CHECK(length(report_sha256)=64), state_version INTEGER NOT NULL CHECK(state_version>=0),
 CHECK((outcome IN ('failure','skip','unavailable','mismatch') AND successful_attempt_id IS NULL AND evidence_id IS NULL AND candidate_id IS NULL AND terminal_attestation_id IS NULL AND evidence_sha256 IS NULL AND candidate_sha256 IS NULL) OR (outcome='evidence_ready' AND successful_attempt_id IS NOT NULL AND evidence_id IS NOT NULL AND candidate_id IS NULL AND terminal_attestation_id IS NULL AND evidence_sha256 IS NOT NULL AND candidate_sha256 IS NULL) OR (outcome='success' AND successful_attempt_id IS NOT NULL AND evidence_id IS NOT NULL AND candidate_id IS NOT NULL AND terminal_attestation_id IS NOT NULL AND evidence_sha256 IS NOT NULL AND candidate_sha256 IS NOT NULL)),
 CHECK(outcome<>'evidence_ready' OR report_version=1), CHECK(outcome<>'success' OR report_version>=2), UNIQUE(provider_id,window_id,trade_date,report_version), UNIQUE(session_report_id,provider_id,job_id,window_id,session_id), UNIQUE(session_report_id,provider_id,job_id,window_id,session_id,report_version), FOREIGN KEY(provider_id,window_id) REFERENCES qualification_window(provider_id,window_id), FOREIGN KEY(job_id,provider_id,window_id) REFERENCES shadow_job(job_id,provider_id,window_id), FOREIGN KEY(successful_attempt_id,provider_id,job_id,window_id,session_id) REFERENCES shadow_attempt_report(attempt_id,provider_id,job_id,window_id,session_id), FOREIGN KEY(evidence_id,provider_id,job_id,window_id,session_id,evidence_sha256) REFERENCES shadow_evidence_ref(evidence_id,provider_id,job_id,window_id,session_id,evidence_sha256), FOREIGN KEY(candidate_id,provider_id,job_id,window_id,session_id) REFERENCES shadow_candidate_ref(candidate_id,provider_id,job_id,window_id,session_id), FOREIGN KEY(candidate_id,provider_id,job_id,window_id,session_id,candidate_sha256) REFERENCES shadow_candidate_ref(candidate_id,provider_id,job_id,window_id,session_id,candidate_sha256), FOREIGN KEY(terminal_attestation_id,provider_id,job_id,window_id,session_id) REFERENCES shadow_terminal_attestation(attestation_id,provider_id,job_id,window_id,session_id)
);
CREATE TABLE shadow_attempt_report (
 attempt_id TEXT PRIMARY KEY, report_id TEXT NOT NULL UNIQUE, job_id TEXT NOT NULL, provider_id TEXT NOT NULL, window_id TEXT NOT NULL, session_id TEXT NOT NULL, request_id TEXT NOT NULL, endpoint TEXT NOT NULL, endpoint_class TEXT NOT NULL, logical_request_ordinal INTEGER NOT NULL CHECK(logical_request_ordinal>=0), attempt_number INTEGER NOT NULL CHECK(attempt_number>=0), version_vector_sha256 TEXT NOT NULL CHECK(length(version_vector_sha256)=64), outcome TEXT NOT NULL CHECK(outcome IN ('evidence_ready','success','failure','skip','unavailable','mismatch')), started_at TEXT NOT NULL, completed_at TEXT NOT NULL, coverage_expected INTEGER NOT NULL CHECK(coverage_expected>=0), coverage_observed INTEGER NOT NULL CHECK(coverage_observed>=0), request_count INTEGER NOT NULL CHECK(request_count>=0), retry_count INTEGER NOT NULL CHECK(retry_count>=0), rate_limit_count INTEGER NOT NULL CHECK(rate_limit_count>=0), failure_class TEXT NOT NULL, page_identities_json TEXT NOT NULL, page_count INTEGER NOT NULL CHECK(page_count>=0), row_count INTEGER NOT NULL CHECK(row_count>=0), terminal_marker INTEGER NOT NULL CHECK(terminal_marker IN (0,1)), durable_report_ref TEXT NOT NULL, report_sha256 TEXT NOT NULL CHECK(length(report_sha256)=64), evidence_refs_json TEXT NOT NULL, evidence_id TEXT, evidence_sha256 TEXT CHECK(evidence_sha256 IS NULL OR length(evidence_sha256)=64), candidate_sha256 TEXT CHECK(candidate_sha256 IS NULL OR length(candidate_sha256)=64), terminal_session_report_id TEXT, state_version INTEGER NOT NULL CHECK(state_version>=0),
 CHECK((outcome IN ('failure','skip','unavailable','mismatch') AND page_identities_json='[]' AND page_count=0 AND row_count=0 AND evidence_refs_json='[]' AND evidence_id IS NULL AND evidence_sha256 IS NULL AND candidate_sha256 IS NULL AND terminal_marker=0) OR (outcome IN ('evidence_ready','success') AND terminal_marker=1 AND page_identities_json<>'[]' AND evidence_refs_json<>'[]')),
 CHECK(outcome<>'evidence_ready' OR (terminal_marker=1 AND evidence_id IS NOT NULL AND evidence_sha256 IS NOT NULL AND candidate_sha256 IS NULL AND terminal_session_report_id IS NULL)), CHECK(outcome<>'success' OR (terminal_marker=1 AND evidence_id IS NOT NULL AND evidence_sha256 IS NOT NULL AND candidate_sha256 IS NOT NULL AND terminal_session_report_id IS NOT NULL)), UNIQUE(job_id,provider_id,window_id,session_id,logical_request_ordinal,attempt_number), UNIQUE(attempt_id,provider_id,job_id,window_id,session_id), UNIQUE(attempt_id,provider_id,job_id,window_id,session_id,logical_request_ordinal), UNIQUE(terminal_session_report_id,provider_id,job_id,window_id,session_id,logical_request_ordinal), FOREIGN KEY(job_id,provider_id,window_id) REFERENCES shadow_job(job_id,provider_id,window_id), FOREIGN KEY(provider_id,window_id) REFERENCES qualification_window(provider_id,window_id), FOREIGN KEY(evidence_id,provider_id,job_id,window_id,session_id) REFERENCES shadow_evidence_ref(evidence_id,provider_id,job_id,window_id,session_id), FOREIGN KEY(terminal_session_report_id,provider_id,job_id,window_id,session_id) REFERENCES session_report(session_report_id,provider_id,job_id,window_id,session_id)
);
CREATE TABLE shadow_evidence_attempt_ref (
 evidence_id TEXT NOT NULL, provider_id TEXT NOT NULL, job_id TEXT NOT NULL, window_id TEXT NOT NULL, session_id TEXT NOT NULL, logical_request_ordinal INTEGER NOT NULL CHECK(logical_request_ordinal>=0), attempt_id TEXT NOT NULL, endpoint TEXT NOT NULL, request_id TEXT NOT NULL, page_refs_json TEXT NOT NULL, page_count INTEGER NOT NULL CHECK(page_count>=0), row_count INTEGER NOT NULL CHECK(row_count>=0), PRIMARY KEY(evidence_id,logical_request_ordinal), UNIQUE(attempt_id,provider_id,job_id,window_id,session_id,logical_request_ordinal), FOREIGN KEY(evidence_id,provider_id,job_id,window_id,session_id) REFERENCES shadow_evidence_ref(evidence_id,provider_id,job_id,window_id,session_id), FOREIGN KEY(attempt_id,provider_id,job_id,window_id,session_id,logical_request_ordinal) REFERENCES shadow_attempt_report(attempt_id,provider_id,job_id,window_id,session_id,logical_request_ordinal)
);
CREATE TABLE shadow_terminal_attestation (
 attestation_id TEXT PRIMARY KEY, provider_id TEXT NOT NULL, job_id TEXT NOT NULL, window_id TEXT NOT NULL, session_id TEXT NOT NULL, evidence_id TEXT NOT NULL, candidate_id TEXT NOT NULL, session_report_id TEXT NOT NULL, session_report_version INTEGER NOT NULL CHECK(session_report_version>=1), request_plan_canonical_json BLOB NOT NULL CHECK(length(request_plan_canonical_json)<=1048576), completion_canonical_json BLOB NOT NULL CHECK(length(completion_canonical_json)<=1048576), attempt_ordinal_closure_canonical_json BLOB NOT NULL CHECK(length(attempt_ordinal_closure_canonical_json)<=1048576), report_digest_canonical_json BLOB NOT NULL CHECK(length(report_digest_canonical_json)<=1048576), attempt_ordinal_closure_sha256 TEXT NOT NULL CHECK(length(attempt_ordinal_closure_sha256)=64), request_plan_sha256 TEXT NOT NULL CHECK(length(request_plan_sha256)=64), completion_sha256 TEXT NOT NULL CHECK(length(completion_sha256)=64), report_digest_sha256 TEXT NOT NULL CHECK(length(report_digest_sha256)=64), evidence_sha256 TEXT NOT NULL CHECK(length(evidence_sha256)=64), candidate_sha256 TEXT NOT NULL CHECK(length(candidate_sha256)=64), terminal_outcome TEXT NOT NULL CHECK(terminal_outcome='success'), immutable_version INTEGER NOT NULL CHECK(immutable_version>=1), UNIQUE(attestation_id,provider_id,job_id,window_id), UNIQUE(attestation_id,provider_id,window_id), UNIQUE(attestation_id,provider_id,job_id,window_id,session_id), UNIQUE(provider_id,job_id,window_id,session_id), FOREIGN KEY(job_id,provider_id,window_id) REFERENCES shadow_job(job_id,provider_id,window_id), FOREIGN KEY(provider_id,window_id) REFERENCES qualification_window(provider_id,window_id), FOREIGN KEY(evidence_id,provider_id,job_id,window_id,session_id) REFERENCES shadow_evidence_ref(evidence_id,provider_id,job_id,window_id,session_id), FOREIGN KEY(evidence_id,provider_id,job_id,window_id,session_id,evidence_sha256) REFERENCES shadow_evidence_ref(evidence_id,provider_id,job_id,window_id,session_id,evidence_sha256), FOREIGN KEY(candidate_id,provider_id,job_id,window_id,session_id) REFERENCES shadow_candidate_ref(candidate_id,provider_id,job_id,window_id,session_id), FOREIGN KEY(candidate_id,provider_id,job_id,window_id,session_id,candidate_sha256) REFERENCES shadow_candidate_ref(candidate_id,provider_id,job_id,window_id,session_id,candidate_sha256), FOREIGN KEY(session_report_id,provider_id,job_id,window_id,session_id,session_report_version) REFERENCES session_report(session_report_id,provider_id,job_id,window_id,session_id,report_version)
);
CREATE TRIGGER shadow_attempt_report_immutable_update BEFORE UPDATE ON shadow_attempt_report BEGIN SELECT RAISE(ABORT,'shadow_attempt_report_append_only'); END;
CREATE TRIGGER shadow_attempt_report_immutable_delete BEFORE DELETE ON shadow_attempt_report BEGIN SELECT RAISE(ABORT,'shadow_attempt_report_append_only'); END;
CREATE TRIGGER session_report_immutable_update BEFORE UPDATE ON session_report BEGIN SELECT RAISE(ABORT,'session_report_append_only'); END;
CREATE TRIGGER session_report_immutable_delete BEFORE DELETE ON session_report BEGIN SELECT RAISE(ABORT,'session_report_append_only'); END;
CREATE TRIGGER terminal_attestation_gate BEFORE INSERT ON shadow_terminal_attestation
BEGIN
 SELECT CASE WHEN shadow_validate_terminal_graph(
  NEW.request_plan_canonical_json,NEW.request_plan_sha256,
  NEW.completion_canonical_json,NEW.completion_sha256,
  NEW.attempt_ordinal_closure_canonical_json,NEW.attempt_ordinal_closure_sha256,
  NEW.report_digest_canonical_json,NEW.report_digest_sha256
 ) <> 1 THEN RAISE(ABORT,'terminal_digest_mismatch') END;
 SELECT CASE WHEN NOT EXISTS (
   SELECT 1 FROM session_report s WHERE s.session_report_id=NEW.session_report_id
    AND s.provider_id=NEW.provider_id AND s.job_id=NEW.job_id
    AND s.window_id=NEW.window_id AND s.session_id=NEW.session_id
    AND s.report_version=NEW.session_report_version AND s.outcome='success'
    AND s.terminal_attestation_id=NEW.attestation_id
 ) THEN RAISE(ABORT,'attestation_requires_terminal_success_session') END;
 SELECT CASE WHEN NOT EXISTS (
   SELECT 1 FROM shadow_evidence_ref e WHERE e.evidence_id=NEW.evidence_id
    AND e.provider_id=NEW.provider_id AND e.job_id=NEW.job_id
    AND e.window_id=NEW.window_id AND e.session_id=NEW.session_id
    AND e.evidence_sha256=NEW.evidence_sha256
 ) THEN RAISE(ABORT,'attestation_evidence_hash_mismatch') END;
 SELECT CASE WHEN NOT EXISTS (
   SELECT 1 FROM shadow_candidate_ref c WHERE c.candidate_id=NEW.candidate_id
    AND c.provider_id=NEW.provider_id AND c.job_id=NEW.job_id
    AND c.window_id=NEW.window_id AND c.session_id=NEW.session_id
    AND c.candidate_sha256=NEW.candidate_sha256
 ) THEN RAISE(ABORT,'attestation_candidate_hash_mismatch') END;
END;
CREATE TRIGGER session_report_hash_match BEFORE INSERT ON session_report
WHEN NEW.outcome IN ('evidence_ready','success')
BEGIN
 SELECT CASE WHEN NOT EXISTS (
   SELECT 1 FROM shadow_evidence_ref e WHERE e.evidence_id=NEW.evidence_id
    AND e.provider_id=NEW.provider_id AND e.job_id=NEW.job_id
    AND e.window_id=NEW.window_id AND e.session_id=NEW.session_id
    AND e.evidence_sha256=NEW.evidence_sha256
 ) THEN RAISE(ABORT,'session_hash_mismatch') END;
 SELECT CASE WHEN NEW.outcome='success' AND NOT EXISTS (
   SELECT 1 FROM shadow_candidate_ref c WHERE c.candidate_id=NEW.candidate_id
    AND c.provider_id=NEW.provider_id AND c.job_id=NEW.job_id
    AND c.window_id=NEW.window_id AND c.session_id=NEW.session_id
    AND c.candidate_sha256=NEW.candidate_sha256
 ) THEN RAISE(ABORT,'session_hash_mismatch') END;
END;
CREATE TRIGGER session_report_success_requires_terminal_attempt BEFORE INSERT ON session_report
WHEN NEW.outcome='success'
BEGIN
 SELECT CASE WHEN NOT EXISTS (
   SELECT 1 FROM shadow_attempt_report a WHERE a.attempt_id=NEW.successful_attempt_id
    AND a.provider_id=NEW.provider_id AND a.job_id=NEW.job_id
    AND a.window_id=NEW.window_id AND a.session_id=NEW.session_id
    AND a.outcome='success' AND a.terminal_marker=1
    AND a.terminal_session_report_id=NEW.session_report_id
 ) THEN RAISE(ABORT,'success_requires_terminal_attempt') END;
END;
CREATE TRIGGER evidence_ready_requires_pending_normalization BEFORE INSERT ON shadow_attempt_report
WHEN NEW.outcome='evidence_ready'
BEGIN
 SELECT CASE WHEN NOT EXISTS (
   SELECT 1 FROM shadow_job j WHERE j.job_id=NEW.job_id
    AND j.provider_id=NEW.provider_id AND j.window_id=NEW.window_id
    AND j.run_status='pending_normalization'
 ) THEN RAISE(ABORT,'evidence_ready_requires_pending_normalization') END;
END;
CREATE TRIGGER candidate_attach_requires_nonterminal_job BEFORE INSERT ON shadow_candidate_ref
BEGIN
 SELECT CASE WHEN NOT EXISTS (
   SELECT 1 FROM shadow_job j WHERE j.job_id=NEW.job_id
    AND j.provider_id=NEW.provider_id AND j.window_id=NEW.window_id
    AND j.run_status IN ('leased','pending_normalization')
 ) THEN RAISE(ABORT,'candidate_attach_after_terminal') END;
END;
"""


def _nfc(value: Any) -> Any:
    if value is None:
        raise ValueError("canonical JSON cannot contain null")
    if isinstance(value, str):
        return unicodedata.normalize("NFC", value)
    if isinstance(value, list):
        return [_nfc(item) for item in value]
    if isinstance(value, dict):
        normalized = {
            unicodedata.normalize("NFC", str(key)): _nfc(item) for key, item in value.items()
        }
        if len(normalized) != len(value):
            raise ValueError("canonical JSON keys collide after NFC normalization")
        return normalized
    return value


_DOMAINS = {
    "request-plan": {"job_id", "provider_id", "window_id", "requests"},
    "completion": {
        "job_id",
        "provider_id",
        "window_id",
        "session_id",
        "evidence_id",
        "request_plan_sha256",
        "requests",
    },
    "attempt-ordinal-closure": {"exact_ordinal_set", "ordinals"},
    "report-digest": {"session_report_id", "report_version", "reports"},
}


def canonical_digest(domain: str, raw: bytes) -> str:
    if domain not in _DOMAINS or not isinstance(raw, (bytes, bytearray, memoryview)):
        raise ValueError("invalid canonical-json input")
    obj = json.loads(bytes(raw).decode("utf-8"))
    if (
        not isinstance(obj, dict)
        or set(obj) != _DOMAINS[domain]
        or any(value is None for value in obj.values())
    ):
        raise ValueError("wrong field set or nullable success value")
    canonical = (
        json.dumps(_nfc(obj), ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n"
    ).encode()
    if canonical != bytes(raw):
        raise ValueError("non-canonical bytes")
    prefix = f"stock-eva/r2f3/{domain}/v1\n".encode()
    return hashlib.sha256(prefix + canonical).hexdigest()


def shadow_sha256_canonical_json(domain: str, canonical_json: bytes) -> str:
    return canonical_digest(domain, canonical_json)


def shadow_validate_terminal_graph(*values: Any) -> int:
    try:
        pairs = (
            ("request-plan", values[0], values[1]),
            ("completion", values[2], values[3]),
            ("attempt-ordinal-closure", values[4], values[5]),
            ("report-digest", values[6], values[7]),
        )
        return int(all(canonical_digest(domain, raw) == digest for domain, raw, digest in pairs))
    except (IndexError, TypeError, ValueError, UnicodeError, json.JSONDecodeError):
        return 0


def initialize_registry(connection: sqlite3.Connection) -> None:
    connection.execute("PRAGMA foreign_keys=ON")
    try:
        connection.execute("BEGIN IMMEDIATE")
        connection.executescript(REGISTRY_DDL)
        connection.executemany(
            "INSERT INTO schema_migration VALUES (?,?,datetime('now'))",
            [(migration_id, index) for index, migration_id in enumerate(MIGRATION_IDS, start=1)],
        )
        connection.commit()
    except sqlite3.DatabaseError:
        connection.rollback()
        raise
