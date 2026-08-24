"""A small, injected and bounded HTTP boundary for shadow providers.

The shadow lane deliberately does not construct a network client itself.  Tests and an
explicitly-authorized caller inject an object exposing ``request``.  This keeps the plan
path network-free and makes all transport limits visible in one place.
"""

from __future__ import annotations

import json
import os
import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from time import sleep
from typing import Any, Protocol

from pydantic import BaseModel, ConfigDict, Field


class HttpTransport(Protocol):
    def request(self, method: str, endpoint: str, **kwargs: Any) -> Any: ...


class HttpPolicy(BaseModel):
    """Fixed bounds shared by both secondary provider adapters."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    connect_timeout_seconds: float = Field(default=5.0, gt=0, le=30)
    read_timeout_seconds: float = Field(default=30.0, gt=0, le=120)
    write_timeout_seconds: float = Field(default=5.0, gt=0, le=30)
    pool_timeout_seconds: float = Field(default=5.0, gt=0, le=30)
    max_attempts: int = Field(default=3, ge=1, le=5)
    max_requests: int = Field(default=32, ge=1, le=256)
    max_response_bytes: int = Field(default=8 * 1024 * 1024, ge=1, le=64 * 1024 * 1024)
    max_rows: int = Field(default=100_000, ge=1, le=10_000_000)
    max_retry_after_seconds: float = Field(default=10.0, ge=0, le=60)

    @property
    def timeout(self) -> dict[str, float]:
        return {
            "connect": self.connect_timeout_seconds,
            "read": self.read_timeout_seconds,
            "write": self.write_timeout_seconds,
            "pool": self.pool_timeout_seconds,
        }


class ProviderHttpError(RuntimeError):
    """Sanitized typed transport failure; never includes URLs or response payloads."""

    def __init__(
        self,
        failure_class: str,
        *,
        endpoint: str = "provider-endpoint",
        status_code: int | None = None,
        attempts: int = 0,
        request_count: int = 0,
    ) -> None:
        self.failure_class = failure_class
        self.endpoint = _safe_identity(endpoint)
        self.status_code = status_code
        self.attempts = attempts
        self.request_count = request_count
        # Keep the text deliberately small.  Callers can use the typed fields for audit.
        super().__init__(f"provider request failed: {failure_class}")


class CanaryPermissionError(PermissionError):
    """A provider request was attempted without the private canary capability."""


class _CanaryPermit:
    """Non-serializable, module-private authority passed to one adapter invocation."""

    __slots__ = ("_provider_id", "_authorization_id", "_token")

    def __init__(self, provider_id: str, authorization_id: str, token: str = "") -> None:
        self._provider_id = provider_id
        self._authorization_id = authorization_id
        self._token = token

    @property
    def provider_id(self) -> str:
        return self._provider_id

    @property
    def authorization_id(self) -> str:
        return self._authorization_id

    def headers(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self._token}"} if self._token else {}

    def __repr__(self) -> str:
        return "<private-canary-permit>"

    def __getstate__(self):
        raise TypeError("canary permits are not serializable")

    def __reduce__(self):
        raise TypeError("canary permits are not serializable")


def _valid_authorization_id(value: str) -> bool:
    return bool(re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}", value))


def build_canary_permit(
    provider_id: str,
    registry: Any,
    *,
    external_authorization_id: str,
    adapter_hash: str,
    endpoint_contract_hash: str,
    source_schema_hash: str,
) -> _CanaryPermit:
    """Validate the Task10 descriptor before reading the closed credential env map."""
    from .shadow_contracts import STATIC_PROVIDER_CONTRACTS, exact_credential_env

    if not _valid_authorization_id(external_authorization_id):
        raise CanaryPermissionError("external authorization is invalid")
    if provider_id not in STATIC_PROVIDER_CONTRACTS:
        raise CanaryPermissionError("provider descriptor unavailable")
    contract = STATIC_PROVIDER_CONTRACTS[provider_id]
    record = registry.read_status(provider_id)
    if record.admission_state.value != "canary":
        raise CanaryPermissionError("provider is not admitted for canary")
    if (
        record.adapter_hash != adapter_hash
        or record.endpoint_contract_hash != endpoint_contract_hash
        or record.source_schema_hash != source_schema_hash
        or record.credential_env_name != exact_credential_env(provider_id)
        or not record.terms_evidence_hash
        or not record.terms_review_id
    ):
        raise CanaryPermissionError("provider descriptor version unavailable")
    # Resolve the reviewed terms row through the registry's descriptor-bound reader.  The
    # token is deliberately not read until every static and reviewed decision passes.
    with registry._lock(shared=True):
        connection = registry._connection_for_read()
        try:
            row = connection.execute(
                "SELECT terms_evidence_id FROM terms_evidence WHERE provider_id=? "
                "AND manifest_sha256=? AND review_id=?",
                (provider_id, record.terms_evidence_hash, record.terms_review_id),
            ).fetchone()
        finally:
            if not getattr(registry, "_memory", False):
                connection.close()
    if row is None:
        raise CanaryPermissionError("reviewed terms evidence unavailable")
    terms = registry.read_terms_evidence(row[0])
    if (
        terms.manifest_sha256 != record.terms_evidence_hash
        or terms.review_id != record.terms_review_id
        or terms.approved_credential_mode != "environment-only"
        or terms.approved_intended_use != record.intended_use
        or terms.approved_retention != record.retention_decision
        or not terms.approved_quota_decision
        or (contract.requires_official_https_proof and not contract.official_https_proven)
    ):
        raise CanaryPermissionError("reviewed provider decisions unavailable")
    env_name = exact_credential_env(provider_id, requested=record.credential_env_name)
    token = os.environ.get(env_name, "")
    if not token:
        raise CanaryPermissionError("provider credential unavailable")
    return _CanaryPermit(provider_id, external_authorization_id, token)


@dataclass(frozen=True)
class HttpResult:
    endpoint: str
    status_code: int
    payload: Any
    attempts: int
    request_count: int


def _safe_identity(value: str) -> str:
    # Endpoints in this lane are static contract names.  Never echo a URL/query/header.
    if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9_.-]{1,64}", value):
        return "provider-endpoint"
    return value


def _bounded_retry_after(headers: Mapping[str, Any], limit: float) -> float:
    value = headers.get("Retry-After") or headers.get("retry-after")
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return 0.0
    return min(max(parsed, 0.0), limit)


def _response_headers(response: Any) -> Mapping[str, Any]:
    headers = getattr(response, "headers", {})
    return headers if isinstance(headers, Mapping) else {}


def _response_bytes(response: Any) -> bytes:
    content = getattr(response, "content", b"")
    if isinstance(content, bytes):
        return content
    if isinstance(content, bytearray):
        return bytes(content)
    return b""


def _row_count(value: Any) -> int:
    if isinstance(value, list):
        return len(value) + sum(_row_count(item) for item in value)
    if isinstance(value, dict):
        return sum(_row_count(item) for item in value.values())
    return 0


class BoundedHttpClient:
    """Injectable request wrapper with hard timeout, retry and response bounds."""

    def __init__(
        self,
        client: HttpTransport,
        *,
        policy: HttpPolicy | None = None,
        sleeper: Callable[[float], None] = sleep,
    ) -> None:
        self.client = client
        self.policy = policy or HttpPolicy()
        self._sleeper = sleeper
        self.request_count = 0
        self._last_result: HttpResult | None = None

    @property
    def last_result(self) -> HttpResult | None:
        return self._last_result

    def request_json(
        self,
        endpoint: str,
        *,
        method: str = "GET",
        params: Mapping[str, Any] | None = None,
        json_body: Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> Any:
        return self.request(
            endpoint,
            method=method,
            params=params,
            json_body=json_body,
            headers=headers,
        ).payload

    def request(
        self,
        endpoint: str,
        *,
        method: str = "GET",
        params: Mapping[str, Any] | None = None,
        json_body: Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> HttpResult:
        identity = _safe_identity(endpoint)
        attempts = 0
        while attempts < self.policy.max_attempts:
            if self.request_count >= self.policy.max_requests:
                raise ProviderHttpError(
                    "request_budget_exhausted",
                    endpoint=identity,
                    attempts=attempts,
                    request_count=self.request_count,
                )
            attempts += 1
            self.request_count += 1
            try:
                response = self.client.request(
                    method,
                    identity,
                    params=dict(params or {}),
                    json=dict(json_body or {}) if json_body is not None else None,
                    headers=dict(headers or {}),  # headers are never persisted or echoed
                    timeout=self.policy.timeout,
                )
            except TimeoutError:
                if attempts >= self.policy.max_attempts:
                    raise ProviderHttpError(
                        "timeout",
                        endpoint=identity,
                        attempts=attempts,
                        request_count=self.request_count,
                    ) from None
                continue
            except Exception:
                if attempts >= self.policy.max_attempts:
                    raise ProviderHttpError(
                        "transport_error",
                        endpoint=identity,
                        attempts=attempts,
                        request_count=self.request_count,
                    ) from None
                continue

            status = int(getattr(response, "status_code", 0))
            content = _response_bytes(response)
            if len(content) > self.policy.max_response_bytes:
                raise ProviderHttpError(
                    "response_oversize",
                    endpoint=identity,
                    status_code=status,
                    attempts=attempts,
                    request_count=self.request_count,
                )
            if status == 429 or status >= 500:
                if attempts < self.policy.max_attempts:
                    delay = _bounded_retry_after(
                        _response_headers(response), self.policy.max_retry_after_seconds
                    )
                    if delay:
                        self._sleeper(delay)
                    continue
                failure = "rate_limited" if status == 429 else "server_error"
                raise ProviderHttpError(
                    failure,
                    endpoint=identity,
                    status_code=status,
                    attempts=attempts,
                    request_count=self.request_count,
                ) from None
            if status in (401, 403):
                raise ProviderHttpError(
                    "unauthorized",
                    endpoint=identity,
                    status_code=status,
                    attempts=attempts,
                    request_count=self.request_count,
                )
            if not 200 <= status < 300:
                raise ProviderHttpError(
                    "http_error",
                    endpoint=identity,
                    status_code=status,
                    attempts=attempts,
                    request_count=self.request_count,
                )
            try:
                payload = response.json()
            except Exception:
                try:
                    payload = json.loads(content.decode("utf-8"))
                except Exception:
                    raise ProviderHttpError(
                        "malformed_json",
                        endpoint=identity,
                        status_code=status,
                        attempts=attempts,
                        request_count=self.request_count,
                    ) from None
            if _row_count(payload) > self.policy.max_rows:
                raise ProviderHttpError(
                    "row_budget_exhausted",
                    endpoint=identity,
                    status_code=status,
                    attempts=attempts,
                    request_count=self.request_count,
                )
            result = HttpResult(identity, status, payload, attempts, self.request_count)
            self._last_result = result
            return result
        raise ProviderHttpError("retry_budget_exhausted", endpoint=identity, attempts=attempts)

    def get(self, endpoint: str, **kwargs: Any) -> HttpResult:
        return self.request(endpoint, method="GET", **kwargs)

    def post(self, endpoint: str, **kwargs: Any) -> HttpResult:
        return self.request(endpoint, method="POST", **kwargs)


# Compatibility spellings for callers that use the acronym form.
BoundedHTTPClient = BoundedHttpClient
HttpRequestPolicy = HttpPolicy
