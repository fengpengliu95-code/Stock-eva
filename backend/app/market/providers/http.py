"""A small, injected and bounded HTTP boundary for shadow providers.

The shadow lane deliberately does not construct a network client itself.  Tests and an
explicitly-authorized caller inject an object exposing ``request``.  This keeps the plan
path network-free and makes all transport limits visible in one place.
"""

from __future__ import annotations

import json
import re
import weakref
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from time import sleep
from typing import Any, Protocol
from urllib.parse import urlsplit

from pydantic import BaseModel, ConfigDict, Field


class HttpTransport(Protocol):
    def request(self, method: str, endpoint: str, **kwargs: Any) -> Any: ...


class CanaryExecutor(Protocol):
    """Opaque executor interface; construction is not an authorization decision."""

    def execute(self, adapter: Any, trade_date: Any, *, symbols: tuple[str, ...] = ()) -> Any: ...


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


class _StreamingHttpxResponse:
    """Small response facade that keeps the body bounded while it is received."""

    def __init__(self, response: Any) -> None:
        self._response = response
        self.status_code = response.status_code
        self.headers = response.headers
        self.url = response.url
        self.history = response.history

    def iter_bytes(self):
        try:
            yield from self._response.iter_bytes()
        finally:
            self._response.close()

    def close(self) -> None:
        self._response.close()


class _TickFlowHttpxTransport:
    """Adapt httpx streaming responses to the injected transport protocol."""

    def __init__(self, client: Any) -> None:
        self._client = client

    def request(self, method: str, endpoint: str, **kwargs: Any) -> _StreamingHttpxResponse:
        # TLS verification, base URL and redirect policy are fixed on the client. BoundedHttpClient
        # still passes these flags to make fake transports assert the same policy.
        kwargs.pop("verify", None)
        kwargs.pop("follow_redirects", None)
        timeout = kwargs.pop("timeout", None)
        if isinstance(timeout, Mapping):
            import httpx

            timeout = httpx.Timeout(
                timeout.get("read", 30),
                connect=timeout.get("connect", 5),
                write=timeout.get("write", 5),
                pool=timeout.get("pool", 5),
            )
        request = self._client.build_request(method, endpoint, timeout=timeout, **kwargs)
        return _StreamingHttpxResponse(self._client.send(request, stream=True))

    def close(self) -> None:
        self._client.close()


def build_tickflow_client() -> HttpTransport:
    """Build the sole deployable TickFlow client with an exact pinned origin."""
    import httpx

    return _TickFlowHttpxTransport(
        httpx.Client(
            base_url="https://api.tickflow.org",
            verify=True,
            follow_redirects=False,
            timeout=httpx.Timeout(30, connect=5, write=5, pool=5),
        )
    )


class CanaryPermissionError(PermissionError):
    """A provider request was attempted without the private canary capability."""


class _CanaryPermit:
    """Non-serializable, module-private authority passed to one adapter invocation."""

    __slots__ = (
        "_provider_id",
        "_authorization_id",
        "_token",
        "_adapter_hash",
        "_endpoint_contract_hash",
        "_source_schema_hash",
        "_terms_review_id",
        "_seal",
        "__weakref__",
    )

    def __init__(
        self,
        provider_id: str,
        authorization_id: str,
        token: str = "",
        *,
        _seal: object | None = None,
        _adapter_hash: str = "",
        _endpoint_contract_hash: str = "",
        _source_schema_hash: str = "",
        _terms_review_id: str = "",
    ) -> None:
        if _seal is not _PERMIT_SEAL:
            raise CanaryPermissionError("canary permit issuer required")
        self._provider_id = provider_id
        self._authorization_id = authorization_id
        self._token = token
        self._adapter_hash = _adapter_hash
        self._endpoint_contract_hash = _endpoint_contract_hash
        self._source_schema_hash = _source_schema_hash
        self._terms_review_id = _terms_review_id
        self._seal = _PERMIT_SEAL
        _ISSUED_PERMITS.add(self)

    @property
    def provider_id(self) -> str:
        return self._provider_id

    @property
    def authorization_id(self) -> str:
        return self._authorization_id

    def headers(self) -> dict[str, str]:
        token = self._token
        if not token:
            return {}
        # TickFlow's pinned contract uses x-api-key; Tushare retains its legacy bearer
        # contract.  The token is consumed exactly once and never persisted.
        if self._provider_id == "tickflow":
            return {"x-api-key": token}
        return {"Authorization": f"Bearer {token}"}

    def __repr__(self) -> str:
        return "<private-canary-permit>"

    def __getstate__(self):
        raise TypeError("canary permits are not serializable")

    def __reduce__(self):
        raise TypeError("canary permits are not serializable")


_PERMIT_SEAL = object()
_ISSUED_PERMITS: weakref.WeakSet[_CanaryPermit] = weakref.WeakSet()


def _check_canary_permit(
    permit: object,
    *,
    provider_id: str,
    adapter_hash: str,
    endpoint_contract_hash: str,
    source_schema_hash: str,
    terms_review_id: str | None = None,
    authorization_id: str | None = None,
) -> _CanaryPermit:
    if type(permit) is not _CanaryPermit or permit not in _ISSUED_PERMITS:
        raise CanaryPermissionError("canary permit is not issued")
    if (
        permit._seal is not _PERMIT_SEAL
        or permit._provider_id != provider_id
        or permit._adapter_hash != adapter_hash
        or permit._endpoint_contract_hash != endpoint_contract_hash
        or permit._source_schema_hash != source_schema_hash
        or (terms_review_id is not None and permit._terms_review_id != terms_review_id)
        or (authorization_id is not None and permit._authorization_id != authorization_id)
    ):
        raise CanaryPermissionError("canary permit descriptor mismatch")
    return permit


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
    environ: Mapping[str, str],
) -> _CanaryPermit:
    """Validate the Task10 descriptor before reading the closed credential env map."""
    from .shadow_contracts import STATIC_PROVIDER_CONTRACTS, exact_credential_env

    if not _valid_authorization_id(external_authorization_id):
        raise CanaryPermissionError("external authorization is invalid")
    if provider_id not in STATIC_PROVIDER_CONTRACTS:
        raise CanaryPermissionError("provider descriptor unavailable")
    record, terms = _read_canary_descriptor(
        provider_id,
        registry,
        adapter_hash=adapter_hash,
        endpoint_contract_hash=endpoint_contract_hash,
        source_schema_hash=source_schema_hash,
    )
    env_name = exact_credential_env(provider_id, requested=record.credential_env_name)
    if not isinstance(environ, Mapping):
        raise CanaryPermissionError("closed environment required")
    token = environ.get(env_name, "")
    if not token:
        raise CanaryPermissionError("provider credential unavailable")
    return _CanaryPermit(
        provider_id,
        external_authorization_id,
        token,
        _seal=_PERMIT_SEAL,
        _adapter_hash=adapter_hash,
        _endpoint_contract_hash=endpoint_contract_hash,
        _source_schema_hash=source_schema_hash,
        _terms_review_id=terms.review_id,
    )


def _read_canary_descriptor(
    provider_id: str,
    registry: Any,
    *,
    adapter_hash: str,
    endpoint_contract_hash: str,
    source_schema_hash: str,
):
    from .registry import ShadowRegistry
    from .shadow_contracts import STATIC_PROVIDER_CONTRACTS, exact_credential_env

    contract = STATIC_PROVIDER_CONTRACTS[provider_id]
    if type(registry) is not ShadowRegistry:
        raise CanaryPermissionError("concrete shadow registry required")
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
        or (
            provider_id == "tickflow"
            and not {
                "https://docs.tickflow.org/zh-Hans/api-reference/openapi.json",
                "https://tickflow.org/legal/terms-of-service.md",
            }.issubset(terms.official_url_allowlist)
        )
    ):
        raise CanaryPermissionError("reviewed provider decisions unavailable")
    return record, terms


@dataclass(frozen=True)
class HttpResult:
    endpoint: str
    status_code: int
    payload: Any
    attempts: int
    request_count: int


@dataclass(frozen=True, init=False)
class _AuthorizedCanarySession:
    """Descriptor-bound executor; only ``execute`` performs the authority gate.

    This private name and same-process object identity are not security controls.  Every
    request reopens the concrete registry and revalidates the reviewed descriptor.
    """

    provider_id: str
    registry_path: Path
    external_authorization_id: str
    expected_state_version: int
    expected_terms_evidence_hash: str
    expected_terms_review_id: str
    credential_env_name: str
    _credential_reader: Callable[[str], str] = field(repr=False)
    _client_factory: Callable[[], HttpTransport] = field(repr=False)

    def __init__(
        self,
        *,
        provider_id: str,
        registry_path: Path,
        external_authorization_id: str,
        expected_state_version: int,
        expected_terms_evidence_hash: str,
        expected_terms_review_id: str,
        credential_env_name: str,
        environ: Mapping[str, str],
        client_factory: Callable[[], HttpTransport],
    ) -> None:
        object.__setattr__(self, "provider_id", provider_id)
        object.__setattr__(self, "registry_path", registry_path)
        object.__setattr__(self, "external_authorization_id", external_authorization_id)
        object.__setattr__(self, "expected_state_version", expected_state_version)
        object.__setattr__(self, "expected_terms_evidence_hash", expected_terms_evidence_hash)
        object.__setattr__(self, "expected_terms_review_id", expected_terms_review_id)
        object.__setattr__(self, "credential_env_name", credential_env_name)
        # Keep only a lookup closure, never the credential mapping or token itself.
        object.__setattr__(self, "_credential_reader", lambda name: environ.get(name, ""))
        object.__setattr__(self, "_client_factory", client_factory)

    def execute(self, adapter: Any, trade_date: Any, *, symbols: tuple[str, ...] = ()) -> Any:
        if getattr(adapter, "PROVIDER_ID", None) != self.provider_id:
            raise CanaryPermissionError("authorized session provider mismatch")
        if self.provider_id == "tushare":
            from .tushare import TushareExecutionBlocked

            raise TushareExecutionBlocked("tushare official HTTPS is unproven")
        from .registry import ShadowRegistry

        client: BoundedHttpClient | None = None
        payloads: dict[str, Any] = {}
        # Task11's old fixture used three source-shaped endpoints. Keep that replay-only
        # compatibility for an empty symbol tuple; an execute request with symbols is Task14's
        # fixed four-request contract.
        endpoints = adapter.ENDPOINTS if symbols else adapter.ENDPOINTS[:3]
        for endpoint in endpoints:
            registry = ShadowRegistry(self.registry_path)
            try:
                current = registry.read_status(self.provider_id)
            except Exception as exc:
                if client is not None:
                    client.close()
                raise CanaryPermissionError("provider descriptor unavailable") from exc
            if (
                current.state_version != self.expected_state_version
                or current.terms_evidence_hash != self.expected_terms_evidence_hash
                or current.terms_review_id != self.expected_terms_review_id
                or current.credential_env_name != self.credential_env_name
            ):
                if client is not None:
                    client.close()
                raise CanaryPermissionError("provider descriptor changed")
            try:
                permit = build_canary_permit(
                    self.provider_id,
                    registry,
                    external_authorization_id=self.external_authorization_id,
                    adapter_hash=adapter.ADAPTER_HASH,
                    endpoint_contract_hash=adapter.ENDPOINT_CONTRACT_HASH,
                    source_schema_hash=adapter.SOURCE_SCHEMA_HASH,
                    environ={
                        self.credential_env_name: self._credential_reader(self.credential_env_name)
                    },
                )
            except Exception:
                if client is not None:
                    client.close()
                raise
            if client is None:
                client = BoundedHttpClient(
                    self._client_factory(),
                    policy=HttpPolicy(
                        connect_timeout_seconds=5,
                        read_timeout_seconds=30,
                        write_timeout_seconds=5,
                        pool_timeout_seconds=5,
                        max_attempts=1,
                        max_requests=4,
                        max_response_bytes=8 * 1024 * 1024,
                    ),
                    allowed_server="https://api.tickflow.org",
                    owns_client=True,
                )
            try:
                payloads[endpoint] = client.request_json(
                    adapter.request_endpoint(endpoint)
                    if hasattr(adapter, "request_endpoint")
                    else endpoint,
                    params=adapter.request_params(endpoint, trade_date, symbols),
                    headers=permit.headers(),
                )
            except Exception:
                client.close()
                raise
        object.__setattr__(
            self,
            "last_raw_content_by_endpoint",
            dict(client.raw_content_by_endpoint if client is not None else {}),
        )
        try:
            result = adapter.parse(
                trade_date,
                payloads,
                request_count=client.request_count if client else 0,
                symbols=symbols,
            )
        except Exception as exc:
            if client is not None:
                client.close()
            if hasattr(exc, "request_count"):
                exc.request_count = client.request_count if client else 0
            else:
                try:
                    exc.request_count = client.request_count if client else 0
                except Exception:
                    pass
            raise
        if client is not None:
            client.close()
        return result


def build_authorized_canary_session(
    provider_id: str,
    registry: Any,
    *,
    external_authorization_id: str,
    environ: Mapping[str, str],
    client_factory: Callable[[], HttpTransport],
) -> CanaryExecutor:
    from .registry import ShadowRegistry
    from .shadow_contracts import exact_credential_env

    if provider_id not in {"tickflow", "tushare"}:
        raise CanaryPermissionError("provider descriptor unavailable")
    if provider_id == "tushare":
        from .tushare import TushareExecutionBlocked

        raise TushareExecutionBlocked("tushare official HTTPS is unproven")
    if not _valid_authorization_id(external_authorization_id):
        raise CanaryPermissionError("external authorization is invalid")
    if not isinstance(environ, Mapping) or not callable(client_factory):
        raise CanaryPermissionError("closed environment and client factory required")
    if type(registry) is ShadowRegistry:
        registry_obj = registry
    elif isinstance(registry, (Path, str)):
        registry_obj = ShadowRegistry(registry)
    else:
        raise CanaryPermissionError("concrete shadow registry required")
    from .tickflow import TickFlowAdapter

    adapter = TickFlowAdapter
    record, terms = _read_canary_descriptor(
        provider_id,
        registry_obj,
        adapter_hash=adapter.ADAPTER_HASH,
        endpoint_contract_hash=adapter.ENDPOINT_CONTRACT_HASH,
        source_schema_hash=adapter.SOURCE_SCHEMA_HASH,
    )
    if record.credential_env_name != exact_credential_env(provider_id):
        raise CanaryPermissionError("provider credential mapping unavailable")
    return _AuthorizedCanarySession(
        provider_id=provider_id,
        registry_path=Path(registry_obj.path),
        external_authorization_id=external_authorization_id,
        expected_state_version=record.state_version,
        expected_terms_evidence_hash=record.terms_evidence_hash or "",
        expected_terms_review_id=terms.review_id,
        credential_env_name=record.credential_env_name,
        environ=environ,
        client_factory=client_factory,
    )


def _safe_identity(value: str) -> str:
    # Only static endpoint names and pinned API paths are accepted.  Never echo a URL/query/header.
    if not isinstance(value, str) or not re.fullmatch(
        r"(?:[A-Za-z0-9_.-]{1,64}|/v1/[A-Za-z0-9_.-]+(?:/[A-Za-z0-9_.-]+)*)", value
    ):
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


def _close_response(response: Any) -> None:
    close = getattr(response, "close", None)
    if callable(close):
        try:
            close()
        except Exception:
            pass


def _is_timeout_error(error: BaseException) -> bool:
    if isinstance(error, TimeoutError):
        return True
    try:
        import httpx
    except ImportError:
        return False
    return isinstance(error, httpx.TimeoutException)


def _response_bytes(response: Any, *, max_bytes: int | None = None) -> bytes:
    iterator = getattr(response, "iter_bytes", None)
    if callable(iterator):
        chunks: list[bytes] = []
        total = 0
        try:
            for chunk in iterator():
                if not isinstance(chunk, (bytes, bytearray)):
                    raise ProviderHttpError("malformed_response")
                total += len(chunk)
                if max_bytes is not None and total > max_bytes:
                    raise ProviderHttpError("response_oversize")
                chunks.append(bytes(chunk))
        except ProviderHttpError:
            raise
        except Exception:
            raise ProviderHttpError("transport_error") from None
        return b"".join(chunks)
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
        allowed_server: str | None = None,
        owns_client: bool = False,
    ) -> None:
        self.client = client
        self.policy = policy or HttpPolicy()
        self._sleeper = sleeper
        self._allowed_server = allowed_server
        self._owns_client = owns_client
        self.request_count = 0
        self.raw_content_by_endpoint: dict[str, bytes] = {}
        self._last_result: HttpResult | None = None

    def close(self) -> None:
        if self._owns_client:
            _close_response(self.client)

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
                    follow_redirects=False,
                    verify=True,
                )
            except Exception as error:
                if not _is_timeout_error(error):
                    if attempts >= self.policy.max_attempts:
                        raise ProviderHttpError(
                            "transport_error",
                            endpoint=identity,
                            attempts=attempts,
                            request_count=self.request_count,
                        ) from None
                    continue
                if attempts >= self.policy.max_attempts:
                    raise ProviderHttpError(
                        "timeout",
                        endpoint=identity,
                        attempts=attempts,
                        request_count=self.request_count,
                    ) from None
                continue
            try:
                status = int(getattr(response, "status_code", 0))
            except (TypeError, ValueError):
                raise ProviderHttpError(
                    "malformed_response",
                    endpoint=identity,
                    attempts=attempts,
                    request_count=self.request_count,
                ) from None
            response_url = getattr(response, "url", None)
            if self._allowed_server:
                if not response_url:
                    _close_response(response)
                    raise ProviderHttpError(
                        "endpoint_url_unavailable",
                        endpoint=identity,
                        attempts=attempts,
                        request_count=self.request_count,
                    )
                try:
                    parsed_url = urlsplit(str(response_url))
                    expected = urlsplit(self._allowed_server)
                except ValueError:
                    _close_response(response)
                    raise ProviderHttpError(
                        "redirect_or_endpoint_mismatch",
                        endpoint=identity,
                        attempts=attempts,
                        request_count=self.request_count,
                    ) from None
                try:
                    response_port = parsed_url.port
                except ValueError:
                    _close_response(response)
                    raise ProviderHttpError(
                        "redirect_or_endpoint_mismatch",
                        endpoint=identity,
                        attempts=attempts,
                        request_count=self.request_count,
                    ) from None
                effective_response_port = response_port or (
                    443 if parsed_url.scheme == "https" else 80
                )
                effective_expected_port = expected.port or (
                    443 if expected.scheme == "https" else 80
                )
                if (
                    parsed_url.scheme != expected.scheme
                    or parsed_url.hostname != expected.hostname
                    or effective_response_port != effective_expected_port
                    or parsed_url.username is not None
                    or parsed_url.password is not None
                    or parsed_url.path != identity
                    or parsed_url.fragment
                ):
                    _close_response(response)
                    raise ProviderHttpError(
                        "redirect_or_endpoint_mismatch",
                        endpoint=identity,
                        attempts=attempts,
                        request_count=self.request_count,
                    )
            if getattr(response, "history", ()):
                _close_response(response)
                raise ProviderHttpError(
                    "redirect_or_endpoint_mismatch",
                    endpoint=identity,
                    attempts=attempts,
                    request_count=self.request_count,
                )
            try:
                content = _response_bytes(response, max_bytes=self.policy.max_response_bytes)
            except ProviderHttpError as error:
                _close_response(response)
                error.endpoint = identity
                error.attempts = attempts
                error.request_count = self.request_count
                raise
            _close_response(response)
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
            if not content:
                raise ProviderHttpError(
                    "malformed_json",
                    endpoint=identity,
                    status_code=status,
                    attempts=attempts,
                    request_count=self.request_count,
                )
            try:
                payload = json.loads(content.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError, TypeError):
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
            self.raw_content_by_endpoint[identity] = bytes(content)
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
