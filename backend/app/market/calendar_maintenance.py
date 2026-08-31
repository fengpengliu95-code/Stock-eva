"""Bounded fetch of the two staged official calendar objects.

This module deliberately has no persistence or scheduling authority.  Source admission is
performed through the pure calendar-generation validator before an HTTP client can be made.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime
from typing import Literal

import httpx

from .calendar_generation import (
    MAX_BODY_BYTES,
    MAX_SOURCE_BYTES,
    CalendarGenerationError,
    CalendarSourceBundleV1,
    body_sha256,
    canonical_json_bytes,
    validate_source_bundle,
)

Failure = Literal[
    "SOURCE_INVALID",
    "SOURCE_CONFLICT",
    "OFFICIAL_UNAVAILABLE",
    "OFFICIAL_HASH_MISMATCH",
]
ClientFactory = Callable[..., object]
_EXPECTED_TRANSPORT_ERRORS = (httpx.HTTPError, OSError, TimeoutError)
_SUPPORTED_CONTENT_ENCODINGS = frozenset({"identity", "gzip", "deflate"})


@dataclass(frozen=True, slots=True)
class _OfficialFetchResult:
    """Private fetch result; verified bodies are intentionally absent from repr output."""

    failure: Failure | None
    official_requests: int
    _bodies: tuple[bytes, bytes] | None = field(default=None, repr=False)

    @property
    def bodies(self) -> tuple[bytes, bytes] | None:
        return self._bodies


def _unavailable(requests: int) -> _OfficialFetchResult:
    return _OfficialFetchResult("OFFICIAL_UNAVAILABLE", requests)


def _close_client(client: object) -> bool:
    close = client.close
    try:
        close()
    except _EXPECTED_TRANSPORT_ERRORS:
        return False
    return True


def _fetch_body(client: object, url: str) -> tuple[bytes, bool]:
    """Return one bounded decoded body and whether the HTTP response was usable."""
    with client.stream("GET", url) as response:
        if response.status_code != 200:
            return b"", False
        content_encoding = response.headers.get("Content-Encoding")
        if content_encoding is not None:
            encodings = tuple(item.strip().lower() for item in content_encoding.split(","))
            if not encodings or any(item not in _SUPPORTED_CONTENT_ENCODINGS for item in encodings):
                return b"", False
        body = bytearray()
        for chunk in response.iter_bytes():
            if not isinstance(chunk, bytes):
                return b"", False
            if len(body) + len(chunk) > MAX_BODY_BYTES:
                return b"", False
            body.extend(chunk)
        if not body:
            return b"", False
        return bytes(body), True


def _pin_source(
    source: CalendarSourceBundleV1, now: datetime
) -> tuple[CalendarSourceBundleV1, str]:
    """Bound, parse, and admit a source snapshot before any client access."""
    if not isinstance(source, CalendarSourceBundleV1):
        raise CalendarGenerationError("source must be CalendarSourceBundleV1")
    try:
        serialized = canonical_json_bytes(source.model_dump(mode="json", warnings="error"))
    except (TypeError, ValueError) as exc:
        raise CalendarGenerationError("source serialization invalid") from exc
    if len(serialized) > MAX_SOURCE_BYTES:
        raise CalendarGenerationError("source exceeds size limit")
    try:
        pinned = CalendarSourceBundleV1.model_validate_json(serialized)
    except (TypeError, ValueError) as exc:
        raise CalendarGenerationError("source validation invalid") from exc
    admission = validate_source_bundle(pinned, now)
    return pinned, admission.outcome


def fetch_official_calendars(
    source: CalendarSourceBundleV1,
    *,
    now: datetime,
    client_factory: ClientFactory | None = None,
) -> _OfficialFetchResult:
    """Fetch exactly the two already-admitted official calendar URLs.

    The function is synchronous and intentionally returns only a private in-memory result.
    ``client_factory`` is injectable for offline tests; the default creates a fresh, fixed
    policy HTTPX client for each request.
    """
    try:
        pinned_source, admission = _pin_source(source, now)
    except CalendarGenerationError:
        return _OfficialFetchResult("SOURCE_INVALID", 0)

    if admission == "SOURCE_CONFLICT":
        return _OfficialFetchResult("SOURCE_CONFLICT", 0)

    def make_client(**kwargs: object) -> object:
        return httpx.Client(**kwargs)

    factory = make_client if client_factory is None else client_factory
    requests = 0
    bodies: list[bytes] = []
    client_options = {
        "trust_env": False,
        "verify": True,
        "follow_redirects": False,
        "auth": None,
        "headers": {"Accept-Encoding": "gzip, deflate"},
    }

    for schedule in pinned_source.schedules:
        client: object | None = None
        try:
            client = factory(
                timeout=httpx.Timeout(connect=5, read=30, write=5, pool=5), **client_options
            )
        except _EXPECTED_TRANSPORT_ERRORS:
            return _unavailable(requests)

        request_result: _OfficialFetchResult | None = None
        try:
            requests += 1
            try:
                body, usable = _fetch_body(client, schedule.official_url)
            except _EXPECTED_TRANSPORT_ERRORS:
                request_result = _unavailable(requests)
            else:
                if not usable:
                    request_result = _unavailable(requests)
                elif body_sha256(body) != schedule.body_sha256:
                    request_result = _OfficialFetchResult("OFFICIAL_HASH_MISMATCH", requests)
                else:
                    bodies.append(body)
        finally:
            close_ok = _close_client(client)
        if not close_ok:
            return _unavailable(requests)
        if request_result is not None:
            return request_result

    return _OfficialFetchResult(None, requests, (bodies[0], bodies[1]))


__all__ = ["fetch_official_calendars"]
