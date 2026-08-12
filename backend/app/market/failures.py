import socket
from typing import Literal

from pydantic import BaseModel, ConfigDict

MarketFailureStage = Literal["fetch", "normalize", "validate", "publish"]
MarketFailureClass = Literal[
    "transport_timeout",
    "transport_connect",
    "dns",
    "auth",
    "rate_limit",
    "provider_4xx",
    "provider_5xx",
    "schema",
    "semantic",
    "calendar",
    "universe",
    "coverage",
    "reconciliation",
    "storage",
    "internal",
]

_PROVIDER_FAILURE_CLASSES = {
    "transport_timeout",
    "transport_connect",
    "dns",
    "auth",
    "rate_limit",
    "provider_4xx",
    "provider_5xx",
}


class MarketFailure(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    failure_stage: MarketFailureStage
    failure_class: MarketFailureClass
    retryable: bool


class MarketFailureError(RuntimeError):
    def __init__(self, failure: MarketFailure) -> None:
        self.failure = failure
        super().__init__(f"market {failure.failure_stage} failed ({failure.failure_class})")


def baostock_status_failure(
    error_code: str,
    *,
    stage: MarketFailureStage = "fetch",
) -> MarketFailure:
    """Classify BaoStock status codes without inspecting provider message text."""
    try:
        numeric_code = int(error_code.strip())
    except (AttributeError, ValueError):
        numeric_code = -1
    if numeric_code in {401, 403}:
        failure_class, retryable = "auth", False
    elif numeric_code == 429:
        failure_class, retryable = "rate_limit", True
    elif 400 <= numeric_code < 500:
        failure_class, retryable = "provider_4xx", False
    else:
        # BaoStock commonly uses non-HTTP numeric statuses.  Unknown provider
        # statuses remain retryable, but are never inferred from error_msg.
        failure_class, retryable = "provider_5xx", True
    return MarketFailure(
        failure_stage=stage,
        failure_class=failure_class,
        retryable=retryable,
    )


def market_failure_from_exception(
    error: Exception,
    *,
    stage: MarketFailureStage,
    default_class: MarketFailureClass = "internal",
    default_retryable: bool = False,
) -> MarketFailure:
    failure = getattr(error, "failure", None)
    if isinstance(failure, MarketFailure):
        return failure
    if stage == "fetch":
        if isinstance(error, socket.gaierror):
            return MarketFailure(
                failure_stage=stage,
                failure_class="dns",
                retryable=True,
            )
        if isinstance(error, TimeoutError):
            return MarketFailure(
                failure_stage=stage,
                failure_class="transport_timeout",
                retryable=True,
            )
        if isinstance(error, OSError):
            return MarketFailure(
                failure_stage=stage,
                failure_class="transport_connect",
                retryable=True,
            )
    return MarketFailure(
        failure_stage=stage,
        failure_class=default_class,
        retryable=default_retryable,
    )


def legacy_failure_quality_issues(failure: MarketFailure) -> list[str]:
    if failure.failure_class in _PROVIDER_FAILURE_CLASSES:
        return ["provider_error"]
    return ["market_refresh_failed"]


def public_failure_message(failure: MarketFailure) -> str:
    if failure.failure_class in _PROVIDER_FAILURE_CLASSES:
        return "provider request failed"
    if failure.failure_stage == "publish":
        return "market publication failed"
    if failure.failure_stage in {"normalize", "validate"}:
        return "market data validation failed"
    return "market refresh failed"
