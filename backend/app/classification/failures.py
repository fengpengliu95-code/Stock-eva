from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

ClassificationFailureStage = Literal[
    "login",
    "security_universe",
    "security_basic",
    "industry",
    "hs300",
    "sz50",
    "csi500",
    "validation",
    "publication",
]
ClassificationFailureClass = Literal[
    "deadline",
    "transport",
    "schema",
    "data_quality",
    "conflict",
    "storage",
    "internal",
]


class ClassificationFailure(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    failure_stage: ClassificationFailureStage
    failure_class: ClassificationFailureClass
    elapsed_seconds: float = Field(ge=0)
    provider_request_count: int = Field(ge=0)
    configured_timeout_seconds: float | None = Field(default=None, gt=0)
    configured_max_attempts: int | None = Field(default=None, ge=1)


class ClassificationFailureError(RuntimeError):
    def __init__(self, failure: ClassificationFailure) -> None:
        self.failure = failure
        super().__init__(f"classification {failure.failure_stage} failed ({failure.failure_class})")


class ClassificationProviderError(ClassificationFailureError):
    pass


class ClassificationSyncError(ClassificationFailureError):
    pass
