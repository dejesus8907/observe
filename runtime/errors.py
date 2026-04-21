"""Structured runtime error taxonomy.

The execution runtime must not rely on vague exception soup. Retry decisions,
lease recovery, timeout handling, stage failure accounting, and operator-facing
diagnostics all depend on failure semantics being explicit.

This file defines a small but durable hierarchy of runtime exceptions with
machine-readable metadata:
- failure category
- retryability
- permanence
- optional stage/job context
- structured details for logging and persistence

These exceptions are intentionally lightweight. They are designed to cross
worker, dispatcher, and repository boundaries without dragging in transport-
specific dependencies.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Optional

from .models import FailureCategory


@dataclass(slots=True)
class ErrorContext:
    """Structured execution context attached to a runtime error.

    Keeping this context separate from the error message prevents brittle
    string parsing later.
    """

    workflow_id: Optional[str] = None
    job_id: Optional[str] = None
    stage_name: Optional[str] = None
    worker_id: Optional[str] = None
    lease_token: Optional[str] = None
    attempt_number: Optional[int] = None
    details: Dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> Dict[str, Any]:
        """Return a logging/persistence-safe representation."""

        payload: Dict[str, Any] = {
            "workflow_id": self.workflow_id,
            "job_id": self.job_id,
            "stage_name": self.stage_name,
            "worker_id": self.worker_id,
            "lease_token": self.lease_token,
            "attempt_number": self.attempt_number,
            "details": dict(self.details),
        }
        return {key: value for key, value in payload.items() if value is not None}


class RuntimeExecutionError(Exception):
    """Base class for all structured runtime failures.

    Parameters
    ----------
    message:
        Human-readable failure message for operators and logs.
    category:
        Machine-readable failure class used by retry/recovery rules.
    retryable:
        Whether the runtime may retry this failure, subject to policy.
    permanent:
        Whether this failure should be treated as non-recoverable once raised.
        Permanent errors may still be retryable in rare cases only if policy
        and subclass semantics explicitly allow it.
    context:
        Optional structured execution context.
    cause:
        Optional underlying exception to preserve provenance.
    details:
        Optional structured key-value details for logging and persistence.
    """

    def __init__(
        self,
        message: str,
        *,
        category: FailureCategory,
        retryable: bool = False,
        permanent: bool = False,
        context: Optional[ErrorContext] = None,
        cause: Optional[BaseException] = None,
        details: Optional[Dict[str, Any]] = None,
    ) -> None:
        super().__init__(message)
        self.message = message
        self.category = category
        self.retryable = retryable
        self.permanent = permanent
        self.context = context or ErrorContext()
        self.cause = cause
        self.details = dict(details or {})
        if cause is not None:
            self.__cause__ = cause

    def to_record(self) -> Dict[str, Any]:
        """Return a persistence-friendly error record."""

        payload = {
            "error_type": self.__class__.__name__,
            "message": self.message,
            "category": self.category.value,
            "retryable": self.retryable,
            "permanent": self.permanent,
            "context": self.context.as_dict(),
            "details": dict(self.details),
        }
        if self.cause is not None:
            payload["cause_type"] = self.cause.__class__.__name__
            payload["cause_message"] = str(self.cause)
        return payload

    def with_context(self, **updates: Any) -> "RuntimeExecutionError":
        """Return a shallow copy with updated structured context."""

        merged = self.context.as_dict()
        details = dict(merged.get("details", {}))
        if "details" in updates and updates["details"] is not None:
            incoming = updates.pop("details")
            if isinstance(incoming, dict):
                details.update(incoming)
        merged.update({k: v for k, v in updates.items() if v is not None})
        merged["details"] = details
        clone = self.__class__(
            self.message,
            context=ErrorContext(**merged),
            cause=self.cause,
            details=self.details,
        )
        if isinstance(clone, RuntimeExecutionError):
            clone.retryable = self.retryable
            clone.permanent = self.permanent
            clone.category = self.category
        return clone


class InputError(RuntimeExecutionError):
    """Invalid job or stage input.

    This is not retryable. Retrying bad input is operational theater.
    """

    def __init__(self, message: str, **kwargs: Any) -> None:
        super().__init__(
            message,
            category=FailureCategory.INPUT,
            retryable=False,
            permanent=True,
            **kwargs,
        )


class ConfigurationError(RuntimeExecutionError):
    """Invalid or missing runtime configuration."""

    def __init__(self, message: str, **kwargs: Any) -> None:
        super().__init__(
            message,
            category=FailureCategory.CONFIG,
            retryable=False,
            permanent=True,
            **kwargs,
        )


class AuthenticationError(RuntimeExecutionError):
    """Authentication or authorization failure during execution."""

    def __init__(self, message: str, **kwargs: Any) -> None:
        super().__init__(
            message,
            category=FailureCategory.AUTH,
            retryable=False,
            permanent=True,
            **kwargs,
        )


class ConnectorError(RuntimeExecutionError):
    """Failure in device/platform access or data collection."""

    def __init__(
        self,
        message: str,
        *,
        retryable: bool = True,
        permanent: bool = False,
        **kwargs: Any,
    ) -> None:
        super().__init__(
            message,
            category=FailureCategory.CONNECTOR,
            retryable=retryable,
            permanent=permanent,
            **kwargs,
        )


class ParseError(RuntimeExecutionError):
    """Failure parsing collected raw artifacts into structured records."""

    def __init__(
        self,
        message: str,
        *,
        retryable: bool = False,
        permanent: bool = True,
        **kwargs: Any,
    ) -> None:
        super().__init__(
            message,
            category=FailureCategory.PARSE,
            retryable=retryable,
            permanent=permanent,
            **kwargs,
        )


class NormalizationError(RuntimeExecutionError):
    """Failure converting parsed records into canonical models."""

    def __init__(
        self,
        message: str,
        *,
        retryable: bool = False,
        permanent: bool = True,
        **kwargs: Any,
    ) -> None:
        super().__init__(
            message,
            category=FailureCategory.NORMALIZATION,
            retryable=retryable,
            permanent=permanent,
            **kwargs,
        )


class PersistenceError(RuntimeExecutionError):
    """Storage or transaction failure.

    Persistence failures are usually transient infrastructure failures rather
    than bad input, so the default is retryable.
    """

    def __init__(
        self,
        message: str,
        *,
        retryable: bool = True,
        permanent: bool = False,
        **kwargs: Any,
    ) -> None:
        super().__init__(
            message,
            category=FailureCategory.PERSISTENCE,
            retryable=retryable,
            permanent=permanent,
            **kwargs,
        )


class TopologyError(RuntimeExecutionError):
    """Failure during topology construction or conflict resolution."""

    def __init__(
        self,
        message: str,
        *,
        retryable: bool = False,
        permanent: bool = True,
        **kwargs: Any,
    ) -> None:
        super().__init__(
            message,
            category=FailureCategory.TOPOLOGY,
            retryable=retryable,
            permanent=permanent,
            **kwargs,
        )


class ValidationError(RuntimeExecutionError):
    """Failure while comparing intended and actual state."""

    def __init__(
        self,
        message: str,
        *,
        retryable: bool = False,
        permanent: bool = True,
        **kwargs: Any,
    ) -> None:
        super().__init__(
            message,
            category=FailureCategory.VALIDATION,
            retryable=retryable,
            permanent=permanent,
            **kwargs,
        )


class TransientSystemError(RuntimeExecutionError):
    """Retryable internal runtime failure.

    Examples:
    - temporary network partition to the DB
    - worker heartbeat write failure
    - short-lived queue contention
    """

    def __init__(self, message: str, **kwargs: Any) -> None:
        super().__init__(
            message,
            category=FailureCategory.TRANSIENT_SYSTEM,
            retryable=True,
            permanent=False,
            **kwargs,
        )


class InternalRuntimeError(RuntimeExecutionError):
    """Bug or invariant violation inside the runtime.

    This class exists so the runtime can clearly distinguish:
    - operator-caused failures
    - target-system failures
    - our own broken logic

    Do not treat this as retryable by default. Blindly retrying bugs is stupid.
    """

    def __init__(self, message: str, **kwargs: Any) -> None:
        super().__init__(
            message,
            category=FailureCategory.INTERNAL,
            retryable=False,
            permanent=True,
            **kwargs,
        )


class LeaseError(TransientSystemError):
    """Base error for lease ownership failures."""


class LeaseLostError(LeaseError):
    """Raised when a worker loses ownership of a leased job."""


class LeaseExpiredError(LeaseError):
    """Raised when a worker attempts execution with an expired lease."""


class LeaseConflictError(LeaseError):
    """Raised when another worker already owns a conflicting lease."""


class CancellationRequestedError(RuntimeExecutionError):
    """Raised cooperatively when cancellation has been requested.

    Cancellation is not a 'failure category' in the domain sense, so it uses
    the transient system bucket for runtime flow control while still surfacing
    explicit type information to callers.
    """

    def __init__(self, message: str = "job cancellation requested", **kwargs: Any) -> None:
        super().__init__(
            message,
            category=FailureCategory.TRANSIENT_SYSTEM,
            retryable=False,
            permanent=False,
            **kwargs,
        )


class TimeoutError(RuntimeExecutionError):
    """Raised when a job or stage exceeds its configured timeout."""

    def __init__(
        self,
        message: str = "execution timed out",
        *,
        retryable: bool = False,
        permanent: bool = False,
        **kwargs: Any,
    ) -> None:
        super().__init__(
            message,
            category=FailureCategory.TRANSIENT_SYSTEM,
            retryable=retryable,
            permanent=permanent,
            **kwargs,
        )


def classify_exception(
    exc: BaseException,
    *,
    default_message: str = "runtime execution failed",
    context: Optional[ErrorContext] = None,
) -> RuntimeExecutionError:
    """Normalize arbitrary exceptions into the runtime taxonomy.

    This gives the worker and dispatcher one normalization path instead of
    letting every call site invent its own exception handling semantics.
    """

    if isinstance(exc, RuntimeExecutionError):
        return exc if context is None else exc.with_context(**context.as_dict())

    if isinstance(exc, ValueError):
        return InputError(str(exc) or default_message, context=context, cause=exc)

    return InternalRuntimeError(default_message, context=context, cause=exc)
