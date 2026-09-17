"""Rulebricks SDK transport adapter and error classification."""

from __future__ import annotations

import datetime
import email.utils
import math
import uuid
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import httpx
from rulebricks import Rulebricks
from rulebricks.core.api_error import ApiError
from rulebricks.core.parse_error import ParsingError
from rulebricks.types.error import Error

from ._config import RunConfig


@dataclass
class BulkResponse:
    data: List[Any]
    http_status: Optional[int]
    execution_id: Optional[str]
    trace_id: Optional[str]
    started_at: datetime.datetime
    completed_at: datetime.datetime
    protocol_error: Optional[str] = None
    errors: Dict[int, tuple[str, str]] = field(default_factory=dict)
    write_status: Dict[int, str] = field(default_factory=dict)


@dataclass
class FailureInfo:
    error_type: str
    message: str
    http_status: Optional[int]
    execution_id: Optional[str]
    trace_id: Optional[str]
    retry_after: Optional[float]
    retryable: bool


def _split_request_options(
    request_options: Optional[Mapping[str, Any]],
) -> tuple[bool, Dict[str, Any]]:
    """Separate Spark transport settings from generated SDK request options."""
    if request_options is not None and not isinstance(request_options, Mapping):
        raise TypeError("request_options must be a mapping or None.")
    sdk_options = dict(request_options or {})
    verify = sdk_options.pop("verify", True)
    if not isinstance(verify, bool):
        raise TypeError("request_options.verify must be a bool.")
    return verify, sdk_options


@contextmanager
def sdk_client(config: RunConfig) -> Iterator[Rulebricks]:
    """Own the HTTP connection pool for one Spark task."""
    verify, _ = _split_request_options(config.request_options)
    with httpx.Client(
        timeout=config.timeout_seconds, follow_redirects=True, verify=verify
    ) as http_client:
        yield Rulebricks(
            api_key=config.api_key,
            base_url=config.base_url,
            timeout=config.timeout_seconds,
            max_retries=0,
            httpx_client=http_client,
        )


def run_attempt(client: Any, config: RunConfig, payloads: List[Dict[str, Any]]) -> BulkResponse:
    """Execute one bulk request, preserving its original JSON and trace headers."""

    started_at = _utcnow()
    _, request_options = _split_request_options(config.request_options)
    request_options.update(max_retries=0, timeout_in_seconds=config.timeout_seconds)
    if config.context_operation:
        from .contexts import _context_attempt

        return _context_attempt(client, config, payloads, request_options, started_at)
    try:
        if config.asset_type == "flow":
            response = client.flows.with_raw_response.execute(
                config.slug,
                version=config.version,
                request=payloads,
                request_options=request_options,
            )
        else:
            response = client.rules.with_raw_response.bulk_solve(
                config.slug,
                version=config.version,
                request=payloads,
                request_options=request_options,
            )
    except (ParsingError, ApiError) as exc:
        # Generated response types cannot express every valid per-item shape.
        if exc.status_code is not None and 200 <= exc.status_code < 300:
            return _attempt_result(
                exc.body, exc.status_code, httpx.Headers(exc.headers), config, started_at
            )
        raise

    try:
        # Avoid coercion by the SDK's generated union types.
        return _attempt_result(
            response._response.json(), response.status_code, response.headers, config, started_at
        )
    finally:
        response.close()


def _attempt_result(
    body: Any,
    status: int,
    headers: httpx.Headers,
    config: RunConfig,
    started_at: datetime.datetime,
) -> BulkResponse:
    is_array = isinstance(body, list)
    return BulkResponse(
        data=body if is_array else [],
        http_status=status,
        execution_id=(
            headers.get("x-rulebricks-flow-execution-id") if config.asset_type == "flow" else None
        ),
        trace_id=headers.get("x-rulebricks-trace-id"),
        started_at=started_at,
        completed_at=_utcnow(),
        protocol_error=(
            None
            if is_array
            else f"Rulebricks bulk response must be a JSON array; got {type(body).__name__}."
        ),
    )


def classify_exception(exc: Exception) -> FailureInfo:
    """Classify SDK and httpx exceptions into the stable error taxonomy."""

    status = exc.status_code if isinstance(exc, (ApiError, ParsingError)) else None
    headers = httpx.Headers(exc.headers if isinstance(exc, (ApiError, ParsingError)) else None)
    body = exc.body if isinstance(exc, (ApiError, ParsingError)) else None

    message = error_message(exc, body)
    trace_id = headers.get("x-rulebricks-trace-id")
    if not trace_id and isinstance(body, dict):
        trace_id = str(body["trace_id"]) if body.get("trace_id") else None
    execution_id = headers.get("x-rulebricks-flow-execution-id")

    if isinstance(exc, (httpx.TimeoutException, httpx.TransportError)):
        error_type = "network"
        retryable = True
    elif status == 408:
        error_type = "network"
        retryable = True
    elif status == 409:
        error_type = "infrastructure"
        retryable = True
    elif status is not None and 500 < status < 600:
        error_type = "infrastructure"
        retryable = True
    elif status == 429:
        error_type = "throttled"
        retryable = True
    elif status == 500:
        error_type = "server"
        retryable = True
    elif status == 413:
        lowered = message.lower()
        error_type = (
            "response_too_large"
            if "response too large" in lowered or "response exceeds" in lowered
            else "payload_too_large"
        )
        retryable = False
    elif status is not None and 400 <= status < 500:
        error_type = "client"
        retryable = False
    else:
        error_type = "unknown"
        retryable = False

    return FailureInfo(
        error_type=error_type,
        message=message,
        http_status=status,
        execution_id=execution_id,
        trace_id=trace_id,
        retry_after=_retry_after_from_headers(headers),
        retryable=retryable,
    )


def derive_item_execution_id(root_execution_id: Optional[str], index: int) -> Optional[str]:
    """Derive the UUIDv5 used by HPS for one item in a bulk flow request."""

    if not root_execution_id:
        return None
    try:
        namespace = uuid.UUID(root_execution_id)
    except (TypeError, ValueError, AttributeError):
        return None
    return str(uuid.uuid5(namespace, str(index)))


def parse_retry_after(value: Optional[str]) -> Optional[float]:
    """Parse finite seconds or an HTTP date; timezone-less dates mean UTC."""

    if not value:
        return None
    try:
        seconds = float(value)
    except (TypeError, ValueError, OverflowError):
        pass
    else:
        return max(0.0, seconds) if math.isfinite(seconds) else None
    try:
        target = email.utils.parsedate_to_datetime(value)
        if target.tzinfo is None:
            target = target.replace(tzinfo=datetime.timezone.utc)
        return max(0.0, (target.astimezone(datetime.timezone.utc) - _utcnow()).total_seconds())
    except (TypeError, ValueError, OverflowError):
        return None


def _retry_after_from_headers(headers: httpx.Headers) -> Optional[float]:
    milliseconds = headers.get("retry-after-ms")
    if milliseconds:
        try:
            value = float(milliseconds)
            if math.isfinite(value):
                return max(0.0, value / 1000.0)
        except (TypeError, ValueError, OverflowError):
            pass
    return parse_retry_after(headers.get("retry-after"))


def error_message(exc: Exception, body: Any) -> str:
    if isinstance(body, Error):
        body = body.error
    if isinstance(body, dict):
        for name in ("error", "message", "detail"):
            value = body.get(name)
            if value:
                return str(value)
    if body not in (None, ""):
        return str(body)
    message = str(exc)
    return message or type(exc).__name__


def _utcnow() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc)
