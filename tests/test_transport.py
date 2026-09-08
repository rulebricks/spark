"""SDK response fidelity, error classification, and request options."""

import datetime

import httpx
import pytest
from rulebricks.core.api_error import ApiError

from rulebricks_spark import _transport
from rulebricks_spark._transport import classify_exception, run_attempt
from tests.conftest import ROOT_ID


def test_context_timeout_is_unknown_and_not_retried(config, sdk):
    from rulebricks_spark._engine import _execute_chunk, _make_record

    config.asset_type = "context"
    config.context_operation = "ingest"
    config.correlation_id_path = "id"
    config.max_retries = 3

    def timeout(request):
        raise httpx.ReadTimeout("response lost", request=request)

    sdk.handler = timeout
    outcomes = _execute_chunk([_make_record({"id": "a"}, config)], sdk.client, config)
    assert len(sdk.requests) == 1
    assert outcomes[0].metadata["_rb_context_write_status"] == "unknown"
    assert outcomes[0].metadata["_rb_error_type"] == "context_write"
    assert outcomes[0].metadata["_rb_attempts"] == 1


@pytest.mark.parametrize("asset_type", ["flow", "rule"])
def test_sdk_preserves_arbitrary_json_and_trace_headers(config, sdk, asset_type):
    config.asset_type = asset_type
    body = [
        {"id": 9007199254740993, "extra": {"flag": False}},
        {"error": "failed"},
        [1, 2],
        None,
        42,
        False,
        "text",
    ]
    sdk.handler = lambda request: httpx.Response(200, json=body)
    result = run_attempt(sdk.client, config, [{} for _ in body])
    assert result.data == body and type(result.data[0]["id"]) is int
    assert result.execution_id == (ROOT_ID if asset_type == "flow" else None)
    assert result.trace_id == "trace-123" and result.protocol_error is None
    assert result.started_at <= result.completed_at
    assert len(sdk.requests) == 1
    sdk.handler = lambda request: httpx.Response(200, content=b"not JSON")
    result = run_attempt(sdk.client, config, [{}])
    assert result.protocol_error and result.trace_id == "trace-123"


@pytest.mark.parametrize("asset_type", ["flow", "rule"])
def test_sdk_options_disable_hidden_retries(config, sdk, asset_type):
    config.asset_type = asset_type
    config.timeout_seconds = 12.5
    config.request_options = {
        "additional_headers": {"X-Workload": "nightly"},
        "additional_query_parameters": {"region": "west"},
    }
    sdk.handler = lambda request: httpx.Response(503, text="unavailable")
    with pytest.raises(ApiError):
        run_attempt(sdk.client, config, [{}])
    assert len(sdk.requests) == 1
    request = sdk.requests[0]
    assert request.headers["X-Workload"] == "nightly" and request.url.params["region"] == "west"
    assert set(request.extensions["timeout"].values()) == {12.5}
    assert "max_retries" not in config.request_options


@pytest.mark.parametrize(
    "status,body,category,retryable",
    [
        (400, {"error": "bad input"}, "client", False),
        (408, "timeout", "network", True),
        (409, "workers unavailable", "infrastructure", True),
        (429, {"message": "busy"}, "throttled", True),
        (500, "failed", "server", True),
        (503, {"error": "outage"}, "infrastructure", True),
        (504, "gateway timeout", "infrastructure", True),
        (413, "request too large", "payload_too_large", False),
        (413, {"error": "Response too large to deliver"}, "response_too_large", False),
    ],
)
def test_http_failure_categories(config, sdk, status, body, category, retryable):
    sdk.handler = lambda request: httpx.Response(
        status, **({"json": body} if isinstance(body, dict) else {"text": body})
    )
    with pytest.raises(ApiError) as caught:
        run_attempt(sdk.client, config, [{}])
    failure = classify_exception(caught.value)
    assert (failure.http_status, failure.error_type, failure.retryable) == (
        status,
        category,
        retryable,
    )
    assert failure.message == (next(iter(body.values())) if isinstance(body, dict) else body)
    assert failure.trace_id == "trace-123"


@pytest.mark.parametrize(
    "headers,seconds",
    [
        ({"Retry-After": "5"}, 5.0),
        ({"Retry-After": "Sat, 05 Sep 2026 12:00:05 GMT"}, 5.0),
        ({"Retry-After": "-2"}, 0.0),
        ({"Retry-After-Ms": "1500", "Retry-After": "7"}, 1.5),
        ({"Retry-After-Ms": "NaN", "Retry-After": "7"}, 7.0),
        ({"Retry-After": "inf"}, None),
        ({"Retry-After": "invalid"}, None),
    ],
)
def test_retry_headers(headers, seconds, monkeypatch):
    monkeypatch.setattr(
        _transport,
        "_utcnow",
        lambda: datetime.datetime(2026, 9, 5, 12, tzinfo=datetime.timezone.utc),
    )
    failure = classify_exception(ApiError(status_code=429, headers=headers, body="busy"))
    assert failure.retry_after == seconds
