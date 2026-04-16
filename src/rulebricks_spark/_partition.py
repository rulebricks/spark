"""Per-partition worker executed on Spark executors.

Each Spark partition invokes :func:`make_partition_worker`'s returned closure
with an iterator of pandas ``DataFrame`` chunks. The worker:

1. Converts each chunk's rows into ``bulk_solve`` request payloads, honoring
   ``input_mapping``.
2. Splits the chunk into batches of ``batch_size`` and issues them
   concurrently via a thread pool (``max_concurrent_requests_per_partition``).
3. Retries retryable failures with exponential backoff.
4. Coerces rule output values into the declared Spark types.
5. Emits a pandas ``DataFrame`` matching the declared output schema, including
   a ``_rb_error`` column that is null on success and carries the error string
   on failure when ``on_error='continue'``.

Imports of ``rulebricks``, ``pandas``, and ``numpy`` happen inside the worker
closure so the module is importable on the driver even if workers are
configured differently.
"""

from __future__ import annotations

import json
import math
import time
from typing import Any, Callable, Dict, Iterator, List, Optional, Tuple

from ._config import SolveConfig
from .exceptions import RuleExecutionError


def make_partition_worker(config: SolveConfig) -> Callable:
    """Build a ``mapInPandas`` worker function closed over ``config``.

    The returned callable takes ``Iterator[pd.DataFrame]`` and yields
    ``pd.DataFrame`` chunks whose columns match the output schema.
    """

    def worker(pdf_iter):
        import pandas as pd  # noqa: F401  (imported for its side effect of availability check)

        client = _make_executor_client(config)

        for pdf in pdf_iter:
            if len(pdf) == 0:
                yield _empty_output(pdf, config)
                continue
            yield _process_partition(pdf, client, config)

    return worker


# --- partition processing ----------------------------------------------------


def _process_partition(pdf, client, config: SolveConfig):
    """Run bulk_solve over one pandas DataFrame chunk with concurrency + retries."""
    from concurrent.futures import ThreadPoolExecutor

    n = len(pdf)
    payloads = _build_payloads(pdf, config)

    batches: List[Tuple[int, List[Dict[str, Any]]]] = [
        (i, payloads[i : i + config.batch_size]) for i in range(0, n, config.batch_size)
    ]

    def run_batch(batch_tuple: Tuple[int, List[Dict[str, Any]]]):
        start_idx, batch_payloads = batch_tuple
        try:
            response = _bulk_solve_with_retry(client, config, batch_payloads)
            return start_idx, _normalize_response(response), None
        except Exception as e:
            return start_idx, None, _format_error(e)

    if config.max_concurrent > 1 and len(batches) > 1:
        with ThreadPoolExecutor(max_workers=config.max_concurrent) as pool:
            batch_results = list(pool.map(run_batch, batches))
    else:
        batch_results = [run_batch(b) for b in batches]

    # Initialize per-row output columns.
    output_data: Dict[str, List[Any]] = {
        name: [None] * n for name, _ in config.output_columns
    }
    errors: List[Optional[str]] = [None] * n

    for start_idx, response_list, err in batch_results:
        batch_len = min(config.batch_size, n - start_idx)

        if err is not None:
            if config.on_error == "raise":
                raise RuleExecutionError(
                    f"bulk_solve failed on partition batch at row {start_idx}: {err}"
                )
            for j in range(batch_len):
                errors[start_idx + j] = err
            continue

        for j in range(batch_len):
            if j < len(response_list) and isinstance(response_list[j], dict):
                row_result = response_list[j]
                # Surface rule-reported row errors (some deployments return
                # {"error": "..."} items mixed into the batch response).
                row_error = row_result.get("error") or row_result.get("_error")
                if row_error and config.on_error != "raise":
                    errors[start_idx + j] = str(row_error)
                for name, rb_type in config.output_columns:
                    output_data[name][start_idx + j] = _coerce_output(
                        row_result.get(name), rb_type
                    )
            else:
                errors[start_idx + j] = "missing_response_for_row"

    return _assemble_output(pdf, output_data, errors, config)


# --- payload construction ----------------------------------------------------


def _build_payloads(pdf, config: SolveConfig) -> List[Dict[str, Any]]:
    """Convert a pandas DataFrame into a list of bulk_solve request dicts.

    ``input_mapping`` is applied as ``{dataframe_col: rule_field}`` — DataFrame
    columns without a mapping are passed through using their existing name.
    """
    mapping = config.input_mapping or {}

    # Vectorized rename; pandas is happy with the empty-mapping case.
    renamed = pdf.rename(columns=mapping) if mapping else pdf
    records = renamed.to_dict(orient="records")

    return [{k: _coerce_input(v) for k, v in rec.items()} for rec in records]


def _coerce_input(val: Any) -> Any:
    """Normalize a pandas/numpy scalar into a JSON-serializable Python value."""
    if val is None:
        return None

    if isinstance(val, float):
        if math.isnan(val):
            return None
        return val

    # pandas-specific null types
    try:
        import pandas as pd

        if val is pd.NaT:
            return None
        if isinstance(val, pd.Timestamp):
            return val.isoformat()
    except ImportError:
        pass

    # numpy scalars → native Python
    try:
        import numpy as np

        if isinstance(val, np.generic):
            return val.item()
        if isinstance(val, np.ndarray):
            return val.tolist()
    except ImportError:
        pass

    import datetime

    if isinstance(val, (datetime.datetime, datetime.date)):
        return val.isoformat()

    return val


# --- output coercion ---------------------------------------------------------


def _coerce_output(val: Any, rb_type: str) -> Any:
    """Convert a rule response value into a Spark-compatible Python value.

    The Spark output schema is derived from ``rb_type``; this function
    guarantees the value will round-trip cleanly through Arrow.
    """
    if val is None:
        return None

    if rb_type == "number":
        if isinstance(val, bool):
            return float(val)
        try:
            return float(val)
        except (TypeError, ValueError):
            return None

    if rb_type == "boolean":
        if isinstance(val, bool):
            return val
        if isinstance(val, (int, float)):
            return bool(val)
        if isinstance(val, str):
            s = val.strip().lower()
            if s in ("true", "yes", "1"):
                return True
            if s in ("false", "no", "0"):
                return False
        return None

    if rb_type == "date":
        if isinstance(val, str):
            try:
                import pandas as pd

                return pd.Timestamp(val).to_pydatetime()
            except Exception:
                return None
        import datetime

        if isinstance(val, (datetime.datetime, datetime.date)):
            return val
        return None

    if rb_type == "list_typed":
        # User provided explicit ArrayType — pass through, Spark/Arrow will
        # validate element types on write.
        if isinstance(val, list):
            return val
        if isinstance(val, (tuple, set)):
            return list(val)
        return None

    if rb_type == "struct_typed":
        # User provided explicit StructType — values must already be dicts.
        if isinstance(val, dict):
            return val
        return None

    # "list", "object", "string" — all StringType outputs.
    if isinstance(val, (dict, list, tuple)):
        try:
            return json.dumps(val, default=_json_default)
        except Exception:
            return str(val)
    if isinstance(val, bool):
        return "true" if val else "false"
    if isinstance(val, str):
        return val
    return str(val)


def _json_default(o: Any) -> Any:
    """Fallback serializer for non-JSON-native objects."""
    import datetime

    if isinstance(o, (datetime.datetime, datetime.date)):
        return o.isoformat()
    try:
        import numpy as np

        if isinstance(o, np.generic):
            return o.item()
    except ImportError:
        pass
    return str(o)


# --- response normalization & retries ----------------------------------------


def _normalize_response(response: Any) -> List[Any]:
    """Coerce any bulk_solve return shape into a plain list of dicts."""
    if response is None:
        return []
    if isinstance(response, list):
        return [_to_plain_dict(item) for item in response]

    # Some SDK versions wrap results in an object with .data / .results.
    for attr in ("data", "results", "items"):
        nested = getattr(response, attr, None)
        if isinstance(nested, list):
            return [_to_plain_dict(item) for item in nested]

    # Single-item fallback: wrap in a list.
    d = _to_plain_dict(response)
    return [d] if d else []


def _to_plain_dict(item: Any) -> Dict[str, Any]:
    if isinstance(item, dict):
        return item
    for dump_method in ("model_dump", "dict"):
        fn = getattr(item, dump_method, None)
        if callable(fn):
            try:
                result = fn()
                if isinstance(result, dict):
                    return result
            except Exception:
                continue
    if hasattr(item, "__dict__"):
        return {k: v for k, v in item.__dict__.items() if not k.startswith("_")}
    return {}


def _bulk_solve_with_retry(client, config: SolveConfig, batch: List[Dict[str, Any]]) -> Any:
    """Invoke bulk_solve with exponential backoff on retryable errors."""
    last_exc: Optional[Exception] = None
    for attempt in range(config.max_retries + 1):
        try:
            return client.rules.bulk_solve(slug=config.rule_slug, request=batch)
        except Exception as e:
            last_exc = e
            if attempt < config.max_retries and _is_retryable(e):
                backoff = min(2**attempt, 30)
                time.sleep(backoff)
                continue
            break

    assert last_exc is not None
    raise last_exc


def _is_retryable(exc: Exception) -> bool:
    """Best-effort classification of errors worth retrying."""
    # Check HTTP status code if available
    status = getattr(exc, "status_code", None) or getattr(exc, "status", None)
    if isinstance(status, int):
        return status in (408, 425, 429, 500, 502, 503, 504)

    msg = str(exc).lower()
    markers = (
        "timeout",
        "timed out",
        "connection",
        "reset",
        "temporarily unavailable",
        "429",
        "500",
        "502",
        "503",
        "504",
        "rate limit",
    )
    return any(m in msg for m in markers)


def _format_error(exc: Exception) -> str:
    """Short human-readable error string suitable for the _rb_error column."""
    return f"{type(exc).__name__}: {exc}"


# --- output assembly ---------------------------------------------------------


def _assemble_output(pdf, output_data: Dict[str, List[Any]], errors: List[Optional[str]], config: SolveConfig):
    """Build a pandas DataFrame whose columns match the declared output schema."""
    output_names = {name for name, _ in config.output_columns}

    # Drop input columns that collide with rule outputs (will be replaced).
    preserved_cols = [c for c in pdf.columns if c not in output_names and c != "_rb_error"]
    result = pdf[preserved_cols].copy() if preserved_cols else pdf.iloc[:, :0].copy()

    # Append rule output columns in declaration order.
    for name, _ in config.output_columns:
        result[name] = output_data[name]

    result["_rb_error"] = errors

    # Match the Spark schema column order exactly:
    # preserved input cols (original order), rule outputs, _rb_error.
    ordered = preserved_cols + [name for name, _ in config.output_columns] + ["_rb_error"]
    return result[ordered]


def _empty_output(pdf, config: SolveConfig):
    """Return a zero-row DataFrame with the correct columns for an empty partition."""
    output_names = {name for name, _ in config.output_columns}
    preserved_cols = [c for c in pdf.columns if c not in output_names and c != "_rb_error"]

    result = pdf[preserved_cols].copy()
    for name, _ in config.output_columns:
        result[name] = []
    result["_rb_error"] = []

    ordered = preserved_cols + [name for name, _ in config.output_columns] + ["_rb_error"]
    return result[ordered]


# --- executor-side client ----------------------------------------------------


def _make_executor_client(config: SolveConfig):
    """Instantiate a fresh Rulebricks client on the executor.

    We don't try to serialize a client from the driver because SDK clients may
    hold non-picklable resources (sessions, event loops) and the cost of
    constructing one per partition is negligible.
    """
    # Test hook: use an injected class instead of the real SDK.
    if config._client_class is not None:
        return config._client_class(api_key=config.api_key)

    try:
        from rulebricks import Rulebricks
    except ImportError as e:
        raise ImportError(
            "The `rulebricks` package must be installed on Spark executors. "
            "In Databricks, install it as a cluster library or via "
            "`%pip install rulebricks-spark` in the notebook."
        ) from e

    if config.base_url and config.base_url != "https://rulebricks.com":
        try:
            return Rulebricks(api_key=config.api_key, base_url=config.base_url)
        except TypeError:
            import os

            os.environ.setdefault("RULEBRICKS_API_URL", config.base_url)
    return Rulebricks(api_key=config.api_key)
