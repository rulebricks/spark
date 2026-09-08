"""Response handling options shared by rules and flows."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Callable, Literal, Optional

from pyspark.sql.types import DataType


@dataclass(frozen=True)
class ResponseOptions:
    """Control schema evolution without changing the execution API.

    ``result_format='json'`` skips schema inference and keeps arbitrary JSON,
    including arrays and scalars. ``'auto'`` uses an explicit schema, infers
    published rule schemas, and otherwise returns JSON for flows.

    ``schema_mismatch='error'`` marks incompatible typed values as row errors;
    ``'null'`` substitutes nulls. Additional object properties are allowed.
    ``raw_column`` optionally retains the original item, including errors.

    ``error_handler(item)`` replaces the default per-item error detection and
    returns a message or None. ``transform(item)`` runs on successful items
    before schema conversion. Both run on executors, must be serializable,
    deterministic, and free of external side effects. Exceptions become row
    errors and never trigger another HTTP request.
    """

    result_format: Literal["auto", "json"] = "auto"
    schema_mismatch: Literal["error", "null"] = "error"
    raw_column: Optional[str] = None
    error_handler: Optional[Callable[[Any], Optional[str]]] = None
    transform: Optional[Callable[[Any], Any]] = None

    def __post_init__(self) -> None:
        if self.result_format not in ("auto", "json"):
            raise ValueError("result_format must be 'auto' or 'json'.")
        if self.schema_mismatch not in ("error", "null"):
            raise ValueError("schema_mismatch must be 'error' or 'null'.")
        if self.raw_column is not None and (
            not isinstance(self.raw_column, str) or not self.raw_column
        ):
            raise ValueError("raw_column must be a non-empty string or None.")
        for name in ("error_handler", "transform"):
            value = getattr(self, name)
            if value is not None and not callable(value):
                raise TypeError(f"{name} must be callable or None.")

    def contract(self) -> dict[str, Any]:
        """Only descriptive settings enter Spark metadata, never Python code."""
        return {
            "result_format": self.result_format,
            "schema_mismatch": self.schema_mismatch,
            "raw_column": self.raw_column,
            "error_handler": self.error_handler is not None,
            "transform": self.transform is not None,
        }


def _evaluate(item: Any, schema: Optional[DataType], options: ResponseOptions):
    from ._payload import coerce_result

    try:
        handler = options.error_handler
        error = handler(item) if handler else _item_error(item)
        if error is not None and not isinstance(error, str):
            raise TypeError("error_handler must return a string or None")
        if error is not None:
            return None, ("execution", error)
        value = options.transform(item) if options.transform else item
        json.dumps(value, allow_nan=False)
    except Exception as exc:
        return None, ("response_handler", f"{type(exc).__name__}: {exc}")
    if schema is not None:
        try:
            value = coerce_result(value, schema, strict=options.schema_mismatch == "error")
        except (TypeError, ValueError, OverflowError) as exc:
            return None, ("schema", str(exc))
    return value, None


def _item_error(item: Any) -> Optional[str]:
    if isinstance(item, dict) and set(item) == {"error"}:
        error = item["error"]
        if error is None or error == "":
            return "Rulebricks execution failed."
        return (
            error
            if isinstance(error, str)
            else json.dumps(error, separators=(",", ":"), ensure_ascii=False, allow_nan=False)
        )
    return None
