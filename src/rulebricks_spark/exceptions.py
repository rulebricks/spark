"""Errors raised by Rulebricks Spark."""


class RulebricksSparkError(Exception):
    """Base exception for Rulebricks Spark errors."""


class SchemaInferenceError(RulebricksSparkError):
    """The published rule schema could not be inferred. Supply output_schema."""


class ExecutionError(RulebricksSparkError):
    """Execution failed with on_error='raise'."""
