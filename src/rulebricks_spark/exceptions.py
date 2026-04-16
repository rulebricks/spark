"""Exceptions raised by rulebricks-spark."""


class RulebricksSparkError(Exception):
    """Base exception for all rulebricks-spark errors."""


class SchemaConflictError(RulebricksSparkError):
    """Rule output columns collide with existing DataFrame columns.

    Raised when ``solve()`` is called with ``on_conflict='error'`` and one or
    more rule output field names match input column names.
    """


class SchemaInferenceError(RulebricksSparkError):
    """Could not infer output schema from rule definition.

    Typically raised when the rule slug is not found in the workspace, the API
    key lacks permission, or the rule has no declared response schema. Pass an
    explicit ``output_schema`` argument to ``solve()`` to bypass inference.
    """


class RuleExecutionError(RulebricksSparkError):
    """A ``bulk_solve`` call failed and ``on_error='raise'`` was set."""
