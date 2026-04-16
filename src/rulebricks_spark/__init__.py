"""Apache Spark integration for Rulebricks.

Apply Rulebricks decision rules to Spark DataFrames at scale, with automatic
bulk batching, intra-partition concurrency, response-schema inference, and
per-row error isolation.

Example
-------
>>> from rulebricks_spark import solve
>>> scored = solve(
...     spark.table("breach_reports"),
...     rule="breach-triage",
...     api_key=dbutils.secrets.get("rulebricks", "api_key"),
... )
>>> scored.display()

See ``help(solve)`` for all options, including ``input_mapping``,
``output_schema``, ``on_conflict``, ``batch_size``, and concurrency knobs.
"""

from rulebricks_spark._version import __version__
from rulebricks_spark.exceptions import (
    RuleExecutionError,
    RulebricksSparkError,
    SchemaConflictError,
    SchemaInferenceError,
)
from rulebricks_spark.solve import solve

__all__ = [
    "solve",
    "RulebricksSparkError",
    "SchemaConflictError",
    "SchemaInferenceError",
    "RuleExecutionError",
    "__version__",
]
