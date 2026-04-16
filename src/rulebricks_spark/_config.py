"""Internal configuration container passed to partition workers.

Must contain only primitives so it serializes cleanly across Spark workers.
"""

from dataclasses import dataclass, field
from typing import Dict, List, Tuple


@dataclass
class SolveConfig:
    """Config bundle sent to each Spark executor.

    All fields must be picklable primitives. No SDK client objects.
    """

    rule_slug: str
    api_key: str
    base_url: str
    batch_size: int
    max_concurrent: int
    input_mapping: Dict[str, str]
    on_missing_input: str
    on_error: str
    timeout_seconds: float
    max_retries: int
    # List of (field_name, rulebricks_type) tuples for the rule's response schema.
    # Rulebricks types: "number" | "string" | "boolean" | "date" | "list" | "object".
    output_columns: List[Tuple[str, str]] = field(default_factory=list)
    input_columns: List[str] = field(default_factory=list)
    # Test hook: if set, used instead of importing rulebricks.Rulebricks on
    # executors.  Must be a picklable class (module-level, not a closure).
    _client_class: object = None
