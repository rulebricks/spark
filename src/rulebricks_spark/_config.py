"""Picklable configuration passed from the Spark driver to executors."""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from pyspark.sql.types import DataType

from .responses import ResponseOptions


@dataclass
class RunConfig:
    """Configuration shared by flow and rule partition workers.

    SDK clients are created on executors. Callbacks must be Spark-serializable.
    """

    asset_type: str
    slug: str
    version: str
    api_key: str = field(repr=False)
    base_url: str
    input_columns: Optional[List[str]] = None
    payload_column: Optional[str] = None
    input_mapping: Dict[str, str] = field(default_factory=dict)
    correlation_id_path: Optional[str] = None
    max_batch_bytes: int = 5 * 1024 * 1024
    max_batch_rows: Optional[int] = 1000
    max_retries: int = 3
    isolate_failures: bool = True
    timeout_seconds: float = 300.0
    on_error: str = "continue"
    output_schema: Optional[DataType] = None
    responses: ResponseOptions = field(default_factory=ResponseOptions)
    request_options: Dict[str, Any] = field(default_factory=dict, repr=False)
    operation_id: str = ""
    diagnostics: bool = False
    context_operation: Optional[str] = None
    context_include: Optional[str] = None
    state_key: Optional[str] = None
    context_key: Optional[str] = None
    context_namespace: Optional[str] = None
    spark_timezone: Optional[str] = None
