"""Picklable configuration passed from the Spark driver to executors."""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from pyspark.sql.types import DataType

from .responses import ResponseOptions

DEFAULT_MAX_BATCH_BYTES = 64 * 1024 * 1024


class _DefaultBatchRows(int):
    """Preserve the legacy default while distinguishing an explicit batch limit."""


DEFAULT_MAX_BATCH_ROWS = _DefaultBatchRows(1000)


def initial_batch_rows(max_batch_rows: Optional[int], row_budget: int) -> int:
    """Form the request plan before choosing its Spark producers."""
    rows = 4 if isinstance(max_batch_rows, _DefaultBatchRows) else (max_batch_rows or row_budget)
    return min(rows, row_budget)


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
    max_batch_bytes: int = DEFAULT_MAX_BATCH_BYTES
    max_batch_rows: Optional[int] = DEFAULT_MAX_BATCH_ROWS
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
    max_inflight_rows: Optional[int] = None
    execution_partitions: Optional[int] = None

    @property
    def tunes_batches(self) -> bool:
        return self.max_inflight_rows is not None and isinstance(
            self.max_batch_rows, _DefaultBatchRows
        )

    def row_budget(self, partition_id: int) -> Optional[int]:
        """The row allowance assigned to one input partition."""
        if self.max_inflight_rows is None:
            return None
        if self.execution_partitions is None or not 0 <= partition_id < self.execution_partitions:
            raise ValueError("Spark partition does not match the row concurrency plan.")
        batch_rows = initial_batch_rows(self.max_batch_rows, self.max_inflight_rows)
        full_requests, remainder = divmod(self.max_inflight_rows, batch_rows)
        if self.execution_partitions <= full_requests:
            requests, extra = divmod(full_requests, self.execution_partitions)
            # Keep full batches intact; put any partial slot on a task
            # with fewer full requests. The total remains exactly the budget.
            return (requests + int(partition_id < extra)) * batch_rows + (
                remainder if partition_id == extra else 0
            )
        # An explicit producer override can require smaller batches so
        # every input partition still has a positive allowance.
        rows, extra = divmod(self.max_inflight_rows, self.execution_partitions)
        return rows + int(partition_id < extra)

    def request_slots(self, partition_id: int) -> int:
        """Divide the planned budget without rounding up every partition."""
        rows = self.row_budget(partition_id)
        if rows is not None:
            # A large row budget must not create one thread per row.
            return min(rows, 32)
        return 1
