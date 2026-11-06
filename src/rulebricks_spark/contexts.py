"""Context setup, batch staging, and ordered stateful execution."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, Optional, cast
from urllib.parse import quote

import httpx
from pyspark.sql import DataFrame, Window, functions
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    ByteType,
    DataType,
    DateType,
    IntegerType,
    LongType,
    MapType,
    NumericType,
    ShortType,
    StringType,
    StructType,
    TimestampNTZType,
    TimestampType,
)
from rulebricks import Rulebricks
from rulebricks.core.api_error import ApiError
from rulebricks.core.jsonable_encoder import jsonable_encoder
from rulebricks.core.parse_error import ParsingError
from rulebricks.core.request_options import RequestOptions
from rulebricks.types import ContextSchema, ContextSchemaField

from ._apply import (
    DEFAULT_MAX_BATCH_BYTES,
    DEFAULT_MAX_BATCH_ROWS,
    DEFAULT_TIMEOUT_SECONDS,
    apply_dataframe,
    literal_column,
)
from ._checkpoints import Checkpoint
from ._config import RunConfig
from ._transport import (
    BulkResponse,
    _split_request_options,
    _utcnow,
    classify_exception,
    error_message,
)
from .responses import ResponseOptions

__all__ = ["ensure", "ingest", "lookup", "apply_flow", "Checkpoint"]


def ensure(
    name: str,
    *,
    schema: StructType | ContextSchema,
    key: str,
    api_key: str,
    base_url: str,
    auto_execute_decisions: bool = False,
    ttl_seconds: int = 86400,
    history_limit: int = 100,
    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
    request_options: Optional[Mapping[str, Any]] = None,
) -> str:
    """Create or check a named context on the driver and return its actual slug.

    Spark schemas describe base facts; nullability does not imply requiredness.
    Use the SDK's ``ContextSchema`` for defaults, required or derived facts.
    Existing definitions must contain the requested fields and match identity,
    automatic execution, retention and strict schema handling. They are never edited.

    Run setup from one driver before launching writers. The API has no atomic
    create-if-absent operation; concurrent provisioning can create duplicate names.
    """
    if not isinstance(name, str) or not name.strip():
        raise ValueError("name must be a non-empty context name.")
    name = name.strip()
    if isinstance(schema, StructType):
        fields = []
        for field in schema:
            kind = field.dataType
            if isinstance(kind, StringType):
                fact_type = "string"
            elif isinstance(kind, BooleanType):
                fact_type = "boolean"
            elif isinstance(kind, NumericType):
                fact_type = "number"
            elif isinstance(kind, (DateType, TimestampType, TimestampNTZType)):
                fact_type = "date"
            elif isinstance(kind, ArrayType):
                fact_type = "list"
            elif isinstance(kind, (StructType, MapType)):
                fact_type = "object"
            else:
                raise TypeError(f"Unsupported context fact {field.name!r}: {kind.simpleString()}")
            if "." in field.name or "[]" in field.name:
                raise ValueError("Use ContextSchema for dotted or array fact paths.")
            fields.append(ContextSchemaField(key=field.name, name=field.name, type=fact_type))
        schema = ContextSchema(base=fields)
    if not isinstance(schema, ContextSchema):
        raise TypeError("schema must be a Spark StructType or an SDK ContextSchema.")
    expected = jsonable_encoder(schema.dict(by_alias=True, exclude_unset=True))
    base_fields = expected.get("base", [])
    identities = [field for field in base_fields if field["key"] == key]
    if len(identities) != 1 or identities[0]["type"] not in ("string", "number"):
        raise ValueError("key must name a string or number base fact in schema.")
    if len({field["key"] for field in base_fields}) != len(base_fields):
        raise ValueError("Context schema contains duplicate base facts.")
    verify, options = _split_request_options(request_options)
    if options.get("additional_query_parameters") or options.get("additional_body_parameters"):
        raise ValueError("request_options cannot override context setup fields or filters.")
    options.update(max_retries=0, timeout_in_seconds=timeout_seconds)
    settings: dict[str, Any] = dict(
        identity_fact=key,
        auto_execute_decisions=auto_execute_decisions,
        ttl_seconds=ttl_seconds,
        history_limit=history_limit,
        on_schema_mismatch="reject",
    )
    with httpx.Client(timeout=timeout_seconds, verify=verify) as http_client:
        client = Rulebricks(
            api_key=api_key,
            base_url=base_url,
            httpx_client=http_client,
            max_retries=0,
        )
        contexts = client.assets.contexts
        matches: list[dict[str, Any]] = []
        cursor = None
        while True:
            response = contexts.with_raw_response.list(
                name=name,
                limit=100,
                cursor=cursor,
                request_options=cast(RequestOptions, options),
            )
            try:
                page = response._response.json()
            finally:
                response.close()
            matches.extend(item for item in page["data"] if item["name"] == name)
            if len(matches) > 1:
                raise ValueError(f"Multiple contexts are named {name!r}; use an explicit slug.")
            next_cursor = page.get("cursor")
            if not next_cursor:
                break
            if next_cursor == cursor:
                raise ValueError("Context listing did not advance its pagination cursor.")
            cursor = next_cursor
        if not matches:
            created = contexts.create(
                name=name,
                schema=schema,
                **settings,
                request_options=cast(RequestOptions, options),
            )
            if not created.slug:
                raise ValueError("Context creation returned no slug; check the admin listing.")
            return created.slug
        detail = contexts.get(matches[0]["id"], request_options=cast(RequestOptions, options))
        existing = jsonable_encoder(detail)
        conflicts = [name for name, value in settings.items() if existing.get(name) != value]
        for section in ("base", "derived"):
            actual = {field["key"]: field for field in existing["schema"].get(section) or []}
            for field in expected.get(section) or []:
                wanted = dict(field)
                if section == "base":
                    wanted["key"] = wanted["key"].replace("[]", "")
                    if field["key"] == key:
                        wanted["required"] = True
                        wanted["output_only"] = False
                    else:
                        wanted.setdefault("required", False)
                    wanted.setdefault("output_only", False)
                found = actual.get(wanted["key"], {})
                if any(
                    (
                        bool(found.get(prop))
                        if prop in ("required", "output_only")
                        else found.get(prop)
                    )
                    != value
                    for prop, value in wanted.items()
                    if prop not in ("name", "description")
                ):
                    conflicts.append(f"schema.{section}.{field['key']}")
        if conflicts:
            raise ValueError(
                f"Context {name!r} differs in: {', '.join(conflicts)}. Use a matching definition."
            )
        if not detail.slug:
            raise ValueError("Context definition returned no slug.")
        return detail.slug


def ingest(
    df: DataFrame,
    context: str,
    *,
    key: str,
    api_key: str,
    base_url: str,
    input_columns: Optional[Sequence[str]] = None,
    input_mapping: Optional[Mapping[str, str]] = None,
    order_by: Optional[Sequence[str]] = None,
    execution_partitions: int = 1,
    max_batch_bytes: int = DEFAULT_MAX_BATCH_BYTES,
    max_batch_rows: int = DEFAULT_MAX_BATCH_ROWS,
    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
    include: Optional[str] = None,
    result_column: str = "rulebricks_result",
    metadata_column: str = "rulebricks_meta",
    request_options: Optional[Mapping[str, Any]] = None,
) -> DataFrame:
    """Merge source records into contexts and append a JSON receipt per source row.

    ``key`` is the source column supplying the context's identity fact. Rename
    it with ``input_mapping`` when necessary. Equal keys share a partition;
    ``order_by`` supplies ascending merge order within a key. Ties and concurrent
    jobs have no defined order. A batch evaluates the merged state, not each event.

    The context's configured bindings govern automatic execution. For staging,
    use a context configured without automatic or pending executions.
    Receipts include positions, readiness, and execution summaries. ``include``
    adds API fields such as ``state`` or ``execution_results``.

    No HTTP retries or failure splitting occur. Spark can still replay tasks;
    persist receipts once and reconcile uncertain writes before resubmission.
    """
    _check_source(df, key, execution_partitions, result_column, metadata_column)
    if isinstance(input_columns, (str, bytes)):
        raise TypeError("input_columns must be a sequence of column names.")
    selected = list(input_columns) if input_columns is not None else df.columns
    if key not in selected:
        raise ValueError("key must be included in input_columns.")
    mapping = dict(input_mapping or {})
    if isinstance(order_by, (str, bytes)):
        raise TypeError("order_by must be a sequence of column names.")
    ordering = list(order_by or [])
    missing = sorted(set(ordering) - set(df.columns))
    if missing:
        raise ValueError("Order columns not found: " + ", ".join(missing))
    partitioned = df.repartition(execution_partitions, literal_column(key))
    if ordering:
        partitioned = partitioned.sortWithinPartitions(
            literal_column(key), *[literal_column(name) for name in ordering]
        )
    fields = _include(include, {"positions", "status", "need", "executed", "triggered", "reason"})
    return apply_dataframe(
        partitioned,
        asset_type="context",
        slug=context,
        version="latest",
        input_columns=selected,
        payload_column=None,
        input_mapping=mapping,
        correlation_id_path=mapping.get(key, key),
        execution_partitions=None,
        max_retries=0,
        output_schema=None,
        diagnostics=True,
        on_error="continue",
        isolate_failures=False,
        context_include=fields,
        api_key=api_key,
        base_url=base_url,
        max_batch_bytes=max_batch_bytes,
        max_batch_rows=max_batch_rows,
        timeout_seconds=timeout_seconds,
        result_column=result_column,
        metadata_column=metadata_column,
        request_options=request_options,
        context_operation="ingest",
    )


def apply_flow(
    df: DataFrame,
    flow: str,
    *,
    key: str,
    order_by: Sequence[str],
    api_key: str,
    base_url: str,
    version: str = "latest",
    input_columns: Optional[Sequence[str]] = None,
    input_mapping: Optional[Mapping[str, str]] = None,
    correlation_id_path: Optional[str] = None,
    execution_partitions: int = 1,
    max_batch_bytes: int = DEFAULT_MAX_BATCH_BYTES,
    max_batch_rows: int = 1,
    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
    output_schema: Optional[DataType] = None,
    response_options: Optional[ResponseOptions] = None,
    result_column: str = "rulebricks_result",
    metadata_column: str = "rulebricks_meta",
    request_options: Optional[Mapping[str, Any]] = None,
    checkpoint: Optional[Checkpoint] = None,
) -> DataFrame:
    """Execute a regular flow sequentially within each state key.

    The flow reads and writes state through Context Operation steps. No context
    binding is required. ``key`` identifies independent state owners in the
    source; ``order_by`` must be unique and non-null within each key.

    One event is sent per request by default; ``max_batch_rows`` enables ordered
    bulk execution. Batches interleave keys while retaining each key's order.
    There are no HTTP retries or failure splits. A bulk can have
    later committed effects before an earlier item's error is returned.
    A failed event blocks later events for that key; an uncertain request stops
    the partition. Start Spark with speculation disabled, task.maxFailures=1 and
    stage.maxConsecutiveAttempts=1. Use exclusive writers; repeated actions can
    still repeat effects. Reconcile interrupted runs before resubmitting.

    ``checkpoint=Checkpoint(...)`` instead executes eagerly, saves immutable input,
    window results and plain Context state to caller-owned Delta storage, and
    enables ``resume(checkpoint.path)``. See Checkpoint for its state constraints.
    """
    _check_source(df, key, execution_partitions, result_column, metadata_column)
    _require_ordered_session(df.sparkSession)
    if not order_by or isinstance(order_by, (str, bytes)):
        raise ValueError("order_by must be a non-empty sequence of source columns.")
    missing = sorted(set(order_by) - set(df.columns))
    if missing:
        raise ValueError("Order columns not found: " + ", ".join(missing))
    if checkpoint is not None:
        from ._checkpoints import run_checkpointed

        return run_checkpointed(
            df,
            flow,
            checkpoint=checkpoint,
            key=key,
            order_by=list(order_by),
            api_key=api_key,
            base_url=base_url,
            version=version,
            input_columns=input_columns,
            input_mapping=input_mapping,
            correlation_id_path=correlation_id_path,
            execution_partitions=execution_partitions,
            max_batch_bytes=max_batch_bytes,
            max_batch_rows=max_batch_rows,
            timeout_seconds=timeout_seconds,
            output_schema=output_schema,
            response_options=response_options,
            result_column=result_column,
            metadata_column=metadata_column,
            request_options=request_options,
        )
    rounds = Window.partitionBy(literal_column(key)).orderBy(
        *[literal_column(name) for name in order_by]
    )
    ordered = (
        df.repartition(execution_partitions, literal_column(key))
        .withColumn("_rb_context_round", functions.row_number().over(rounds))
        .sortWithinPartitions("_rb_context_round", literal_column(key))
        .drop("_rb_context_round")
    )
    return apply_dataframe(
        ordered,
        asset_type="flow",
        slug=flow,
        version=version,
        api_key=api_key,
        base_url=base_url,
        input_columns=input_columns,
        payload_column=None,
        input_mapping=input_mapping,
        correlation_id_path=correlation_id_path,
        execution_partitions=None,
        max_batch_bytes=max_batch_bytes,
        max_batch_rows=max_batch_rows,
        max_retries=0,
        timeout_seconds=timeout_seconds,
        output_schema=output_schema,
        result_column=result_column,
        metadata_column=metadata_column,
        diagnostics=True,
        on_error="continue",
        isolate_failures=False,
        response_options=response_options,
        request_options=request_options,
        state_key=key,
    )


def lookup(
    df: DataFrame,
    context: str,
    *,
    key: str,
    api_key: str,
    base_url: str,
    execution_partitions: int = 1,
    max_retries: int = 3,
    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
    include: Optional[str] = None,
    result_column: str = "rulebricks_result",
    metadata_column: str = "rulebricks_meta",
    request_options: Optional[Mapping[str, Any]] = None,
) -> DataFrame:
    """Look up each distinct key and join JSON state and audit metadata to the source.

    GET responses retain base facts in ``state`` and computed facts in ``derived``.
    Missing instances become per-row ``not_found`` errors. Lookups use bounded
    parallelism, but are live reads, not a consistent snapshot across identities.
    Materialize this DataFrame once before downstream reuse.
    """
    _check_source(df, key, execution_partitions, result_column, metadata_column)
    keys = df.select(literal_column(key)).distinct()
    looked_up = apply_dataframe(
        keys,
        asset_type="context",
        slug=context,
        version="latest",
        api_key=api_key,
        base_url=base_url,
        input_columns=[key],
        payload_column=None,
        input_mapping={key: "instance_id"},
        correlation_id_path="instance_id",
        execution_partitions=execution_partitions,
        max_batch_bytes=DEFAULT_MAX_BATCH_BYTES,
        max_batch_rows=1,
        max_retries=max_retries,
        timeout_seconds=timeout_seconds,
        output_schema=None,
        result_column=result_column,
        metadata_column=metadata_column,
        diagnostics=True,
        on_error="continue",
        isolate_failures=False,
        request_options=request_options,
        context_operation="lookup",
        context_include=_include(include, {"state", "derived", "status", "need"}),
    )
    source, state = df.alias("source"), looked_up.alias("context_state")

    def column(frame, name):
        return frame["`" + name.replace("`", "``") + "`"]

    return source.join(state, column(source, key).eqNullSafe(column(state, key)), "left").select(
        *[column(source, name) for name in df.columns],
        column(state, result_column),
        column(state, metadata_column),
    )


def _require_ordered_session(spark):
    conf = spark.sparkContext.getConf()
    if (
        conf.get("spark.speculation", "false").lower() != "false"
        or conf.get("spark.task.maxFailures", "4") != "1"
        or conf.get("spark.stage.maxConsecutiveAttempts", "4") != "1"
    ):
        raise ValueError(
            "Start ordered stateful jobs with spark.speculation=false, "
            "spark.task.maxFailures=1 and spark.stage.maxConsecutiveAttempts=1. "
            "Recover uncertain work through its checkpoint path if configured, "
            "otherwise reconcile state before replaying."
        )


def _check_source(df, key, partitions, result_column, metadata_column):
    if not isinstance(df, DataFrame):
        raise TypeError("df must be a pyspark.sql.DataFrame.")
    if not isinstance(key, str) or key not in df.columns:
        raise ValueError("key must name a source column.")
    if not isinstance(
        df.schema[key].dataType, (StringType, ByteType, ShortType, IntegerType, LongType)
    ):
        raise TypeError("Context key must be a string or integer column.")
    if isinstance(partitions, bool) or not isinstance(partitions, int) or partitions < 1:
        raise ValueError("execution_partitions must be a positive integer.")
    if len(df.columns) != len(set(df.columns)):
        raise ValueError("Source DataFrame contains duplicate column names.")
    if any(name.startswith("_rb_") for name in df.columns):
        raise ValueError("Source columns beginning with _rb_ are reserved.")
    if {result_column, metadata_column} & set(df.columns):
        raise ValueError("Rulebricks output columns already exist in the source.")
    if df.isStreaming:
        raise ValueError("Use context operations inside foreachBatch with a batch DataFrame.")


def _include(value, required):
    if value is not None and not isinstance(value, str):
        raise TypeError("include must be a comma-separated string or None.")
    return ",".join(
        sorted(required | {part.strip() for part in (value or "").split(",") if part.strip()})
    )


def _context_attempt(client, config, payloads, request_options, started_at) -> BulkResponse:
    try:
        if config.context_operation == "ingest":
            response = client.contexts.with_raw_response.bulk_ingest(
                config.slug,
                request=payloads,
                include=config.context_include,
                request_options=request_options,
            )
        else:
            # The generated SDK interpolates path parameters without URL escaping.
            instance = quote(str(payloads[0]["instance_id"]), safe="").replace(".", "%2E")
            response = client.contexts.with_raw_response.get(
                config.slug,
                instance,
                include=config.context_include,
                request_options=request_options,
            )
        try:
            status, headers = response.status_code, response.headers
            try:
                body = response._response.json()
            except ValueError:
                body = response._response.text
        finally:
            response.close()
    except (ApiError, ParsingError, httpx.TransportError) as exc:
        if (
            isinstance(exc, (ApiError, ParsingError))
            and exc.status_code is not None
            and 200 <= exc.status_code < 300
        ):
            body, status, headers = exc.body, exc.status_code, httpx.Headers(exc.headers)
        elif config.context_operation == "lookup":
            if not isinstance(exc, (ApiError, ParsingError)) or exc.status_code != 404:
                raise
            return BulkResponse(
                data=[{"instance_id": str(payloads[0]["instance_id"])}],
                http_status=404,
                execution_id=None,
                trace_id=httpx.Headers(exc.headers).get("x-rulebricks-trace-id"),
                started_at=started_at,
                completed_at=_utcnow(),
                errors={0: ("not_found", "Context instance not found.")},
            )
        else:
            failure = classify_exception(exc)
            body = jsonable_encoder(exc.body) if isinstance(exc, (ApiError, ParsingError)) else None
            failure.message = error_message(exc, body)
            committed = body.get("committed_instance_ids", []) if isinstance(body, dict) else []
            committed = (
                {str(value) for value in committed} if isinstance(committed, list) else set()
            )
            # Neither a timeout nor an absent committed list proves rollback.
            write_status = {
                i: "committed" if str(p.get(config.correlation_id_path)) in committed else "unknown"
                for i, p in enumerate(payloads)
            }
            execution_id = body.get("execution_id") if isinstance(body, dict) else None
            return BulkResponse(
                data=[{"request_error": body, "message": failure.message} for _ in payloads],
                http_status=failure.http_status,
                execution_id=failure.execution_id
                or (execution_id if isinstance(execution_id, str) else None),
                trace_id=failure.trace_id,
                started_at=started_at,
                completed_at=_utcnow(),
                errors={i: ("context_write", failure.message) for i in range(len(payloads))},
                write_status=write_status,
            )
    return _context_response(body, status, headers, config, payloads, started_at)


def _context_response(
    body, status, headers, config: RunConfig, payloads, started_at
) -> BulkResponse:
    count = len(payloads)
    result = BulkResponse(
        data=[],
        http_status=status,
        execution_id=None,
        trace_id=headers.get("x-rulebricks-trace-id")
        or (body.get("trace_id") if isinstance(body, dict) else None),
        started_at=started_at,
        completed_at=_utcnow(),
    )
    if config.context_operation == "lookup":
        if (
            isinstance(body, dict)
            and isinstance(body.get("state"), dict)
            and body.get("status") in ("complete", "pending")
        ):
            result.data = [body]
            return result
        result.data = [{"response": body}]
        result.errors[0] = ("protocol", "Context lookup returned an invalid state response.")
        return result

    try:
        if (
            not isinstance(body, dict)
            or not isinstance(body.get("results"), list)
            or not isinstance(body.get("rejections"), list)
        ):
            raise ValueError("Context batch must contain results and rejections arrays.")
        slots: dict[int, Any] = {}

        def assign(position, item, write_status):
            if type(position) is not int or not 0 <= position < count or position in slots:
                raise ValueError("Context batch contains invalid or overlapping input positions.")
            slots[position] = item
            result.write_status[position] = write_status

        execution_failures = any(
            isinstance(row, dict)
            and any(
                isinstance(entry, dict)
                and entry.get("status") in ("evaluation_error", "infrastructure_error")
                for entry in (row.get("executed") or [])
            )
            for row in body["results"]
        )
        for row in body["results"]:
            if (
                not isinstance(row, dict)
                or not isinstance(row.get("instance_id"), str)
                or not isinstance(row.get("positions"), list)
                or not row["positions"]
                or row.get("status") not in ("complete", "pending")
                or not isinstance(row.get("executed"), list)
            ):
                raise ValueError(
                    "Context result lacks identity, positions, status, or execution outcomes."
                )
            item = dict(row)
            for name in ("execution_degraded", "cascade_rejections", "cascaded"):
                if name in body:
                    item[name] = body[name]
            error = None
            for entry in row["executed"]:
                if not isinstance(entry, dict):
                    raise ValueError("Invalid context execution outcome.")
                state = entry.get("status")
                if state not in (
                    "success",
                    "skipped_already_run",
                    "skipped_in_progress",
                    "evaluation_error",
                    "infrastructure_error",
                ):
                    error = ("protocol", "Unrecognized context execution status.")
                elif state in ("evaluation_error", "infrastructure_error"):
                    error = ("context_execution", str(entry.get("error") or state))
                elif state == "skipped_in_progress":
                    item["status"] = "pending"
            if body.get("cascade_rejections") or any(
                entry.get("error")
                or entry.get("execution_degraded")
                or entry.get("rejected")
                or entry.get("evaluation_errors")
                or entry.get("infrastructure_errors")
                for entry in body.get("cascaded", [])
            ):
                error = (
                    "context_cascade",
                    "Dependent context work is incomplete; inspect the receipt.",
                )
            elif not error and body.get("execution_degraded") and not execution_failures:
                error = ("context_execution", str(body["execution_degraded"]))
            if row.get("reason") == "execution_unavailable" and not error:
                error = ("context_execution", "Context execution is unavailable.")
            for position in row["positions"]:
                assign(position, item, "committed")
                if str(payloads[position].get(config.correlation_id_path)) != row["instance_id"]:
                    raise ValueError(
                        "Returned instance identity does not match the supplied context key."
                    )
                if error:
                    result.errors[position] = error
        for rejection in body["rejections"]:
            if not isinstance(rejection, dict) or not isinstance(rejection.get("error"), str):
                raise ValueError("Invalid context rejection.")
            position = rejection.get("position")
            if type(position) is not int:
                raise ValueError("Context rejection lacks a valid input position.")
            assign(position, rejection, "rejected")
            result.errors[position] = ("context_validation", rejection["error"])
        if len(slots) != count:
            raise ValueError("Context batch did not account for every input position.")
        result.data = [slots[i] for i in range(count)]
    except (ValueError, TypeError, AttributeError) as exc:
        result.data = [{"response": body} for _ in range(count)]
        result.errors = {i: ("protocol", str(exc)) for i in range(count)}
        result.write_status = {i: "unknown" for i in range(count)}
    return result
