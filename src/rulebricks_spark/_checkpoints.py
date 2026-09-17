"""Caller-owned Delta checkpoints for explicitly declared plain Context state.

No server transaction or recovery store participates. Each accepted window is a
quiescent prefix. Failed attempts are abandoned and reconstructed under fresh
physical instance identities. A Delta control row fences competing drivers.
"""

from __future__ import annotations

import json
import re
import uuid
from dataclasses import asdict, dataclass
from functools import reduce
from urllib.parse import quote

import httpx
from pyspark.sql import DataFrame, SparkSession, Window, functions
from pyspark.sql.types import StringType

from ._apply import CONTRACT_METADATA_KEY, apply_dataframe, literal_column
from ._payload import context_instance_key
from ._schema import _data_type_from_json
from ._transport import _split_request_options
from .exceptions import ExecutionError
from .responses import ResponseOptions

_POSITION = "__rb_checkpoint_position"
_WINDOW = "__rb_checkpoint_window"
_PHYSICAL = "__rb_checkpoint_instance"
_STATE = "__rb_checkpoint_state"


@dataclass(frozen=True)
class Checkpoint:
    """Persist ordered flow progress in a caller-owned Delta directory.

    ``context`` declares the one plain Context used by the flow. ``context_key``
    names an existing string input column containing its instance identity;
    it is separate from the business/state-owner ``key``. Every Context step,
    including subflows, must use that input and have no other stateful effects.

    ``events_per_key`` is the number of ordered events per state owner between
    checkpoints, independent of HTTP batch size. Existing instances are read
    before execution; no initial state is inferred. Opting in makes apply_flow
    eager and returns saved results. Requires Delta Lake, exclusive writers,
    fixed assets, disabled Spark speculation/task retries, and current-state-only
    decisions without relationships, pending decisions, history or external effects.
    """

    path: str
    context: str
    context_key: str = "context_id"
    events_per_key: int = 100_000

    def __post_init__(self):
        for name in ("path", "context", "context_key"):
            if not isinstance(getattr(self, name), str) or not getattr(self, name).strip():
                raise ValueError(f"Checkpoint {name} must be a non-empty string.")
        if (
            isinstance(self.events_per_key, bool)
            or not isinstance(self.events_per_key, int)
            or self.events_per_key < 1
        ):
            raise ValueError("Checkpoint events_per_key must be a positive integer.")

    def state(self, spark: SparkSession) -> DataFrame:
        """Read a completed run's saved state and its current physical identities.

        Returns the business ``key`` column, ``context_key`` containing the
        physical instance IDs, and ``state`` containing complete state as JSON.
        This reads caller storage only. It is a saved observation, not a live GET.
        Resume an interrupted run before exporting keys for another job.
        """
        _, spec = _control(spark, self.path.rstrip("/") + "/control")
        if not spec.get("complete"):
            raise ExecutionError(
                "Resume or finish this run before exporting its current Context identities."
            )
        key = spec["key"]
        context_key = spec["checkpoint"]["context_key"]
        if "state" in (key, context_key):
            raise ValueError(
                "The state view needs the name 'state'; use distinct business and Context key names."
            )
        identity = spec["definition"]["identity_fact"]

        def instance(raw):
            return json.loads(raw)[identity]

        return (
            spark.read.format("delta")
            .load(spec["state_path"])
            .select(
                literal_column(key),
                functions.udf(instance, StringType())(functions.col(_STATE)).alias(context_key),
                functions.col(_STATE).alias("state"),
            )
        )


def _json(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _delta(spark, path):
    try:
        from delta.tables import DeltaTable
    except ImportError as exc:
        raise ValueError(
            "Checkpoint execution requires Delta Lake configured in this Spark session."
        ) from exc
    return DeltaTable.forPath(spark, path)


def _save(frame, path):
    frame.write.format("delta").mode("errorifexists").save(path)
    return frame.sparkSession.read.format("delta").load(path)


def _control(spark, path):
    rows = spark.read.format("delta").load(path).take(2)
    if len(rows) != 1:
        raise ExecutionError("Invalid checkpoint control table; expected one run.")
    return rows[0].owner, json.loads(rows[0].data)


def _owned(spark, path, owner):
    current, data = _control(spark, path)
    if current != owner:
        raise ExecutionError(
            "A newer resume attempt owns this checkpoint. This attempt has stopped."
        )
    return data


def _publish(spark, path, owner, data):
    encoded = _json(data)
    _delta(spark, path).update(functions.col("owner") == owner, {"data": functions.lit(encoded)})
    current, published = _control(spark, path)
    if current != owner or published != data:
        raise ExecutionError("Checkpoint ownership changed before this window committed.")


def _require_session(spark):
    from .contexts import _require_ordered_session

    _require_ordered_session(spark)
    try:
        import delta.tables  # noqa: F401
    except ImportError as exc:
        raise ValueError("Checkpoint execution requires Delta Lake.") from exc


def _definition(checkpoint, args):
    """Read the declared Context via existing admin APIs, without SDK type coercion."""
    verify, _ = _split_request_options(args.get("request_options"))
    headers = {
        "x-api-key": args["api_key"],
        **(args.get("request_options") or {}).get("additional_headers", {}),
    }
    with httpx.Client(timeout=args["timeout_seconds"], headers=headers, verify=verify) as client:
        endpoint = args["base_url"].rstrip("/") + "/admin/contexts"
        cursor, matched = None, None
        while True:
            response = client.get(
                endpoint + "/list", params={"limit": 200, **({"cursor": cursor} if cursor else {})}
            )
            response.raise_for_status()
            page = response.json()
            for item in page["data"]:
                if checkpoint.context in (item.get("slug"), item.get("id")):
                    matched = item
                    break
            if matched or not page.get("cursor"):
                break
            if page["cursor"] == cursor:
                raise ValueError("Context listing did not advance its cursor.")
            cursor = page["cursor"]
        if matched is None:
            raise ValueError(f"Checkpoint Context {checkpoint.context!r} was not found.")
        response = client.get(endpoint + "/" + quote(matched["id"], safe=""))
        response.raise_for_status()
        detail = response.json()
    schema = detail["schema"]
    relations = detail.get("relationships") or {}
    if (
        detail.get("auto_execute_decisions")
        or detail.get("bound_rules")
        or detail.get("bound_flows")
        or relations.get("incoming")
        or relations.get("outgoing")
        or schema.get("derived")
        or any(field.get("track_history") for field in schema.get("base", []))
    ):
        raise ValueError(
            "Checkpoint mode supports unbound plain current-state Contexts only; no derived fields, history or relationships."
        )
    identity = detail["identity_fact"]
    if not any(
        f.get("key") == identity and f.get("type") == "string" for f in schema.get("base", [])
    ):
        raise ValueError(
            "Checkpoint Context identity must be a string field for isolated replay keys."
        )
    return {
        name: detail.get(name)
        for name in (
            "id",
            "slug",
            "schema",
            "identity_fact",
            "auto_execute_decisions",
            "history_limit",
            "ttl_seconds",
        )
    }


def _assert_ok(frame, metadata="rulebricks_meta"):
    if (
        frame.filter(
            literal_column(metadata).getField("status").isNull()
            | (literal_column(metadata).getField("status") != "ok")
        )
        .limit(1)
        .count()
    ):
        raise ExecutionError(
            "A window failed or is uncertain and was not checkpointed. Context writes may have committed; resume from the checkpoint path after resolving the failure."
        )


def _physical(keys, context_key, namespace):
    if namespace is None:
        value = literal_column(context_key)
    else:
        value = functions.concat(
            functions.lit(namespace + ":"), functions.sha2(literal_column(context_key), 256)
        )
    return keys.withColumn(_PHYSICAL, value)


def _capture(keys, namespace, spec, args, destination):
    from . import contexts

    keys = _physical(keys, spec["checkpoint"]["context_key"], namespace)
    observed = _save(
        contexts.lookup(
            keys,
            spec["definition"]["slug"],
            key=_PHYSICAL,
            execution_partitions=args["execution_partitions"],
            api_key=args["api_key"],
            base_url=args["base_url"],
            timeout_seconds=args["timeout_seconds"],
            request_options=args.get("request_options"),
        ),
        destination + "/observations",
    )
    _assert_ok(observed)
    state = observed.select(
        spec["key"],
        spec["checkpoint"]["context_key"],
        functions.get_json_object("rulebricks_result", "$.state").alias(_STATE),
    )
    if state.filter(functions.col(_STATE).isNull()).limit(1).count():
        raise ExecutionError(
            "A complete existing Context state is required before checkpoint execution."
        )
    return _save(state, destination + "/state")


def _seed(state, namespace, spec, args, destination):
    identity = spec["definition"]["identity_fact"]
    context_key = spec["checkpoint"]["context_key"]

    def payload(raw, original):
        values = json.loads(raw)
        values[identity] = context_instance_key(namespace, original)
        return _json(values)

    prepared = state.withColumn(
        "__rb_checkpoint_payload",
        functions.udf(payload, StringType())(functions.col(_STATE), literal_column(context_key)),
    )
    receipt = apply_dataframe(
        prepared,
        asset_type="context",
        slug=spec["definition"]["slug"],
        api_key=args["api_key"],
        base_url=args["base_url"],
        version="latest",
        input_columns=None,
        payload_column="__rb_checkpoint_payload",
        input_mapping=None,
        correlation_id_path=identity,
        execution_partitions=args["execution_partitions"],
        max_batch_bytes=args["max_batch_bytes"],
        max_batch_rows=args["max_batch_rows"],
        max_retries=0,
        timeout_seconds=args["timeout_seconds"],
        output_schema=None,
        result_column="rulebricks_result",
        metadata_column="rulebricks_meta",
        diagnostics=True,
        on_error="continue",
        isolate_failures=False,
        context_operation="ingest",
        context_include="state,positions,status,executed",
        request_options=args.get("request_options"),
    )
    receipt = _save(receipt, destination)
    _assert_ok(receipt)
    if receipt.count() != state.count():
        raise ExecutionError("Incomplete checkpoint restore receipts; abandon this attempt.")


def _execute(frame, namespace, spec, args):
    columns = spec["source_columns"]
    ordered = (
        frame.select(*columns)
        .repartition(args["execution_partitions"], literal_column(spec["key"]))
        .sortWithinPartitions(
            literal_column(spec["key"]), *[literal_column(name) for name in spec["order_by"]]
        )
    )
    result = apply_dataframe(
        ordered,
        asset_type="flow",
        slug=spec["flow"],
        version=args["version"],
        api_key=args["api_key"],
        base_url=args["base_url"],
        input_columns=args.get("input_columns") or columns,
        input_mapping=args.get("input_mapping"),
        payload_column=None,
        correlation_id_path=args.get("correlation_id_path"),
        execution_partitions=None,
        max_batch_bytes=args["max_batch_bytes"],
        max_batch_rows=args["max_batch_rows"],
        max_retries=0,
        timeout_seconds=args["timeout_seconds"],
        output_schema=args.get("output_schema"),
        result_column=args["result_column"],
        metadata_column=args["metadata_column"],
        diagnostics=True,
        on_error="continue",
        isolate_failures=False,
        response_options=args.get("response_options"),
        request_options=args.get("request_options"),
        state_key=spec["key"],
        context_key=spec["checkpoint"]["context_key"],
        context_namespace=namespace,
        contract=spec.get("execution_contract"),
    )
    return result


def _results(spark: SparkSession, paths: list[str]) -> DataFrame:
    frames = [spark.read.format("delta").load(path) for path in paths]
    return reduce(lambda left, right: left.unionByName(right), frames)


def _continue(spark, control_path, owner, spec, args, *, restore) -> DataFrame:
    source = spark.read.format("delta").load(spec["source_path"])
    keys = source.select(spec["key"], spec["checkpoint"]["context_key"]).distinct()
    attempt_path = spec["checkpoint"]["path"].rstrip("/") + "/attempts/" + owner
    namespace = owner if restore else None
    if spec["state_path"] is None:
        _owned(spark, control_path, owner)
        _capture(keys, None, spec, args, attempt_path + "/initial")
        spec["state_path"] = attempt_path + "/initial/state"
        _publish(spark, control_path, owner, spec)
    if restore:
        _owned(spark, control_path, owner)
        state = spark.read.format("delta").load(spec["state_path"])
        _seed(state, namespace, spec, args, attempt_path + "/restore")
        restored = _capture(keys, namespace, spec, args, attempt_path + "/restored")
        identity = spec["definition"]["identity_fact"]

        def without_identity(raw):
            values = json.loads(raw)
            values.pop(identity, None)
            return _json(values)

        normalize = functions.udf(without_identity, StringType())
        expected = state.withColumn(_STATE, normalize(functions.col(_STATE)))
        actual = restored.withColumn(_STATE, normalize(functions.col(_STATE)))
        if (
            expected.exceptAll(actual).limit(1).count()
            or actual.exceptAll(expected).limit(1).count()
        ):
            raise ExecutionError(
                "Restored Context state did not match the checkpoint; abandon this attempt."
            )
        spec = {**spec, "state_path": attempt_path + "/restored/state"}
    last_window = source.agg(functions.max(_WINDOW)).first()[0]
    for number in range(spec["window"] + 1, (last_window + 1) if last_window is not None else 0):
        _owned(spark, control_path, owner)
        window_path = attempt_path + f"/windows/{number}"
        window = source.filter(functions.col(_WINDOW) == number)
        result = _save(_execute(window, namespace, spec, args), window_path + "/results")
        _assert_ok(result, args["metadata_column"])
        if result.count() != window.count():
            raise ExecutionError("Incomplete checkpoint window results.")
        _owned(spark, control_path, owner)
        _capture(keys, namespace, spec, args, window_path)
        spec = {
            **spec,
            "window": number,
            "state_path": window_path + "/state",
            "results": spec["results"] + [window_path + "/results"],
        }
        _publish(spark, control_path, owner, spec)
    if not spec["results"]:
        empty_path = attempt_path + "/empty-results"
        _save(_execute(source.limit(0), namespace, spec, args), empty_path)
        spec = {**spec, "results": [empty_path]}
    spec = {**spec, "complete": True}
    _publish(spark, control_path, owner, spec)
    return _results(spark, spec["results"])


def run_checkpointed(df, flow, *, checkpoint, key, order_by, **args) -> DataFrame:
    if not isinstance(checkpoint, Checkpoint):
        raise TypeError("checkpoint must be contexts.Checkpoint.")
    spark = df.sparkSession
    _require_session(spark)
    if not re.fullmatch(r"[1-9][0-9]*", args["version"]):
        raise ValueError(
            "Checkpoint execution requires an explicit published numeric flow version."
        )
    context_key = checkpoint.context_key
    if (
        context_key == key
        or context_key not in df.columns
        or not isinstance(df.schema[context_key].dataType, StringType)
    ):
        raise ValueError(
            "Checkpoint context_key must be a separate existing string column containing the initial Context instance ID."
        )
    if any(name in df.columns for name in (_POSITION, _WINDOW, _PHYSICAL, _STATE)):
        raise ValueError("Source columns collide with private checkpoint storage fields.")
    if args.get("input_columns") is not None and context_key not in args["input_columns"]:
        raise ValueError("Include checkpoint.context_key in input_columns.")
    definition = _definition(checkpoint, args)
    owner = str(uuid.uuid4())
    root = checkpoint.path.rstrip("/")
    serial_args = {
        name: value
        for name, value in args.items()
        if name not in ("api_key", "request_options", "response_options", "output_schema")
    }
    serial_args["output_schema"] = (
        args["output_schema"].jsonValue() if args.get("output_schema") is not None else None
    )
    serial_args["responses"] = (args.get("response_options") or ResponseOptions()).contract()
    serial_args["request_options_required"] = bool(args.get("request_options"))
    spec = {
        "format": 1,
        "flow": flow,
        "key": key,
        "order_by": order_by,
        "checkpoint": asdict(checkpoint),
        "definition": definition,
        "args": serial_args,
        "source_columns": df.columns,
        "source_path": root + "/source",
        "state_path": None,
        "window": -1,
        "results": [],
        "complete": False,
    }
    draft = _execute(df.limit(0), None, spec, args)
    contract = dict(draft.schema[args["metadata_column"]].metadata[CONTRACT_METADATA_KEY])
    contract["stateful_checkpoint"] = asdict(checkpoint)
    contract["order_by"] = order_by
    spec["execution_contract"] = contract
    prepared = False
    try:
        # Validate the exact immutable inputs that will execute, not repeated
        # evaluations of a potentially changing caller DataFrame.
        df = _save(df, root + "/input")
        if (
            df.filter(
                reduce(
                    lambda left, right: left | right,
                    [literal_column(name).isNull() for name in [key, context_key, *order_by]],
                )
            )
            .limit(1)
            .count()
        ):
            raise ValueError("Checkpoint keys and ordering values must not be null.")
        if df.groupBy(key, *order_by).count().filter("count != 1").limit(1).count():
            raise ValueError("Checkpoint ordering must be unique within each state owner.")
        keys = df.select(key, context_key).distinct()
        if (
            keys.groupBy(key).count().filter("count != 1").limit(1).count()
            or keys.groupBy(context_key).count().filter("count != 1").limit(1).count()
        ):
            raise ValueError(
                "Each checkpoint state owner must map to exactly one distinct Context instance."
            )
        planned = df.withColumn(
            _POSITION,
            functions.row_number().over(
                Window.partitionBy(key).orderBy(*[literal_column(name) for name in order_by])
            ),
        ).withColumn(
            _WINDOW,
            functions.floor((functions.col(_POSITION) - 1) / checkpoint.events_per_key).cast(
                "long"
            ),
        )
        _save(planned, spec["source_path"])
        _save(
            spark.createDataFrame([(owner, _json(spec))], "owner string, data string"),
            root + "/control",
        )
        prepared = True
        return _continue(spark, root + "/control", owner, spec, args, restore=False)
    except Exception as exc:
        if not prepared:
            raise ExecutionError(
                f"Checkpoint preparation failed before flow execution. Fix the input or storage problem and use a new checkpoint path. {exc}"
            ) from exc
        raise ExecutionError(
            f"Checkpointed execution stopped. Resolve the failure, then call resume({checkpoint.path!r}). {exc}"
        ) from exc


def resume_checkpoint(
    path, *, api_key, base_url, response_options=None, request_options=None
) -> DataFrame:
    spark = SparkSession.getActiveSession()
    if spark is None:
        raise ValueError("resume(checkpoint_path) requires an active Spark session.")
    control_path = path.rstrip("/") + "/control"
    _, spec = _control(spark, control_path)
    if spec.get("format") != 1 or spec["args"]["base_url"].rstrip("/") != base_url.rstrip("/"):
        raise ValueError("Checkpoint format or service URL does not match this run.")
    if spec["complete"]:
        return _results(spark, spec["results"])
    _require_session(spark)
    saved = spec["args"]
    response_options = response_options or ResponseOptions(
        **{
            key: value
            for key, value in saved["responses"].items()
            if key not in ("error_handler", "transform")
        }
    )
    if response_options.contract() != saved["responses"]:
        raise ValueError("Supply the original checkpoint ResponseOptions callbacks/settings.")
    if saved["request_options_required"] and not request_options:
        raise ValueError("Supply the original checkpoint request_options.")
    args = {
        name: value
        for name, value in saved.items()
        if name not in ("responses", "request_options_required")
    }
    args.update(
        api_key=api_key,
        base_url=base_url,
        response_options=response_options,
        request_options=request_options,
        output_schema=_data_type_from_json(saved["output_schema"])
        if saved["output_schema"]
        else None,
    )
    if _definition(Checkpoint(**spec["checkpoint"]), args) != spec["definition"]:
        raise ValueError(
            "Checkpoint Context definition changed; restore requires the original definition."
        )
    owner = str(uuid.uuid4())
    _delta(spark, control_path).update(set={"owner": functions.lit(owner)})
    spec = _owned(spark, control_path, owner)
    try:
        return _continue(spark, control_path, owner, spec, args, restore=True)
    except Exception as exc:
        raise ExecutionError(
            f"Checkpoint resume stopped. Resolve the failure and resume {path!r} again. {exc}"
        ) from exc
