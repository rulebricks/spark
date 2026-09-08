# Errors and exceptions

Most failures during execution are returned as data so the original record remains
available for investigation. Preparation errors, such as invalid configuration or
an unusable published schema, raise before execution. `on_error="raise"` converts
execution failures into task exceptions when stopping the job is the intended policy.

## Row error categories

| Category | Meaning |
| --- | --- |
| `network` | Transport failure or HTTP 408; server execution may be uncertain |
| `throttled` | HTTP 429 |
| `infrastructure` | HTTP 409 or 501 through 599, including 503 and 504 |
| `server` | HTTP 500 |
| `client` | Other HTTP 4xx or a record rejected during local preparation |
| `payload_too_large` | Request exceeds a size limit |
| `response_too_large` | Response exceeds an enforced limit |
| `protocol` | Response cannot be aligned or interpreted as a bulk response |
| `execution` | A per-item calculation or configured business error |
| `response_handler` | A local response callback failed |
| `schema` | Output value cannot fit the selected Spark type |
| `context_write` | A context write failed or its outcome cannot be established |
| `context_execution` | Context-triggered decision execution failed |
| `context_validation` | Context validation rejected the input |
| `context_blocked` | An earlier failure prevented this record from being sent |
| `context_cascade` | Dependent context work was incomplete |
| `unknown` | An unexpected failure that could not be classified more specifically |
| `not_found` | Requested context instance was absent |

The precise context receipt remains in JSON alongside its classification.
Network, throttling, infrastructure, and server failures are the default recovery
categories. Context writes and ordered stateful events do not use automatic HTTP
retries, and a timeout is not evidence of rollback.

## Raised exceptions

```{eval-rst}
.. autoapiexception:: rulebricks_spark.RulebricksSparkError
```

```{eval-rst}
.. autoapiexception:: rulebricks_spark.SchemaInferenceError
```

```{eval-rst}
.. autoapiexception:: rulebricks_spark.ExecutionError
```

Invalid argument types and values also raise Python `TypeError` and `ValueError`.
A schema lookup can surface `SchemaInferenceError` during driver preparation;
an execution exception inside a task is wrapped by Spark when reported to the driver.
