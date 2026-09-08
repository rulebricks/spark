# Result and metadata fields

Every transformation retains its source columns. The default result column is
`rulebricks_result`, and the default metadata column is `rulebricks_meta`. Their
names can be changed when necessary to avoid collisions with the source.

## Business result

Rule results are typed by default using the selected version's output schema.
Flow results default to compact JSON strings. Explicit output schemas and
`ResponseOptions` can change these choices. A failed rule or flow item has a null
business result, while a valid JSON null is retained as the string `"null"` in
JSON mode. Optional raw retention keeps the original decoded item even when
interpretation fails.

Context ingestion returns JSON receipts rather than business decisions, including
receipts with partial failure. Context lookup returns JSON with `state` and
`derived` kept separate. Ordered `contexts.apply_flow()` returns ordinary flow
results, so it cannot establish which internal writes occurred during a failed flow.

## Execution metadata

| Field | Type | Meaning |
| --- | --- | --- |
| `operation_id` | string | Identity of this Spark transformation |
| `status` | string | `ok`, `error`, or `pending` for context receipts |
| `error` | struct, nullable | Failure type, message, and HTTP status |
| `execution_id` | string, nullable | Flow execution ID returned by the server |
| `item_execution_id` | string, nullable | Per-item flow ID derived from the bulk execution ID and position |
| `trace_id` | string, nullable | Backend trace ID when supplied by HPS |
| `correlation_id` | string, nullable | Value selected from the prepared payload |
| `attempts` | integer | Number of submitted attempts; zero for locally rejected or blocked records |
| `started_at`, `completed_at` | timestamp | UTC instants describing processing |
| `asset_type` | string | `rule`, `flow`, or `context` |
| `slug` | string | Executed asset or context identifier |
| `version` | string, nullable | Requested selector; null for context operations |

`error` contains `type`, `message`, and nullable `http_status`. IDs remain null
when the server does not provide them. In particular, the local app can return a
flow execution ID without the HPS trace header. Correlation values are audit keys;
they do not enable automatic deduplication.

Context metadata also includes `operation` (`ingest` or `lookup`) and
`write_status`. Its values distinguish `committed`, `rejected`, `unknown`, and
`not_sent`. A committed write can still have a later decision failure, so preserve
both its receipt and error information. Read operations do not assert a write.

## Diagnostics

With `diagnostics=True`, bulk execution adds a `diagnostics` struct containing
`chunk_id`, `chunk_size`, and nullable `attempt_history`. Context operations and
ordered stateful execution include diagnostics automatically. Attempt history is an
array of structs describing earlier errors, their timing, HTTP statuses, and available IDs.
Enable this additional detail when it supports your operating procedure, since it
adds storage per record.

Each attempt-history entry has `at` (an ISO timestamp), `http_status`, `error_type`,
`error`, `trace_id`, `execution_id`, and `chunk_id`. The final outcome remains in
the enclosing metadata; the history preserves earlier failures that led to it.

The execution contract is stored in Spark field metadata under
`rulebricks_contract`. It describes the selected asset, payload preparation, result
format, and recovery requirements without storing credentials, request headers,
or executable callbacks. Table formats differ in how they preserve field metadata;
verify it when using `resume()` with a new sink. One field-metadata contract cannot
represent several appended operations.
