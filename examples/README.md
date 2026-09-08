# Notebook walkthroughs

Start with a rule applied to a table, then move to flows and choose the more
specialized examples as your pipeline needs them. Each notebook introduces its
use case, explains the data being added, and ends with saved output you can inspect.

| Notebook | Workflow |
| --- | --- |
| [1. Bulk rules](01-bulk-rules.ipynb) | Apply a published rule and use typed decisions |
| [2. Bulk flows](02-bulk-flows.ipynb) | Process 20,000 independent records in bounded requests |
| [3. Streaming](03-streaming.ipynb) | Drain a file backlog and restart from a checkpoint |
| [4. Staging data](04-staging-data.ipynb) | Combine inputs arriving in separate phases before running decisions |
| [5. Stateful processing](05-stateful-processing.ipynb) | Process ordered events against shared account state |
| [6. Fault tolerance](06-fault-tolerance.ipynb) | Retry stateless results and resume checkpointed stateful runs |

Use a Spark 3.5 session with Delta Lake and install the library on the driver and
executors. Set the API URL, published asset, version, and storage paths directly
in each notebook's setup cell. The API key comes from `RULEBRICKS_API_KEY` or your
secret manager. Local paths are provided for local Spark; replace them with shared
storage when running on a cluster.

The example rule or flow must match the small contract described in its notebook.
See [installation](../docs/getting-started.md) for runtime setup and the
[documentation](../docs/index.md) for the complete guides and API reference.
