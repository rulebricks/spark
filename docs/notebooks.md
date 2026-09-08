# Notebook walkthroughs

Each notebook develops one complete workflow, beginning with the problem it solves
and ending with a saved result that can be inspected independently of execution.
Start with rules and flows to learn the common pattern, then choose streaming,
staging, or stateful processing when your source and decision logic need them.

The setup cells contain the job's URL, asset identifier, version, and storage
paths. Credentials remain in a secret manager or `RULEBRICKS_API_KEY`. Example
asset contracts are described in each notebook so you can publish an isolated
rule or flow with the expected behavior before running it.

| Walkthrough | What you learn | Download |
| --- | --- | --- |
| [Bulk rules](_notebooks/01-bulk-rules.ipynb) | Apply a versioned policy and use typed results | {download}`Notebook <../examples/01-bulk-rules.ipynb>` |
| [Bulk flows](_notebooks/02-bulk-flows.ipynb) | Process independent records in bounded requests | {download}`Notebook <../examples/02-bulk-flows.ipynb>` |
| [Streaming](_notebooks/03-streaming.ipynb) | Drain a backlog and restart from a checkpoint | {download}`Notebook <../examples/03-streaming.ipynb>` |
| [Staging data](_notebooks/04-staging-data.ipynb) | Combine inputs arriving in separate phases | {download}`Notebook <../examples/04-staging-data.ipynb>` |
| [Stateful processing](_notebooks/05-stateful-processing.ipynb) | Process ordered events against shared account state | {download}`Notebook <../examples/05-stateful-processing.ipynb>` |
| [Fault tolerance](_notebooks/06-fault-tolerance.ipynb) | Resume a stateful run using caller-owned Delta checkpoints | {download}`Notebook <../examples/06-fault-tolerance.ipynb>` |

The walkthroughs use Spark 3.5 with Delta Lake and a writable location shared by
the executors. Local paths in the setup cells work for local Spark; replace them
when using a cluster. The documentation build renders the notebooks without
executing them, so reading this site never makes Rulebricks requests.

```{toctree}
:hidden:

_notebooks/01-bulk-rules
_notebooks/02-bulk-flows
_notebooks/03-streaming
_notebooks/04-staging-data
_notebooks/05-stateful-processing
_notebooks/06-fault-tolerance
```
