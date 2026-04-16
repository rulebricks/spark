# Breach Triage Example

A canonical end-to-end example of `rulebricks-spark`: batch-score a Delta table
of incident reports against a Rulebricks decision table, then watch the output
change by editing the rule in the UI — no code redeploy.

## What this example covers

- Building a `breach-triage` rule in Rulebricks
- Applying it to 20 hand-authored reports for narrative clarity
- Querying the decision audit log
- Re-running against 100,000 synthetic rows to demonstrate scale

See [`demo.py`](./demo.py) for the full Databricks notebook.

## 1. Build the rule in Rulebricks

In your Rulebricks workspace, create a new rule. Name it whatever makes sense
to your team (`breach-triage` is a good default). Configure its schemas as
follows.

### Request schema (inputs)

| Field | Type | Notes |
|---|---|---|
| `report_id` | string | Pass-through identifier |
| `entity_name` | string | Reporting entity or business unit |
| `incident_type` | string | Enum: `policy_violation`, `disclosure_failure`, `insider_activity`, `fiduciary_breach`, `compliance_gap`, `conflict_of_interest` |
| `financial_impact` | number | In your reporting currency |
| `customer_harm` | boolean | |
| `parties_affected` | number | Integer-valued |
| `days_open` | number | Integer-valued |

### Response schema (outputs)

| Field | Type |
|---|---|
| `priority` | string (`P1` / `P2` / `P3` / `P4`) |
| `reportable` | boolean |
| `risk_score` | number |
| `rationale` | string |

### Suggested decision tiers

Use these as starting conditions — tune to match your organization's
thresholds:

- **P1** when `financial_impact > 5,000,000` OR `incident_type = insider_activity`
- **P2** when `financial_impact > 1,000,000` OR (`customer_harm = true` AND `parties_affected > 1000`)
- **P3** when `financial_impact > 100,000` OR `customer_harm = true`
- **P4** otherwise
- `reportable = true` when priority ∈ {P1, P2} OR (`days_open <= 30` AND `customer_harm = true`)
- `risk_score` as a weighted composite (start simple: `min(100, financial_impact / 200_000 + 20 * customer_harm)`)
- `rationale` as a short templated string referencing which tier fired

**Publish the rule** so it's callable via the API.

## 2. Find your rule slug

Rulebricks slugs are short opaque IDs — roughly 10 characters of nanoid-style
text, for example `abc123xyz9`. Two ways to find yours:

- **From the rule editor URL**: the slug appears as the last path segment,
  e.g. `https://rulebricks.com/dashboard/editor/abc123xyz9`.
- **From the Rules list**: in the Rulebricks Dashboard, each rule card shows
  its slug underneath the title.

Copy this slug into the `RULE_SLUG` constant at the top of `demo.py`.

## 3. Get an API key

1. Open your Rulebricks workspace.
2. Go to **Settings** → **API Keys**.
3. Create a new key. Name it something like `databricks-demo`.
4. Copy the key — you won't be shown it again.

The key needs permission to call `bulk_solve` on the rule and, if you want
automatic schema inference, to read rule definitions (`assets.rules.list` /
`assets.rules.pull`). A default workspace key covers both.

## 4. Wire it into Databricks

### Create a secret scope (one-time, per workspace)

From your local machine with the Databricks CLI installed and authenticated:

```bash
databricks secrets create-scope rulebricks
databricks secrets put-secret rulebricks api_key
```

The `put-secret` command will prompt you to paste the key.

### Cluster requirements

- Databricks Runtime 14.0 or newer (for the modern `%pip install` + restart flow)
- Any cluster size — the notebook auto-scales via the concurrency knobs

### Install the package

Either install on the cluster once (Cluster → Libraries → PyPI →
`rulebricks-spark`) or use the notebook cell already included in `demo.py`:

```python
%pip install rulebricks-spark
dbutils.library.restartPython()
```

### Run the notebook

Import `demo.py` into your workspace as a notebook, edit `RULE_SLUG` near the
top, attach it to your cluster, and run all cells.

## 5. Running locally (without Databricks)

If you want to try the example outside Databricks, replace the secrets cell
with an environment variable:

```python
import os
RULE_SLUG = "abc123xyz9"
api_key = os.environ["RULEBRICKS_API_KEY"]
```

Install prerequisites:

```bash
pip install pyspark rulebricks-spark
```

And replace `display(...)` calls with `.show()` where applicable. The
`%pip`, `%sql`, and `dbutils` cells are Databricks-specific and can be
skipped locally.

## Troubleshooting

- **`SchemaInferenceError: Rule with slug ... not found`** — your API key
  doesn't have access to the rule, or the slug is wrong. Double-check both.
- **`SchemaConflictError`** — a column in your input DataFrame collides with
  a rule output. Pass `on_conflict="overwrite"` (the default) or rename the
  input column.
- **All rows land in `_rb_error`** — check the error string. Common causes
  are a bad API key, a network-blocked cluster, or a rule input field name
  that doesn't match the DataFrame column name. Use `input_mapping=` to fix
  the latter.
