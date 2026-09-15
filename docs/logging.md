# Structured logging

stellar-etl writes one JSON object per line to **stdout**, in an envelope meant
to be shared with the other data-platform services rather than owned by this
repo. The Go implementation is `internal/gcplog`.

## The envelope

```json
{
  "severity": "ERROR",
  "message": "could not transform ledger 58231044",
  "service": "stellar-etl",
  "component": "export_ledgers",
  "project": "hubble-261722",
  "environment": "prod",
  "context": { "strict_export": false }
}
```

| Key           | Meaning                                                                     |
| ------------- | --------------------------------------------------------------------------- |
| `severity`    | Cloud Logging `LogSeverity`. One of DEBUG, INFO, WARNING, ERROR, CRITICAL.  |
| `message`     | The human-readable line.                                                    |
| `service`     | Which workload emitted it: `stellar-etl`, `dbt`, a dlt pipeline name.       |
| `component`   | The sub-unit inside that service: a CLI subcommand, dbt node, dlt resource. |
| `project`     | The GCP project the run belongs to.                                         |
| `environment` | The deployment: `prod`, `staging`, `dev_pubnet`, `test`.                    |
| `context`     | Service-specific detail. No meaning shared across services.                 |

Only `severity` and `message` are interpreted by Cloud Logging. The rest are
ordinary `jsonPayload` fields that a Logs Explorer filter can select on.

**Keep the top level small.** A key belongs there only if it means the same
thing in every service, because that is what lets one filter span services
without knowing who wrote the line. Anything else goes in `context`, where it
stays queryable without claiming a shared meaning it does not have.

`project` and `environment` earn the top level for the same reason `service`
does: one image runs in every deployment, so without them a filter that spans
services also spans environments and a prod incident cannot be told apart from a
dev_pubnet one.

`stellar-etl` currently puts one thing in `context`: `strict_export`, which says
whether an error halted the export or was counted and carried past.

## Deployment fields

`project` and `environment` come from the pod, not the build:

| Env var       | Envelope key  | Example                               |
| ------------- | ------------- | ------------------------------------- |
| `GCP_PROJECT` | `project`     | `hubble-261722`, `test-hubble-319619` |
| `ENVIRONMENT` | `environment` | `prod`, `staging`, `dev_pubnet`       |

Either key is **omitted** when its variable is unset or blank, rather than
filled with a placeholder. A value like `unknown` would sort alongside real ones
in a group-by and quietly widen an environment-scoped filter; an absent key
simply drops the line out of a filter it was never meant to match. A laptop run
therefore carries neither field and a pod carries both.

Setting them is an Airflow-side change: both belong in the `env_vars` the
`KubernetesPodOperator` passes, sourced per environment from
`airflow_variables_*.json`.

## Why stdout

Cloud Logging derives severity from the stream when it cannot parse a line, and
GKE records everything on stderr as ERROR. A service logging plain text to
stderr therefore shows up as 100% ERROR no matter what it actually said, which
is what stellar-etl looked like before this change. Writing every line to stdout
hands that decision to the `severity` field.

One consequence for local use: `get_ledger_range_from_times` prints its result
to stdout when `-o` is omitted, so logs and that payload interleave. Airflow is
unaffected, since `build_time_task` always passes
`-o /airflow/xcom/return.json`.

## Correlating with Airflow

Nothing here repeats `dag_id`, `task_id`, `run_id` or `try_number`. Airflow
already stamps those as pod labels on every `KubernetesPodOperator` pod, and a
second copy in the payload could only disagree with the labels.

## Log level

`LOG_LEVEL` overrides verbosity for a single run, so a pod can be put in debug
from an Airflow variable without rebuilding the image. It accepts the logrus
level names (`debug`, `info`, `warn`, `error`). Unset leaves each command's own
default in place; set, it outranks them.

## Mirroring this in another service

The Go side is `internal/gcplog`, which holds no stellar-etl specifics. Porting
it means setting a new `service` value and leaving the rest alone.

Two notes for non-Go services:

- The duplicate `level` and `msg` keys in stellar-etl's output are an artifact of
  `go-stellar-sdk`, whose `UseJSONFormatter` builds a `logrus.JSONFormatter` with
  no `FieldMap` and exposes no way to replace the formatter. `internal/gcplog`
  works around it with a hook that adds the mapped keys alongside. Services that
  can set their own formatter should use `gcplog.Formatter()` and emit neither
  duplicate.
- Python services get the same envelope from `google-cloud-logging`'s
  `StructuredLogHandler` on stdout, passing `service`, `component` and `context`
  through `extra`.
