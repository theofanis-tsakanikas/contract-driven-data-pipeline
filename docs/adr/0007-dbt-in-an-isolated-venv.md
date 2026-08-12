# ADR-0007: dbt lives in an isolated virtualenv inside the Airflow image

- **Status:** Accepted
- **Date:** 2026-06 (recorded 2026-08)

## Context

The `run_dbt` and `run_dbt_test` tasks execute inside the Airflow worker, so dbt has to be reachable
from that container. The natural move is to add `dbt-postgres` to `infra/requirements-airflow.txt`
and let pip resolve everything together.

Airflow 2.11 pins a large, deliberately narrow dependency set (its constraints file exists precisely
because the resolver otherwise produces broken environments). `dbt-core` pins its own — `jinja2`,
`click`, `packaging`, `protobuf` and the networking stack are common ground, and the ranges do not
reliably intersect. The failure is not a clean resolver error at build time; it is a pip resolution
that "succeeds" and then breaks the scheduler at import, or downgrades a provider silently.

## Decision

Install dbt into a **separate virtualenv inside the same image** (`/home/airflow/dbt-venv`) and invoke
it by absolute path:

```bash
/home/airflow/dbt-venv/bin/dbt run \
  --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt
```

Airflow resolves its dependencies; dbt resolves its own; neither sees the other's `site-packages`.
`dbt-postgres` must **never** be added to `requirements-airflow.txt` — the rule is recorded in
[`CLAUDE.md`](../../CLAUDE.md) as a gotcha because the temptation recurs.

`run_dbt_test` runs the schema tests as a **gate**: if they fail, the DAG fails rather than publish
broken marts.

## Alternatives rejected

- **`dbt-postgres` in the Airflow requirements.** The dependency conflict above. It is the option that
  looks simplest and costs the most.
- **A separate dbt container / `DockerOperator`.** Cleanest isolation, and the right answer at scale.
  Rejected here because it adds an image, a mount and a Docker-socket dependency to a stack that
  already runs six services on a laptop.
- **Cosmos or another dbt–Airflow integration.** Real value on a large dbt project (per-model tasks,
  fine-grained retries). Disproportionate for three models.
- **`PythonVirtualenvOperator`.** Rebuilds the environment per task run — slow, and it moves the
  dependency set out of the image where it can no longer be pinned or scanned.

## Consequences

- Airflow and dbt upgrade independently; neither can break the other's resolution.
- The dbt binary is invoked by absolute path, so the tasks are explicit about which interpreter runs.
- One extra layer in the image, and dbt's dependencies are not covered by Airflow's constraints file —
  they are pinned separately instead.
- Because dbt is not importable from the Airflow interpreter, the dbt models are not unit-tested by
  the pytest suite; they are covered by `run_dbt_test` at run time.
