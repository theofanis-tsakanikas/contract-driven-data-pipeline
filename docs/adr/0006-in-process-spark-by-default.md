# ADR-0006: In-process Spark (`local[*]`) as the default execution mode

- **Status:** Accepted
- **Date:** 2026-06 (recorded 2026-08)

## Context

The compose stack ships `spark-master` and `spark-worker` services, and `SparkSubmitOperator` can
target either them or an in-process session. A standalone cluster looks more impressive in a
`docker compose ps` listing and is closer to how Spark runs in production.

It also does not work reliably on the machine this project is developed on. The Spark image's JDK is
architecture-native, and the standalone services assume `amd64`; on Apple Silicon (`arm64`)
`spark-submit` against `spark://spark-master:7077` fails with
`qemu-x86_64: Could not open '/lib64/ld-linux-x86-64.so.2'`. A default that crashes on the reviewer's
laptop is worse than a default that is architecturally modest.

## Decision

Default to in-process Spark, and keep the cluster as an opt-in:

```python
SparkSession.builder.master(os.getenv("SPARK_MASTER", "local[*]"))
```

Compose sets both `SPARK_MASTER` and the `spark_default` Airflow connection to `local[*]`, so
`spark-clean-task` runs Spark inside `airflow-worker`. To use the standalone cluster, set
`SPARK_MASTER` and the `AIRFLOW_CONN_SPARK_DEFAULT` host to `spark://spark-master:7077` on an amd64
host — the services stay in the compose file for exactly that.

Both Dockerfiles install an **arch-aware** Temurin JDK (`aarch64` on arm64, `x64` on amd64), so the
in-process path is native on either architecture.

The transform uses only Spark-SQL built-ins and no Python UDFs, so executors need no `--py-files` and
the code is identical in both modes.

## Alternatives rejected

- **Standalone cluster as the default.** Fails on arm64, which is the primary development machine and
  a large share of any reviewer's laptops.
- **Remove the cluster services entirely.** Would make the compose file honest at the cost of removing
  the demonstration that the job *can* be submitted to a cluster unchanged.
- **Emulate amd64 under QEMU.** Works, slowly, and introduces a failure mode that is very hard to
  diagnose from a Spark stack trace.

## Consequences

- `make up && make run` works on any architecture, first try.
- **Nothing here proves distributed execution.** The transform is written to run distributed and the
  submit path is unchanged, but no run in this repository has used more than one JVM. Stated in the
  README's [What this does not do](../../README.md#what-this-does-not-do).
- Spark memory is bounded by the `airflow-worker` container's limits, which is the reason CI runs the
  test suite with `local[1]` and low driver memory.
