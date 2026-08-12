# ADR-0002: Quarantine rejected rows with their reason, never drop silently

- **Status:** Accepted
- **Date:** 2026-06 (recorded 2026-08)

## Context

The generator produces deliberately dirty data, and a typical run accepts only ~16–19% of rows. The
obvious implementation is a filter: keep what passes, let the rest fall on the floor.

That produces a pipeline nobody can audit. The counts never reconcile, and the only question that
matters after a bad load — *which records never reached the warehouse, and why?* — has no answer
better than "re-run it and watch the logs". A dropped row leaves no evidence that it ever existed.

## Decision

The transform emits **two** outputs from one pass, and they partition the input exactly:

- `clean_dataframe()` — rows satisfying every contract rule.
- `rejected_dataframe()` — the complement, each row annotated with `rejection_reason`: the **first**
  rule it violated, taken from the same contract ([ADR-0001](0001-one-declared-data-contract.md)).

Rejected rows are written to the `rejects/dt=<ds>/` zone in S3, and a per-run summary
(`dq_report.json` — accept rate and rejections by reason) is written to `quality/dt=<ds>/`, emitted
as `airflow.dq.*` StatsD gauges, and logged in the Airflow task. Glue catalogues both zones so the
history is queryable from Athena.

A test asserts the partition property directly: for any input, accepted ∪ rejected = input, and
accepted ∩ rejected = ∅.

## Alternatives rejected

- **Fail the run on any bad row.** Correct for a contract-strict source; wrong here, where ~81% of
  rows are bad by construction. It also conflates *detection* with *reaction* — the run would tell
  you nothing about which rule fired.
- **Log the rejects and move on.** Logs are not a dataset. They rotate, they are not partitioned, and
  you cannot run `GROUP BY rejection_reason` over them a month later.
- **All violated rules per row, not just the first.** More information, and genuinely better for
  root-cause analysis. Rejected for now because "first rule" makes `rejection_reason` a single
  low-cardinality column that Athena and Grafana can group by without unnesting an array.

## Consequences

- Data quality is a **measurement**, not a feeling: the accept rate and the rejection breakdown are
  live in Grafana and historical in Athena, from the same numbers.
- The lake grows by a rejects file per run — bounded by the same lifecycle rules as the raw zone
  ([ADR-0004](0004-terraform-provisions-the-bucket.md)).
- Every record missing from the warehouse is traceable to the rule that refused it.
