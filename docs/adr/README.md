# Architecture Decision Records

What was chosen, what was rejected, and what it cost. Decisions were made during development
(2026-06) and recorded here in 2026-08 — the reasoning is drawn from the code, the Terraform
comments and [`CLAUDE.md`](../../CLAUDE.md), not reconstructed after the fact.

| ADR | Decision | Rejected |
|---|---|---|
| [0001](0001-one-declared-data-contract.md) | One declared contract drives validation, rejection reasons, PII classification and the data dictionary | Great Expectations / Soda · documentation by convention |
| [0002](0002-quarantine-rejected-rows.md) | Rejected rows are quarantined with the rule they violated, never dropped | Fail the run on bad data · log-and-move-on |
| [0003](0003-md5-surrogate-key.md) | An MD5 surrogate key, so no direct identifier is ever a primary key | Email as PK · UUID (breaks idempotency) · HMAC (needs a managed key) |
| [0004](0004-terraform-provisions-the-bucket.md) | Terraform creates the bucket; the pipeline holds no `s3:CreateBucket` and no `s3:DeleteObject` | `create_bucket()` at runtime · console-created bucket · one shared bucket |
| [0005](0005-control-plane-and-data-plane-are-separate.md) | Terraform provisions, Airflow runs; the ETL is never triggered from CI | ETL in GitHub Actions · apply-on-merge · a cron schedule |
| [0006](0006-in-process-spark-by-default.md) | In-process `local[*]` Spark by default; the standalone cluster is opt-in | Cluster as default (fails on arm64) · QEMU emulation |
| [0007](0007-dbt-in-an-isolated-venv.md) | dbt in its own virtualenv, invoked by absolute path | `dbt-postgres` in the Airflow requirements · a separate container · Cosmos |

## Not recorded here

Two choices were considered for an ADR and deliberately left out, because an ADR gives a decision a
weight it should only carry when there was a real trade-off:

- **One PostgreSQL container hosting two logical databases** (`airflow` metadata, `user_data`
  analytics). A local resource-saving measure, explained in the README and reversible by changing
  `DB_HOST`.
- **`coalesce(1)` on the cleaned and rejected outputs.** Produces one readable CSV per run at this
  data size; it would be the first thing to remove at scale.

## Format

Each record states **Context** (the forces, including what was tried first), **Decision** (what was
chosen, with the code that implements it), **Alternatives rejected** (and why), and **Consequences**
(including the ones that hurt). A decision that turns out to be wrong is superseded by a new record,
not edited — the ledger keeps the mistake.
