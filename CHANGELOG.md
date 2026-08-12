# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- **`SECURITY.md`** — scope, vulnerability reporting, the hardened controls, and eight known
  limitations, each stated with the control a real deployment would use instead. Includes a
  pre-publish checklist.
- **`docs/adr/`** — seven Architecture Decision Records (contract as single source of truth,
  quarantine over silent drop, MD5 surrogate key, Terraform-owned bucket, control/data-plane
  separation, in-process Spark, isolated dbt venv), each recording what was rejected and why.
- **`.github/dependabot.yml`** — weekly grouped update PRs for pip (infra + app), GitHub Actions and
  Docker base images; monthly for the Terraform providers.

### Changed

- **README rewritten to the portfolio README standard** — the results gallery was dissolved and every
  screenshot moved beside the claim it proves (no image removed); added `Status`, `Testing`,
  `What this does not do`, `Cost`, `Decisions`, `Docs`, `Security` and `License` sections; the inline
  `.env` block was replaced by a link to `.env.example`.

- **The Terraform workflow no longer fails when the estate is torn down.** `terraform fmt -check`,
  `init -backend=false` and `validate` now run **offline** on every PR — real signal with no
  credentials. The cloud `plan` is gated on the five deploy variables being set; when they are not,
  the step emits a `::notice::` naming the missing ones and the job stays green. The `apply` job does
  the opposite and **fails loudly** on the same condition, because an apply with nothing to apply to
  is a mistake rather than a no-op.

- **The bootstrap no longer assumes it owns the account's GitHub OIDC provider.** It is an
  account-level singleton shared by every repository that federates into the account, and this
  repository's `bootstrap` declared it as a resource — so the next apply would have failed with
  `EntityAlreadyExists`, and an apply-then-destroy would have deleted a provider another project
  depends on. New variable `create_oidc_provider` (default `true`); set it to `false` to reference
  an existing provider instead. Verified with a `plan` against the live account: 9 resources to add,
  no provider among them. New output `oidc_provider_owned_here` records which side of that line the
  state is on.

- **CI now tests the versions the images actually ship.** The `test` job installed its dependencies
  from a list written inline in `ci.yml`, so a bump to `infra/requirements-*.txt` changed the image
  while the tests kept running the old pins. It now installs `-r requirements-dev.txt`, which pulls
  in `infra/requirements-spark.txt`.
- **`tests/test_dependency_pins.py`** — nine guards on the invariants that previously lived only in
  comments: the Airflow version in `Dockerfile.airflow` must equal the one `dag-validate` installs
  and the constraints file it resolves against, and packages pinned in both images must carry the
  same version. Each was verified by breaking it on purpose and confirming it refuses.
- **Dependabot version updates switched off (`open-pull-requests-limit: 0`), matching the rest of the
  portfolio.** Enabling pip, Docker and Terraform version updates
  produced seventeen pull requests in minutes — pandas 2→3, numpy 1→2, both Airflow providers across
  majors, and the base image from Airflow 2.11 to 3.3 — and every one passed all five checks, because
  nothing built the images. Dependabot **security** updates remain enabled, so a real CVE still opens
  a PR — that is the part worth keeping.

### Security

- **OIDC trust policy tightened.** The deployer role's `sub` condition moved from
  `StringLike repo:<repo>:*` to `StringEquals` on exactly three subjects — `pull_request`,
  `ref:refs/heads/<default>` and `environment:production`. A workflow dispatched from a feature
  branch is now refused by AWS. New variable: `github_default_branch` (default `main`).
- **Three local-stack changes were made and then reverted**, deliberately: binding every published
  port to `127.0.0.1`, turning off the Airflow Config view, and composing the bulk `INSERT`'s column
  identifiers with `psycopg2.sql.Identifier`. This pipeline is deployed and working; each of those
  altered its runtime behaviour in exchange for a gain that is theoretical on a single-user laptop
  stack. They are documented as limitations 6 and 9 in `SECURITY.md`, with exactly what to change
  before running it anywhere shared — a written trade-off rather than a silent one.

## [0.1.0] - 2026-05-31

Initial documented release of the containerised **Faker → S3 → PySpark → PostgreSQL → dbt**
ETL pipeline orchestrated by Apache Airflow.

### Added

- **ETL pipeline & orchestration**
  - Airflow DAG (`s3-to-postgres-etl`, TaskFlow API) wiring the stages
    `run_ingestion → spark-clean-task → run_loading → run_dbt`.
  - Ingestion (`generate_dirty_data_S3.py`): Faker-generated dirty data uploaded to AWS S3 via boto3.
  - Transform (`clean_dirty_data_S3.py`): PySpark schema enforcement, regex/null validation,
    age casting, and a deterministic MD5 `user_id` surrogate key.
  - Load (`load_to_db_final.py`): bulk upsert into PostgreSQL with `execute_values`
    and `ON CONFLICT (user_id) DO NOTHING`.
- **dbt analytics layer** (`dbt/`): `stg_users` silver view (adds `email_domain`, `age_band`)
  feeding `users_by_city` and `users_by_age_band` marts, with source/model tests.
  Installed in an isolated venv and run as the `run_dbt` DAG task.
- **Connections as code**: `AIRFLOW_CONN_SPARK_DEFAULT`, `AIRFLOW_CONN_AWS_DEFAULT`, and
  `AIRFLOW_CONN_POSTGRES_DEFAULT` declared in `docker-compose.yml` (JSON form), removing
  manual UI setup.
- **Testing**: pytest unit tests for the pure `clean_dataframe()` transform, exercised
  in a local SparkSession.
- **CI** (`.github/workflows/ci.yml`): `lint` (ruff), `test` (PySpark on Java 17), and a
  `smoke` job loading a CSV fixture into a `postgres:13` service container.
- **Documentation**: `CLAUDE.md` engineering reference, `.env.example`, a Mermaid
  data-lineage diagram in the README, and project badges.
- **Tooling**: conservative ruff configuration in `pyproject.toml`.

### Changed

- Pinned Apache Spark to `3.5.2` consistently across all Docker and requirements files.

### Fixed

- Corrected malformed logging calls in `load_to_db_final.py`
  (`logger.info("...", e)` → `logger.error(f"... {e}")`) so error details render.

[Unreleased]: https://github.com/theofanis-tsakanikas/s3-spark-pg-etl/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/theofanis-tsakanikas/s3-spark-pg-etl/releases/tag/v0.1.0
