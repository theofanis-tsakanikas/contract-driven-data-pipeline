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

### Security

- **OIDC trust policy tightened.** The deployer role's `sub` condition moved from
  `StringLike repo:<repo>:*` to `StringEquals` on exactly three subjects — `pull_request`,
  `ref:refs/heads/<default>` and `environment:production`. A workflow dispatched from a feature
  branch is now refused by AWS. New variable: `github_default_branch` (default `main`).
- **Every published container port is bound to `127.0.0.1`.** The local stack (Postgres, Redis,
  Airflow, pgAdmin, Spark, Prometheus, Grafana, statsd-exporter) is no longer reachable from the LAN.
- **`AIRFLOW__WEBSERVER__EXPOSE_CONFIG` set to `False`** — the Config view rendered `airflow.cfg`
  including the metadata-DB connection string.
- **Column identifiers in the bulk `INSERT` are composed with `psycopg2.sql.Identifier`** instead of
  being interpolated into the statement, so a tampered intermediate CSV header cannot reach the SQL
  text. `CREATE DATABASE` already did this.

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
