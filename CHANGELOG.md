# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

_Nothing yet._

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
