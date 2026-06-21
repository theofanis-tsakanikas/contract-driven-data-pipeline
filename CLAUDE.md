# CLAUDE.md — Engineering Reference

Onboarding and engineering reference for **s3-spark-pg-etl**, a containerised ETL
pipeline: Faker → AWS S3 → PySpark clean → PostgreSQL → dbt analytics, orchestrated
by Apache Airflow. See [README.md](README.md) for the narrative overview and the
data-lineage diagram.

## Repo structure

```
s3-spark-pg-etl/
├── dags/                       # Airflow DAG (TaskFlow API) — dag_id: s3-to-postgres-etl
│   └── s3-spark-pg-etl.py
├── scripts/                    # ETL stage logic
│   ├── generate_dirty_data_S3.py   # ingestion: Faker → S3
│   ├── data_contract.py            # declarative data contract (rules + PII class) — single source of truth + data-dictionary generator
│   ├── clean_dirty_data_S3.py      # transform: PySpark; clean_dataframe (accepts) + rejected_dataframe (provenance) + data_quality_report
│   └── load_to_db_final.py         # load: bulk upsert into PostgreSQL
├── docs/governance/            # Generated DATA_DICTIONARY.md (contract + PII classification; CI --check keeps it in sync)
├── dbt/                        # dbt silver/analytics layer on user_data.users
│   ├── dbt_project.yml
│   ├── profiles.yml
│   └── models/{staging,marts}/
├── infra/                      # Docker build + compose
│   ├── docker-compose.yml
│   ├── Dockerfile.airflow      # Airflow + Spark + isolated dbt venv
│   ├── Dockerfile.spark        # standalone Spark image
│   ├── init.sh                 # db init + admin user bootstrap
│   ├── requirements-airflow.txt
│   ├── requirements-spark.txt
│   └── spark-defaults.conf
├── tests/                      # pytest unit tests for the PySpark transform
├── .github/workflows/ci.yml    # lint + test + smoke CI
├── data/  logs/                # runtime mounts (gitignored)
├── .env / .env.example         # configuration (.env is gitignored)
└── README.md
```

## Versions

| Component | Version | Source |
| :-- | :-- | :-- |
| Apache Airflow | 2.11.0 | `infra/Dockerfile.airflow` (`apache/airflow:2.11.0-python3.12`) |
| Python | 3.12 | base image |
| Apache Spark | 3.5.2 | requirements + both Dockerfiles + `spark-defaults.conf` |
| PostgreSQL | 16 | `infra/docker-compose.yml` |
| Java (runtime) | Temurin 21 (containers) / 17 (CI) | Dockerfiles use 21; CI uses 17 (both supported by Spark 3.5.2) |
| dbt-postgres | 1.8.2 | isolated venv `/home/airflow/dbt-venv` |
| Providers | apache-spark 4.8.1, amazon 8.24.0 | `requirements-airflow.txt` |

## Docker Compose services & ports

Brought up with:

```bash
docker compose --env-file .env -f infra/docker-compose.yml up --build -d
```

| Service | Purpose | Host ports |
| :-- | :-- | :-- |
| `postgres` (`airflow-docker-postgres-1`) | Airflow metadata + target DB | 5432 |
| `redis` | Celery broker | 6379 |
| `airflow-init` | runs `init.sh` (db init + admin user), then idles for healthcheck | — |
| `airflow-webserver` | Airflow UI | 8088 → 8080 |
| `airflow-scheduler` | scheduling | 42000, 42001 |
| `airflow-worker` | Celery task execution (where the ETL + dbt actually run) | — |
| `airflow-triggerer` | deferrable triggers | — |
| `pgadmin` | Postgres UI | 5050 → 80 |
| `spark-master` | standalone Spark master (see note below) | 8080, 7077 |
| `spark-worker` | standalone Spark worker | 8081 (unpublished) |

UIs: Airflow http://localhost:8088 · pgAdmin http://localhost:5050 · Spark master http://localhost:8080

> **Note — the standalone Spark cluster is currently decorative.** The transform
> script forces `SparkSession.builder.master("local[*]")`, so Spark runs in-process
> inside `airflow-worker`. The `spark-master`/`spark-worker` services are not used by
> the pipeline as written. The declared `AIRFLOW_CONN_SPARK_DEFAULT` points at the
> cluster so the connection is consistent if you later remove the hardcoded master.

## Triggering the DAG (`dag_id: s3-to-postgres-etl`)

- **UI:** http://localhost:8088 → enable the DAG → ▶ Trigger DAG. Task order:
  `run_ingestion → spark-clean-task → run_loading → run_dbt → run_dbt_test`.
- **CLI:**
  ```bash
  docker compose -f infra/docker-compose.yml exec airflow-scheduler \
    airflow dags trigger s3-to-postgres-etl
  # watch state
  docker compose -f infra/docker-compose.yml exec airflow-scheduler \
    airflow dags list-runs -d s3-to-postgres-etl
  ```

The DAG is `schedule=None` (manual only) and `catchup=False`.

## Where to find logs

- **Airflow UI:** Grid/Graph view → click a task → **Logs** tab.
- **Host mount:** `./logs/` (mounted to `/opt/airflow/logs`).
- **Container logs:** `docker compose -f infra/docker-compose.yml logs -f <service>`
  (e.g. `airflow-worker` for ETL/dbt output, `airflow-init` for bootstrap issues).
- **dbt:** run output appears in the `run_dbt` task log; dbt artifacts land in `dbt/target/`.

## Environment variables

All config lives in `.env` (gitignored; copy from [.env.example](.env.example)) and is
loaded by compose via `--env-file` and `env_file`. Source of each:

| Variable | Source / meaning |
| :-- | :-- |
| `DB_USER`, `DB_PASS` | Postgres superuser creds you choose |
| `DB_HOST`, `DB_PORT` | `airflow-docker-postgres-1` / `5432` |
| `POSTGRES_DB` | Airflow metadata DB (`airflow`) |
| `TARGET_DB` | analytics DB (`user_data`) — loader + dbt target |
| `DEFAULT_DB` | bootstrap DB (`postgres`) used to `CREATE DATABASE` the target |
| `AIRFLOW_ADMIN_USER/PASSWORD/EMAIL` | UI admin, created by `init.sh` |
| `PGADMIN_MAIL`, `PGADMIN_PASS` | pgAdmin login |
| `AWS_ACCESS_KEY_ID/SECRET_ACCESS_KEY` | dedicated IAM user with S3 access |
| `AWS_DEFAULT_REGION` | bucket region (e.g. `eu-central-1`) |
| `S3_BUCKET_NAME` | globally-unique bucket name (created once; raw objects are retained per run, never deleted) |
| `S3_FILE_KEY` | fallback object key; the DAG overrides it per run with `raw/dt=<ds>/dirty-data.csv` |
| `LOCAL_REJECTS_PATH`, `DQ_REPORT_PATH` | quarantined rejects (with `rejection_reason`) + the per-run data-quality summary, under `/opt/airflow/data` |
| `LOCAL_DIRTY_PATH`, `LOCAL_CLEAN_FOLDER`, `LOCAL_CLEAN_PATH` | container staging paths under `/opt/airflow/data` |
| `AIRFLOW_UID`, `AIRFLOW_GID` | file ownership for mounts (`1000:0`) |

### Connections as code

No manual UI connection setup is required. Connections are declared in
`infra/docker-compose.yml` via the `AIRFLOW_CONN_<ID>` env-var pattern (JSON form,
interpolated from `.env`):

- `AIRFLOW_CONN_SPARK_DEFAULT` → `spark://spark-master:7077`
- `AIRFLOW_CONN_AWS_DEFAULT` → IAM creds + region
- `AIRFLOW_CONN_POSTGRES_DEFAULT` → target Postgres

> These are **declared but not yet consumed** by the scripts (the scripts use boto3 /
> psycopg2 / a hardcoded Spark master directly). They make the stack reproducible and
> ready for a future migration to Airflow hooks (`S3Hook`, `PostgresHook`).

## dbt

Lives in `dbt/`, installed into an **isolated venv** (`/home/airflow/dbt-venv`) so its
pinned deps never clash with Airflow. The `run_dbt` task runs:

```bash
/home/airflow/dbt-venv/bin/dbt run --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt
```

Models (silver/analytics on `public.users` in `user_data`):
`stg_users` (view; adds `email_domain`, `age_band`) → `users_by_city`, `users_by_age_band` (tables).

## Tests, lint, CI

- **Unit tests:** `pytest tests/` — exercise `clean_dataframe()` (accepts), `rejected_dataframe()`
  (provenance: every row carries the first contract rule it violated), `data_quality_report()`,
  the loader edge cases, and the pure `data_contract.py` (rules + PII classification) in a local
  SparkSession. Needs Java 17+.
- **Lint:** `ruff check .` — conservative ruleset (`E4/E7/E9` + `F`) in `pyproject.toml`;
  stylistic findings are suppressed inline with `# noqa` rather than rewritten.
- **Data dictionary in sync:** the CI `lint` job runs `python scripts/data_contract.py --check`
  (pure stdlib, no Spark) — it fails if `docs/governance/DATA_DICTIONARY.md` drifts from the
  declared contract. Regenerate with `python scripts/data_contract.py`.
- **CI** (`.github/workflows/ci.yml`, on push + PR): `lint`, `test` (PySpark, Java 17,
  `local[1]`), `dag-validate` (DagBag integrity), and a `smoke` job that loads a CSV fixture
  into a `postgres:16` service container and asserts the row count.

## Known failure modes & gotchas

- **Raw objects are retained.** Each DAG run writes its raw CSV to a date-partitioned
  key (`raw/dt=<ds>/dirty-data.csv`) and nothing is deleted afterwards — the bucket
  accumulates a raw-zone history. Clean up old partitions with an S3 lifecycle rule
  if cost matters.
- **AWS creds / region.** Missing/invalid `AWS_*` → ingestion fails at upload, or boto3
  raises `NoRegionError`. Region must match where the bucket can be created.
- **StatsD host doesn't exist.** Compose sets `AIRFLOW__METRICS__STATSD_HOST: statsd-exporter`
  but there is **no `statsd-exporter` service**. Metrics are UDP, so they silently drop —
  harmless, the line is intentionally kept. Add a `statsd-exporter`/Prometheus service if
  you want metrics collected.
- **Empty result set.** If PySpark filters out every row, `clean_data.csv` is empty and
  `load_to_db_final.py` short-circuits with a warning (no rows inserted).
- **Rejects + DQ report are run artifacts.** The clean task now also writes
  `data/rejected_data.csv` (failing rows + `rejection_reason`) and `data/dq_report.json`
  (accept rate, rejections by reason) — both under the gitignored `data/` mount. The
  validation rules live once in `scripts/data_contract.py`; regenerate the committed
  `docs/governance/DATA_DICTIONARY.md` with `python scripts/data_contract.py` (CI's
  `--check` fails if it drifts from the contract).
- **Spark master override.** As noted above, `local[*]` is hardcoded; the standalone
  Spark containers don't participate.
- **dbt dependency isolation.** dbt is intentionally in its own venv. Do **not** add
  `dbt-postgres` to `requirements-airflow.txt` — it can conflict with Airflow's pins.
- **`docker compose` env file path.** Always pass both `--env-file .env` and
  `-f infra/docker-compose.yml` (the compose file expects `.env` at repo root).
- **`AIRFLOW__WEBSERVER__BASE_URL`** is set to `http://localhost:8080` in compose, but the
  webserver is published on host port `8088` — cosmetic mismatch for generated links only.
