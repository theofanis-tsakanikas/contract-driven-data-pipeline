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
│   ├── spark-defaults.conf
│   └── terraform/              # IaC: data-lake bucket + lifecycle, least-priv IAM, Glue crawler + Athena
│       └── bootstrap/          # one-time: remote-state bucket + lock table + GitHub OIDC deployer role
├── tests/                      # pytest unit tests for the PySpark transform
├── Makefile                    # dev ergonomics: make up / run / tf-apply / crawler ... (make = help)
├── .github/workflows/ci.yml    # lint + test + smoke + dag-validate CI (the data/code plane)
├── .github/workflows/terraform.yml  # Terraform plan (PR) + apply (manual button, OIDC) — the infra plane
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
| `statsd-exporter` | Airflow StatsD → Prometheus bridge | 9102 |
| `prometheus` | scrapes + stores Airflow metrics | 9090 |
| `grafana` | pipeline-observability dashboards | 3000 |

UIs: Airflow http://localhost:8088 · pgAdmin http://localhost:5050 · Spark master http://localhost:8080 · Grafana http://localhost:3000 (admin/admin) · Prometheus http://localhost:9090

> **Note — Spark runs in-process (`local[*]`) by default.** The transform script uses
> `SparkSession.builder.master(os.getenv("SPARK_MASTER", "local[*]"))`, and compose
> defaults both `SPARK_MASTER` and the `spark_default` connection to `local[*]`, so the
> `spark-clean-task` runs Spark inside `airflow-worker` — reliable on every architecture,
> **including Apple Silicon (arm64)**. The standalone `spark-master`/`spark-worker`
> services are available but **opt-in**: to use the cluster, set `SPARK_MASTER` and the
> `AIRFLOW_CONN_SPARK_DEFAULT` host to `spark://spark-master:7077` — which requires an
> amd64 host (the Spark image's JDK is native per-arch, but distributed mode wasn't the
> default for portability). The transform uses only Spark-SQL built-ins (no Python UDFs),
> so executors need no `--py-files`.

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
| `S3_BUCKET_NAME` | globally-unique bucket name (provisioned by `infra/terraform`; the pipeline only reads/writes objects, raw zone expired by a lifecycle rule) |
| `S3_FILE_KEY` | fallback object key; the DAG overrides it per run with `raw/dt=<ds>/dirty-data.csv` |
| `LOCAL_REJECTS_PATH`, `DQ_REPORT_PATH` | quarantined rejects (with `rejection_reason`) + the per-run data-quality summary, under `/opt/airflow/data` |
| `LOCAL_DIRTY_PATH`, `LOCAL_CLEAN_FOLDER`, `LOCAL_CLEAN_PATH` | container staging paths under `/opt/airflow/data` |
| `AIRFLOW_UID`, `AIRFLOW_GID` | file ownership for mounts (`1000:0`) |

### Connections as code

No manual UI connection setup is required. Connections are declared in
`infra/docker-compose.yml` via the `AIRFLOW_CONN_<ID>` env-var pattern (JSON form,
interpolated from `.env`):

- `AIRFLOW_CONN_SPARK_DEFAULT` → `local[*]` (in-process; opt into the cluster with `spark://spark-master:7077`)
- `AIRFLOW_CONN_AWS_DEFAULT` → IAM creds + region
- `AIRFLOW_CONN_POSTGRES_DEFAULT` → target Postgres

> These are **consumed by the DAG tasks** (not the scripts, which stay standalone- and
> unit-testable): `run_ingestion` resolves S3 credentials via `S3Hook(aws_conn_id="aws_default")`
> and `run_loading` reads `postgres_default` via `BaseHook.get_connection`, injecting a
> connection factory into the loader. `spark-clean-task` submits through `spark_default`.
> The ETL scripts themselves still use plain boto3/psycopg2 by default (dependency
> injection), so credentials live in Airflow's connection store at orchestration time
> while the modules remain importable without Airflow (CI `test`/`smoke` jobs).

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

## Infrastructure & deploy (two planes)

Keep the **control plane** (infra) and **data plane** (the ETL run) separate — they
have different lifecycles and are driven by different tools.

- **Control plane — Terraform.** Provisions the AWS side (data-lake bucket + lifecycle,
  least-privilege pipeline IAM user, Glue crawler + Athena). One-time `infra/terraform/bootstrap`
  (local, admin creds) creates the **remote-state bucket + DynamoDB lock + GitHub OIDC
  deployer role**; the main config then uses that S3 backend. Apply it via `make tf-apply`
  locally, **or** via `.github/workflows/terraform.yml`: every PR touching `infra/terraform/**`
  gets a read-only `plan`; the real `apply` is the manual *Run workflow* button, gated by the
  `production` environment's approval, authenticating with **OIDC (no stored AWS keys)**.
  CI reads GitHub **Variables** (`AWS_REGION`, `AWS_DEPLOY_ROLE_ARN`, `TF_STATE_BUCKET`,
  `TF_LOCK_TABLE`, `DATA_LAKE_BUCKET_NAME`).
- **Data plane — Airflow.** The ETL itself is **not** run from GitHub/Terraform. Bring the
  stack up (`make up`) and trigger the DAG with ▶ in the UI (or `make run`). It is
  **manual-only by design** (`schedule=None`) — there is no cron.
- **Makefile** is the local front door: `make` (help), `make up`, `make run`, `make tf-apply`,
  `make crawler`, `make test`, ...

## Known failure modes & gotchas

- **Raw zone is date-partitioned and lifecycle-managed.** Each DAG run writes its raw
  CSV to `raw/dt=<ds>/dirty-data.csv` (an auditable raw-zone history). The Terraform
  `expire-raw-zone` lifecycle rule (`infra/terraform/s3.tf`, default 30 days) reaps old
  partitions so the bucket doesn't grow unbounded.
- **AWS creds / region.** Missing/invalid `AWS_*` → ingestion fails at upload, or boto3
  raises `NoRegionError`. Region must match where the bucket can be created.
- **Observability (StatsD → Prometheus → Grafana).** Airflow emits UDP StatsD metrics →
  `statsd-exporter` (`:9125` in, `:9102/metrics` out, cleaned up by
  `infra/observability/statsd_mapping.yml`) → `prometheus` (`:9090`) scrapes them →
  `grafana` (`:3000`, admin/admin) renders the provisioned **Airflow — Pipeline
  Observability** dashboard (`infra/observability/grafana/dashboards/`). This monitors the
  *pipeline* (run durations, task finishes by state, heartbeat); the *data* is visualised
  by the Streamlit app and the Athena saved queries instead.
- **Empty result set.** If PySpark filters out every row, `clean_data.csv` is empty and
  `load_to_db_final.py` short-circuits with a warning (no rows inserted).
- **Rejects + DQ report are run artifacts.** The clean task also writes
  `data/rejected_data.csv` (failing rows + `rejection_reason`) and `data/dq_report.json`
  (accept rate, rejections by reason) under the gitignored `data/` mount, **and** uploads
  them back to the lake under date-partitioned `rejects/dt=<ds>/` and `quality/dt=<ds>/`
  zones (non-fatal if that upload fails). The validation rules live once in
  `scripts/data_contract.py`; regenerate the committed `docs/governance/DATA_DICTIONARY.md`
  with `python scripts/data_contract.py` (CI's `--check` fails if it drifts).
- **Spark master.** Defaults to in-process `local[*]` (compose sets both `SPARK_MASTER`
  and `spark_default` to it) for cross-arch portability; opt into the standalone cluster
  via `SPARK_MASTER`/`AIRFLOW_CONN_SPARK_DEFAULT` on amd64. See the cluster note above.
- **Apple Silicon / arm64.** The Dockerfiles install an **arch-aware Temurin JDK**
  (`aarch64` on arm64, `x64` on amd64). An x64-only JDK fails on arm64 with
  `qemu-x86_64: Could not open '/lib64/ld-linux-x86-64.so.2'` and crashes `spark-submit`.
- **dbt dependency isolation.** dbt is intentionally in its own venv. Do **not** add
  `dbt-postgres` to `requirements-airflow.txt` — it can conflict with Airflow's pins.
- **`docker compose` env file path.** Always pass both `--env-file .env` and
  `-f infra/docker-compose.yml` (the compose file expects `.env` at repo root).
- **`AIRFLOW__WEBSERVER__BASE_URL`** is set to `http://localhost:8088`, matching the
  published host port — generated UI links resolve correctly.
