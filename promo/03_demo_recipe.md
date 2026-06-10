# Promo Demo — Recipe (how to get a clean green run + a great payoff)

The hero pairs two assets: the **Airflow DAG** going green, and the **Marts BI Dashboard** as the
payoff. Film the dashboard in demo mode (no infra), and pre-run the DAG so it's green on camera.

## TL;DR
| Choice | Value | Why |
|---|---|---|
| **BI dashboard** | **Demo data**, ~2000 rows, seed `7` | Zero infra, repeatable, realistic distributions. |
| **Airflow run** | **pre-run once**, then film a clean re-run | Known timing, guaranteed green. |
| **Dirty data frame** | `scripts/generate_dirty_data_S3.py` output | A real messy row for the "before". |
| **DAG** | `s3-to-postgres-etl` (`dags/s3-spark-pg-etl.py`) | Tasks: ingest → clean → load → dbt. |

## Part 1 — The BI dashboard (demo mode)
```
cd app
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
streamlit run streamlit_app.py
```
Open http://localhost:8501 → **Demo data**. Set sample size ~`2000`, seed `7` (🔄 Regenerate to
re-roll). Confirm the **"◆ DEMO DATA"** badge.

### What's real vs synthesised
- **Real:** the `age_band` boundaries (18-29 / 30-44 / 45-64 / 65+) and the marts' group-bys are the
  exact dbt logic (`dbt/models/staging/stg_users.sql`, `dbt/models/marts/*.sql`); the `users`
  columns match `scripts/load_to_db_final.py`.
- **Synthesised:** the user rows (weighted real-US cities so "by city" shows realistic concentration).
- **Caption honesty:** keep the badge visible; this is "the marts the pipeline produces", shown on demo data.

## Part 2 — Airflow (the orchestration)
```
docker compose -f infra/docker-compose.yml up -d
# wait ~30–60s for the scheduler + webserver
open http://localhost:8088          # login from .env (AIRFLOW_ADMIN_USER / _PASSWORD)
```
Un-pause **`s3-to-postgres-etl`**. The DAG (`dags/s3-spark-pg-etl.py`) is **manual-trigger**
(`schedule=None`), tasks in order:
```
run_ingestion → spark-clean-task → run_loading → run_dbt
```

### To get a real green run
`run_ingestion` uploads to S3, so it needs AWS creds + a bucket in `.env`
(`S3_BUCKET_NAME`, `S3_FILE_KEY`, `AWS_DEFAULT_REGION`, AWS keys). With those set, trigger once and
let it complete — then film a clean re-run (timing is known, idempotent upsert means re-runs are safe).

> No AWS handy? Film a **previously successful** run from the Grid/Graph view (the green history is
> real) and skip the live trigger. Don't fake a green state.

## Part 3 — The "before" frame (dirty data)
```
python scripts/generate_dirty_data_S3.py     # or open an existing dirty CSV
```
Pick a row with a visible defect: blank `name`, `invalid-email`, `age` of `150`/`-5`/`0`, or a
malformed `zip_code`. Highlight 1–2 cells red in the edit. This is your scene-0 hook.

## After the shoot
```
docker compose -f infra/docker-compose.yml down
```
Nothing to tear down for the dashboard (demo mode). If you ran the real pipeline, the S3 object +
Postgres rows are negligible and idempotent.
