# 📊 Marts BI Dashboard

A compact **Streamlit** business-intelligence view over the marts (gold) output
of the **S3 → Spark → Postgres → dbt** pipeline. It turns the dbt analytics
marts into a clean executive dashboard — users by age band, by city, by email
domain — and shows the data lineage that produced them.

It shares the dark/cyan branding of the rest of the portfolio so everything
reads as one coherent product suite.

> Built for presentations: demo mode runs **fully in-process** (no Airflow,
> Spark, S3 or Postgres) while reproducing the same marts the dbt models build.

---

## Quick start (demo mode)

```bash
cd app
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
streamlit run streamlit_app.py
```

Open http://localhost:8501. Use the sidebar to change the sample size / seed and
**regenerate** the dataset.

## Live mode (Postgres)

Run the pipeline so the loader has populated the `users` table, then pick
**"Postgres (live)"** in the sidebar. Defaults match docker-compose
(`localhost:5432`, db `user_data`, user `airflow`). Credentials can also come
from env / `.streamlit/secrets.toml` (`DB_HOST`, `DB_PORT`, `TARGET_DB`,
`DB_USER`, `DB_PASS`). If Postgres is unreachable, it falls back to demo data.

---

## How faithful is the demo?

[`marts_data.py`](marts_data.py) mirrors the dbt layer exactly:

* **`age_band`** boundaries (18-29 / 30-44 / 45-64 / 65+) and **`email_domain`**
  derivation come straight from
  [`dbt/models/staging/stg_users.sql`](../dbt/models/staging/stg_users.sql).
* The marts are the same group-bys as
  [`users_by_age_band.sql`](../dbt/models/marts/users_by_age_band.sql) and
  [`users_by_city.sql`](../dbt/models/marts/users_by_city.sql).
* The `users` columns match [`scripts/load_to_db_final.py`](../scripts/load_to_db_final.py).

Demo cities are drawn from a weighted real-US-city list so the "by city" chart
shows realistic concentration — the shape the mart takes at scale.

## Files

| File | Purpose |
|---|---|
| `streamlit_app.py` | UI: KPIs, mart charts, lineage + data-quality tab |
| `marts_data.py` | Data layer: demo synthesis + Postgres reader (one interface) |
| `requirements.txt` | UI dependencies (light; no Spark/Airflow) |
| `.streamlit/config.toml` | Dark/cyan theme matching the rest of the portfolio |
