# Promo Video — Master Plan (Orchestrated Batch ETL → Analytics)

## Goal & audience
A LinkedIn / portfolio piece. A recruiter or engineer must grasp in **~15 seconds**:
> *Dirty raw data → an orchestrated Airflow pipeline (S3 → Spark → Postgres → dbt) → clean, tested, analytics-ready marts.*

This one tells a **transformation story**: messy input on the left, a real BI dashboard on the
right, and a credible orchestration engine in the middle. The payoff is *"look what came out"*.

## One deliverable (hero) — optional short deep-dive
| | Hero promo |
|---|---|
| Length | **~45–60s** |
| Audio | Captions + light music, **no voiceover** |
| Use | LinkedIn / top of the repo |
| Plan | this file + `01_caption_script_hero.md` + `02_shot_list.md` + `03_demo_recipe.md` |

> Batch ETL has less inherent motion than streaming, so the energy comes from (a) the **Airflow
> DAG graph** lighting up task-by-task, and (b) the **before → after** reveal (dirty CSV → BI marts).

## The hero assets (two)
1. **Airflow** — the `s3-to-postgres-etl` DAG graph going green task by task (the orchestration).
2. **Marts BI Dashboard** (`app/streamlit_app.py`) — users by age band / city / email domain + the
   lineage tab. The clean, business-ready payoff. Record it in **demo mode** (no infra — see
   `03_demo_recipe.md`).

## 4 principles
1. **Before → after.** Open on a *dirty* CSV row (nulls, `invalid-email`, age `150`), close on the polished marts. The contrast is the story.
2. **Muted-friendly.** One caption per beat.
3. **Show the orchestration.** The DAG graph turning green is the proof it's engineered, not a script.
4. **Honest.** Demo-mode marts are labelled; the age-band logic and group-bys are the real dbt ones.

## Structure — Hero (~55s, 6 scenes)
| # | Time | On screen | Caption |
|---|------|-----------|---------|
| 0 | 0–6s | Title → a dirty CSV snippet (blank name, `invalid-email`, age `150`, bad zip) | *Real-world data is messy. Here's the pipeline that fixes it.* |
| 1 | 6–18s | Airflow DAG graph; trigger it; tasks go green one by one | *Orchestrated with Airflow — ingest, clean, load, transform.* |
| 2 | 18–30s | Zoom a task: the PySpark cleaning (range filters, regex, MD5 surrogate key) | *PySpark cleans and validates. dbt builds the marts.* |
| 3 | 30–44s | Cut to the **Marts BI Dashboard**: KPIs + users-by-age-band + top-cities + domain donut | *From dirty CSV to analytics-ready marts.* |
| 4 | 44–52s | Lineage tab: S3 → Spark → Postgres → dbt stg → marts; the cleaning-rules table | *Tested, idempotent, fully lineaged.* |
| 5 | 52–58s | End card: project name + value line + your name / GitHub | *Modern batch data engineering — Airflow · Spark · Postgres · dbt* |

## Non-negotiables (the video must contain)
- The **dirty data** open (the problem)
- The **Airflow DAG** going green (the orchestration proof)
- A glimpse of the **transform** (Spark cleaning + dbt marts)
- The **BI dashboard** payoff (the result)
- A nod to **quality/testing/idempotency** (the rigour)

## Pre-production checklist
- [ ] **BI dashboard (demo):** `cd app && python3 -m venv .venv && source .venv/bin/activate && pip install -r requirements.txt && streamlit run streamlit_app.py` → **"◆ DEMO DATA"** badge, sample size ~2000.
- [ ] **Airflow:** `docker compose -f infra/docker-compose.yml up -d`; open http://localhost:8088; confirm the `s3-to-postgres-etl` DAG is parsed and **un-paused**.
- [ ] Have a **dirty CSV** sample ready to show (generate one, or screenshot `scripts/generate_dirty_data_S3.py` output).
- [ ] (For a real green run) AWS creds + S3 bucket in `.env`, so `run_ingestion` actually uploads. Otherwise film a **pre-run** successful DAG.
- [ ] Screen Studio: 16:9, retina, clean menu bar.

## Honest do / don't
- **DO** show a **real dirty row** and a **real clean mart** — the contrast sells it.
- **DO** film a DAG run that actually **goes green** (pre-run it once so timing is known).
- **DON'T** show a failed/red task, a stuck scheduler, or AWS credential errors — warm it up first.
- **DON'T** imply real-time — this is **batch** (manual trigger / scheduled). Say "orchestrated batch".

## The one-line test
If a stranger watches the **first 15 seconds on mute** and says *"messy data goes in, a clean pipeline cleans it"* — the opening works.
