# Hero Promo — Caption Script (exact words + timings)

**Target:** ~55s · captions + music, no voiceover · captions in English.
**Hero assets:** Airflow (`s3-to-postgres-etl` DAG) + the **Marts BI Dashboard**
(`app/streamlit_app.py`, demo mode).

Caption style: large, lower-third, white with a cyan accent on the key word, dark backing.
2.5–4s on screen. One line per beat.

---

### SCENE 0 — Hook (0:00–0:06)
- **Screen:** Title card → a **dirty CSV** snippet: a blank name, `invalid-email`, age `150`, a malformed zip. Highlight 1–2 bad cells in red.
- **Caption (0:02):** `Real-world data is messy.`
- **Caption (0:04):** `Here's the pipeline that fixes it.`
- **Music:** soft, purposeful bed.

### SCENE 1 — Orchestration (0:06–0:18)
- **Screen:** Airflow **Graph view** of `s3-to-postgres-etl`. Trigger the run; the tasks turn green in order: `run_ingestion → spark-clean-task → run_loading → run_dbt`.
- **Caption (0:08):** `Orchestrated with Airflow.`
- **Caption (0:13):** `Ingest → clean → load → transform.`

### SCENE 2 — The transform (0:18–0:30)
- **Screen:** Quick cuts: the PySpark cleaning snippet (range filters, regex, MD5 surrogate key) → the dbt marts (`stg_users`, `users_by_age_band`, `users_by_city`).
- **Caption (0:20):** `PySpark validates and cleans — schema, ranges, dedup.`
- **Caption (0:26):** `dbt builds the analytics marts.`

### SCENE 3 — The payoff (0:30–0:44)
- **Screen:** Cut to the **Marts BI Dashboard**. KPIs read, then pan across **users by age band**, **top cities**, and the **email-domain donut**.
- **Caption (0:32):** `From dirty CSV…`
- **Caption (0:38):** `…to analytics-ready marts.`

### SCENE 4 — Trust (0:44–0:52)
- **Screen:** The dashboard's **Lineage & Quality** tab: S3 → Spark → Postgres → dbt → marts; the cleaning-rules table.
- **Caption (0:46):** `Tested. Idempotent. Fully lineaged.`

### SCENE 5 — Close (0:52–0:58)
- **Screen:** End card (dark). Project name + value line + your name / GitHub.
- **Caption (static):**
  > **End-to-End Batch ETL — Airflow · Spark · Postgres · dbt**
  > Orchestrated · tested · analytics-ready
  > *<your name> — github.com/<you>*

---

## Caption master list (copy-paste ready)
```
1.  Real-world data is messy.
2.  Here's the pipeline that fixes it.
3.  Orchestrated with Airflow.
4.  Ingest → clean → load → transform.
5.  PySpark validates and cleans — schema, ranges, dedup.
6.  dbt builds the analytics marts.
7.  From dirty CSV…
8.  …to analytics-ready marts.
9.  Tested. Idempotent. Fully lineaged.
10. [End card] Modern batch data engineering — Airflow · Spark · Postgres · dbt
```

## Notes
- The **before → after** pairing (scene 0 dirty CSV ↔ scene 3 clean marts) is the spine — make
  sure both are clearly legible.
- The DAG task names are real: `run_ingestion`, `spark-clean-task`, `run_loading`, `run_dbt`
  (`dags/s3-spark-pg-etl.py`). Don't relabel them in captions.
- If tight on time, scene 2 can drop to ~8s (one cut of Spark, one of dbt).
