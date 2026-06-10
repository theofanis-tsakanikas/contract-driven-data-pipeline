# Hero Promo — Shot List (record this, in this order)

Record the **BI dashboard** first (demo mode, fully repeatable), then the **Airflow** run (pre-run
it so timing is known), then the code cuts. Assemble in scene order in the edit.

## Stage 0 — Setup (before any recording)
1. **BI dashboard (demo):**
   ```
   cd app && python3 -m venv .venv && source .venv/bin/activate
   pip install -r requirements.txt
   streamlit run streamlit_app.py
   ```
   Sidebar: **Demo data**, sample size ~`2000`, seed `7`. Confirm dark theme, **"◆ DEMO DATA"** badge.
2. **Airflow:** `docker compose -f infra/docker-compose.yml up -d` → http://localhost:8088
   (login from `.env`). Un-pause `s3-to-postgres-etl`. **Do one full run first** so you know it
   goes green and how long each task takes.
3. Prepare a **dirty CSV** frame: run `scripts/generate_dirty_data_S3.py` (or open a sample) and
   pick a row with a blank name / `invalid-email` / age `150` / bad zip.
4. Screen Studio: 16:9, retina, clean menu bar.

---

## Clips to record (in this recording order)

### CLIP A — BI dashboard payoff (record first; reusable)
- **What:** The **Marts BI Dashboard**. KPI row, then a slow pan across **users by age band**,
  **top cities** (horizontal bars), and the **email-domain donut**. Then the **Lineage & Quality**
  tab (the flow strip + cleaning-rules table).
- **Length:** ~18s raw.
- **Screen Studio:** gentle auto-zoom into each chart; smooth cursor.

### CLIP B — Airflow DAG run (the orchestration)
- **What:** Airflow **Graph view** of `s3-to-postgres-etl`. Trigger; capture the tasks turning
  green in order: `run_ingestion → spark-clean-task → run_loading → run_dbt`.
- **Length:** record the full run; **speed-ramp** the waits, **slow to 1×** on each task going green.
- **Screen Studio:** keep the whole graph in frame; trim later.

### CLIP C — Dirty data (the problem)
- **What:** The dirty CSV row(s) with bad cells highlighted. A clean screenshot is fine.
- **Length:** ~6s raw.

### CLIP D — Transform code cuts
- **What:** 3–4s each: the PySpark cleaning in `scripts/clean_dirty_data_S3.py` (range filters /
  regex / MD5 key) and a dbt model (`dbt/models/marts/users_by_age_band.sql` or `stg_users.sql`).
- **Length:** ~8s raw.

### CLIP E — DAG code (OPTIONAL b-roll)
- **What:** `dags/s3-spark-pg-etl.py` — the TaskFlow `>>` dependency line. Good 2s "it's code" beat.
- **Length:** ~4s raw.

### Title + End cards
- Built in the editor. Text from `01_caption_script_hero.md` (scenes 0 and 5).

---

## Assembly order (in the editor) = final scenes
`Title → CLIP C → CLIP B → CLIP D → CLIP A (charts) → CLIP A (lineage) → End card`
Map to the script: 0 → 1 → 2 → 3 → 4 → 5.

---

## Screen Studio tips
- **Speed-ramp the DAG waits** (3–4×); slow to 1× on each green task so the progression reads.
- The **before → after** cut (dirty CSV → clean marts) is the money beat — make both legible.
- Keep the **"◆ DEMO DATA"** badge in frame for the dashboard clips.
- **One motion per beat.** Captions lower-third.
- **Music:** calm, purposeful (it's batch, not frantic). Resolve on the end card.
- Export **1080p MP4, 30–60fps**.

## Final QC before you publish
- [ ] Reads on **mute** (the one-line test from `00_master_plan.md`).
- [ ] The DAG run shown is **all green** — no red/failed tasks, no scheduler stalls.
- [ ] The dirty→clean contrast is obvious.
- [ ] It's clearly **batch** (no false "real-time" implication).
- [ ] Ends with a clear "what is this + who made it" card.
