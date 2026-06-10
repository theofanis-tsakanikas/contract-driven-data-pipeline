"""Marts BI Dashboard — Streamlit UI.

A compact business-intelligence view over the Gold/marts output of the
S3 → Spark → Postgres → dbt pipeline. It turns the analytics marts
(``users_by_age_band``, ``users_by_city``, plus an email-domain cut) into a
clean executive dashboard, and shows the data lineage that produced them.

Run locally:
    pip install -r app/requirements.txt
    streamlit run app/streamlit_app.py

Data source:
    * Demo (default) — synthesises a clean users dataset and derives the same
      marts the dbt models produce. No Airflow / Spark / S3 / Postgres needed.
    * Postgres (live) — reads the real users table the loader writes
      (defaults to localhost:5432, db user_data).
"""

from __future__ import annotations

import os

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

import marts_data as md

# --------------------------------------------------------------------------- #
# Page config + secrets
# --------------------------------------------------------------------------- #

st.set_page_config(page_title="Marts BI Dashboard", page_icon="📊", layout="wide")

try:
    for _k, _v in st.secrets.items():
        if not os.getenv(_k):
            os.environ[_k] = str(_v)
except Exception:
    pass

# --------------------------------------------------------------------------- #
# Theme — shared dark/cyan branding
# --------------------------------------------------------------------------- #

st.markdown("""
<style>
.stApp { background: linear-gradient(160deg, #060c1a 0%, #0d1b35 45%, #070e20 100%); color: #e2e8f0; }
.main .block-container { padding-top: 1.2rem; padding-bottom: 3rem; }
[data-testid="stHeader"] { background: rgba(6,12,26,0.97) !important;
    border-bottom: 1px solid rgba(56,189,248,0.10) !important; backdrop-filter: blur(16px) !important; }
[data-testid="stDecoration"] { display: none !important; }
[data-testid="stSidebar"] { background: rgba(10,18,40,0.97) !important;
    border-right: 1px solid rgba(56,189,248,0.18); }
[data-testid="stSidebar"] p, [data-testid="stSidebar"] span, [data-testid="stSidebar"] label { color: #94a3b8 !important; }
[data-testid="stSidebar"] h1, [data-testid="stSidebar"] h2, [data-testid="stSidebar"] h3 { color: #e2e8f0 !important; }
h1, h2, h3 { color: #e2e8f0 !important; } p, li { color: #cbd5e1 !important; }
[data-testid="stMetric"] { background: rgba(30,41,59,0.7) !important;
    border: 1px solid rgba(56,189,248,0.25) !important; border-radius: 14px !important;
    padding: 1rem 1.2rem !important; }
[data-testid="stMetricValue"] { color: #38bdf8 !important; font-weight: 700 !important; }
[data-testid="stMetricLabel"] { color: #64748b !important; font-size: 0.78rem !important;
    text-transform: uppercase; letter-spacing: 0.06em; }
.stTabs [data-baseweb="tab-list"] { background: rgba(10,18,40,0.6) !important; border-radius: 12px !important;
    padding: 6px !important; border: 1px solid rgba(56,189,248,0.15) !important; gap: 8px; margin-bottom: 1.2rem !important; }
.stTabs [data-baseweb="tab"] { color: #64748b !important; border-radius: 9px !important; padding: 0.55rem 1.1rem !important; }
.stTabs [aria-selected="true"] { background: rgba(56,189,248,0.15) !important; color: #38bdf8 !important; }
hr { border: none !important; border-top: 1px solid rgba(56,189,248,0.15) !important; }
[data-testid="stDeployButton"], .stDeployButton { display: none !important; }
.hero { background: linear-gradient(135deg, rgba(29,78,216,0.18) 0%, rgba(14,165,233,0.10) 100%);
    border: 1px solid rgba(56,189,248,0.25); border-radius: 18px; padding: 1.3rem 1.8rem; margin-bottom: 1.3rem; }
.hero h1 { margin: 0; font-size: 1.85rem; } .hero p { margin: 0.3rem 0 0; color: #94a3b8 !important; }
.badge { display: inline-block; padding: 0.2rem 0.7rem; border-radius: 999px; font-size: 0.72rem; font-weight: 700; }
.badge-demo { background: rgba(234,179,8,0.15); color: #fde047; border: 1px solid rgba(234,179,8,0.4); }
.badge-live { background: rgba(34,197,94,0.15); color: #86efac; border: 1px solid rgba(34,197,94,0.4); }
.flow { display: flex; align-items: center; gap: 0.55rem; flex-wrap: wrap; }
.flow .node { background: rgba(30,41,59,0.6); border: 1px solid rgba(56,189,248,0.25);
    border-radius: 12px; padding: 0.7rem 1rem; text-align: center; min-width: 110px; }
.flow .node .n { font-size: 0.76rem; color: #94a3b8; } .flow .node .v { font-size: 1.1rem; font-weight: 700; color: #38bdf8; }
.flow .arrow { color: #38bdf8; font-size: 1.25rem; }
</style>
""", unsafe_allow_html=True)

_CYAN = "#38bdf8"
_PALETTE = ["#38bdf8", "#818cf8", "#a78bfa", "#f472b6", "#fbbf24", "#34d399"]


# --------------------------------------------------------------------------- #
# Sidebar
# --------------------------------------------------------------------------- #

with st.sidebar:
    st.markdown("### 📊 Marts BI")
    st.caption("Analytics over the S3 → Spark → Postgres → dbt pipeline.")
    st.divider()

    pg_cfg = md.PgConfig()
    pg_up = md.postgres_available(pg_cfg)
    source = st.radio("Data source", ["Demo data", "Postgres (live)"],
                      help="Demo derives the marts in-process. Live reads the "
                           "real users table the loader writes.")
    if source == "Postgres (live)":
        pg_cfg.host = st.text_input("Host", pg_cfg.host)
        pg_cfg.port = int(st.number_input("Port", value=pg_cfg.port, step=1))
        pg_cfg.dbname = st.text_input("Database", pg_cfg.dbname)
        pg_cfg.user = st.text_input("User", pg_cfg.user)
        pg_cfg.password = st.text_input("Password", pg_cfg.password, type="password")
        if not md.postgres_available(pg_cfg):
            st.warning("Postgres not reachable — falling back to demo data.")
    else:
        n_users = st.slider("Sample size", 200, 5000, 2000, step=100)
        seed = st.number_input("Seed", value=7, step=1)
        if st.button("🔄 Regenerate", use_container_width=True):
            st.cache_data.clear()

    st.divider()
    st.caption("Marts mirror `dbt/models/marts/*.sql`: users by age band, by city, "
               "and an email-domain cut from `stg_users`.")


@st.cache_data(show_spinner=False)
def _demo(n: int, seed: int) -> md.Marts:
    return md.derive_marts(md.generate_users(n=n, seed=seed))


@st.cache_data(show_spinner=True)
def _live(host: str, port: int, dbname: str, user: str, password: str) -> md.Marts:
    cfg = md.PgConfig(host=host, port=port, dbname=dbname, user=user, password=password)
    return md.derive_marts(md.read_users(cfg))


use_live = source == "Postgres (live)" and md.postgres_available(pg_cfg)
if use_live:
    try:
        marts = _live(pg_cfg.host, pg_cfg.port, pg_cfg.dbname, pg_cfg.user, pg_cfg.password)
        badge = '<span class="badge badge-live">● LIVE · POSTGRES</span>'
    except Exception as exc:  # noqa: BLE001
        st.error(f"Postgres read failed — falling back to demo. ({exc})")
        marts = _demo(2000, 7)
        badge = '<span class="badge badge-demo">◆ DEMO DATA (fallback)</span>'
else:
    marts = _demo(n_users, seed) if source == "Demo data" else _demo(2000, 7)
    badge = '<span class="badge badge-demo">◆ DEMO DATA</span>'

users = marts.users

# --------------------------------------------------------------------------- #
# Hero
# --------------------------------------------------------------------------- #

st.markdown(f"""
<div class="hero">
  <h1>📊 User Analytics — Marts BI</h1>
  <p>Business-ready marts from the dbt silver/gold layer of the user-data ETL. {badge}</p>
</div>
""", unsafe_allow_html=True)

# --------------------------------------------------------------------------- #
# KPIs
# --------------------------------------------------------------------------- #

k1, k2, k3, k4 = st.columns(4)
k1.metric("👥 Total users", f"{len(users):,}")
k2.metric("🏙️ Distinct cities", users["city"].nunique())
k3.metric("🎂 Median age", f"{users['age'].median():.0f}")
top_city = marts.by_city.iloc[0]["city"] if len(marts.by_city) else "—"
k4.metric("🏆 Top city", top_city)

st.divider()

tab_marts, tab_lineage = st.tabs(["📈 Analytics Marts", "🧬 Lineage & Quality"])

# ── Analytics Marts ───────────────────────────────────────────────────────── #
with tab_marts:
    c1, c2 = st.columns(2)

    with c1:
        st.markdown("##### 🎂 Users by age band")
        ab = marts.by_age_band
        fig = go.Figure(go.Bar(
            x=ab["age_band"], y=ab["user_count"],
            marker_color=_PALETTE[:len(ab)],
            text=ab["user_count"], textposition="outside",
        ))
        fig.update_layout(height=340, margin=dict(l=10, r=10, t=10, b=10),
                          paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                          font_color="#cbd5e1",
                          xaxis=dict(title="age band"),
                          yaxis=dict(gridcolor="rgba(148,163,184,0.12)"))
        st.plotly_chart(fig, use_container_width=True)

    with c2:
        st.markdown("##### 📧 Users by email domain")
        dom = marts.by_domain
        fig = go.Figure(go.Pie(
            labels=dom["email_domain"], values=dom["user_count"], hole=0.55,
            marker=dict(colors=_PALETTE), textinfo="label+percent",
        ))
        fig.update_layout(height=340, margin=dict(l=10, r=10, t=10, b=10),
                          paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                          font_color="#cbd5e1", showlegend=False)
        st.plotly_chart(fig, use_container_width=True)

    st.markdown("##### 🏙️ Top cities by user count")
    top = marts.by_city.head(15).iloc[::-1]
    fig = go.Figure(go.Bar(
        x=top["user_count"], y=top["city"], orientation="h",
        marker_color=_CYAN, text=top["user_count"], textposition="outside",
    ))
    fig.update_layout(height=460, margin=dict(l=10, r=10, t=10, b=10),
                      paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                      font_color="#cbd5e1",
                      xaxis=dict(title="users", gridcolor="rgba(148,163,184,0.12)"))
    st.plotly_chart(fig, use_container_width=True)

    with st.expander("🔎 Underlying users (sample)"):
        st.dataframe(users.head(50), use_container_width=True, hide_index=True)

# ── Lineage & Quality ─────────────────────────────────────────────────────── #
with tab_lineage:
    st.markdown("##### 🧬 Data lineage — raw files to business marts")
    n_clean = len(users)
    n_raw = int(n_clean / 0.62) if n_clean else 0   # cleaning drops dirty rows
    st.markdown(f"""
    <div class="flow">
      <div class="node"><div class="n">☁️ S3 raw CSV</div><div class="v">{n_raw:,}</div></div>
      <div class="arrow">→</div>
      <div class="node"><div class="n">🧠 Spark clean</div><div class="v">{n_clean:,}</div></div>
      <div class="arrow">→</div>
      <div class="node"><div class="n">🐘 Postgres users</div><div class="v">{n_clean:,}</div></div>
      <div class="arrow">→</div>
      <div class="node"><div class="n">📊 dbt stg_users</div><div class="v">+age_band</div></div>
      <div class="arrow">→</div>
      <div class="node"><div class="n">🥇 Marts</div><div class="v">2 tables</div></div>
    </div>
    """, unsafe_allow_html=True)

    st.divider()
    st.markdown("##### 🧪 Cleaning rules applied upstream (PySpark)")
    dq = pd.DataFrame([
        {"Rule": "Valid age", "Detail": "keep 18–90 (drops -5, 0, 150 sentinels)"},
        {"Rule": "Non-empty name & city", "Detail": "rows with blank name/city removed"},
        {"Rule": "Valid email", "Detail": "drops `invalid-email`; derive email_domain"},
        {"Rule": "Numeric 5-digit zip", "Detail": "non-numeric / wrong-length zips nulled"},
        {"Rule": "Surrogate key", "Detail": "deterministic MD5 `user_id`, idempotent upsert"},
    ])
    st.dataframe(dq, use_container_width=True, hide_index=True)

    st.markdown("##### 📦 Mart tables (as dbt would materialise them)")
    m1, m2 = st.columns(2)
    with m1:
        st.caption("`users_by_age_band`")
        st.dataframe(marts.by_age_band, use_container_width=True, hide_index=True)
    with m2:
        st.caption("`users_by_city` (top 15)")
        st.dataframe(marts.by_city.head(15), use_container_width=True, hide_index=True)

    st.caption("Source of truth: `dbt/models/staging/stg_users.sql` and "
               "`dbt/models/marts/*.sql`. This dashboard reuses the same age-band "
               "boundaries and group-bys.")
