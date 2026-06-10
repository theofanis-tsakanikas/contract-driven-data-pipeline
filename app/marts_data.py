"""Data layer for the Marts BI dashboard.

One interface over two sources:

    * **Demo mode** (default) — synthesises a clean ``users`` dataset and derives
      the same analytics the dbt models produce: the ``age_band`` enrichment from
      ``stg_users`` and the ``users_by_age_band`` / ``users_by_city`` marts. It
      needs no Airflow, Spark, S3 or Postgres. To make the "by city" chart read
      like a real BI view (concentration in large cities) it draws from a curated,
      weighted US-city list rather than fully-unique Faker cities — the shape the
      mart takes at scale.
    * **Postgres (live)** — reads the real ``users`` table the loader writes and
      computes the marts in pandas, mirroring the dbt SQL exactly.

Faithful to:
    * ``dbt/models/staging/stg_users.sql`` — ``age_band`` boundaries, ``email_domain``.
    * ``dbt/models/marts/users_by_age_band.sql`` / ``users_by_city.sql`` — group-bys.
    * ``scripts/load_to_db_final.py`` — the ``users`` table columns.
"""

from __future__ import annotations

import os
import random
from dataclasses import dataclass

import pandas as pd

# --------------------------------------------------------------------------- #
# Reference distributions (curated so the demo marts look like real BI)
# --------------------------------------------------------------------------- #

# (city, weight) — roughly population-ordered US metros.
_CITIES = [
    ("New York", 84), ("Los Angeles", 60), ("Chicago", 44), ("Houston", 41),
    ("Phoenix", 38), ("Philadelphia", 33), ("San Antonio", 30), ("San Diego", 28),
    ("Dallas", 27), ("San Jose", 22), ("Austin", 21), ("Jacksonville", 18),
    ("Fort Worth", 16), ("Columbus", 15), ("Charlotte", 14), ("Seattle", 14),
    ("Denver", 13), ("Boston", 12), ("Nashville", 11), ("Portland", 10),
]
_DOMAINS = [
    ("gmail.com", 46), ("yahoo.com", 18), ("hotmail.com", 14),
    ("outlook.com", 12), ("icloud.com", 7), ("aol.com", 3),
]


def age_band(age: int) -> str:
    """Age band, identical to the CASE in ``stg_users.sql``."""
    if 18 <= age <= 29:
        return "18-29"
    if 30 <= age <= 44:
        return "30-44"
    if 45 <= age <= 64:
        return "45-64"
    return "65+"


# --------------------------------------------------------------------------- #
# Demo synthesis
# --------------------------------------------------------------------------- #

def _weighted_choice(rng: random.Random, pairs: list[tuple[str, int]]) -> str:
    items, weights = zip(*pairs)
    return rng.choices(items, weights=weights, k=1)[0]


def generate_users(n: int = 2000, seed: int | None = 7) -> pd.DataFrame:
    """Synthesise a clean ``users`` frame with the stg-level derived columns.

    Ages skew toward working adults but span 18–90 (the valid range the cleaning
    step keeps); city and email-domain are drawn from the weighted reference
    lists so the marts show realistic concentration.
    """
    rng = random.Random(seed)
    rows = []
    for i in range(1, n + 1):
        # Triangular age distribution peaking around 38, clamped to [18, 90].
        age = int(max(18, min(90, rng.triangular(18, 90, 36))))
        city = _weighted_choice(rng, _CITIES)
        domain = _weighted_choice(rng, _DOMAINS)
        rows.append({
            "user_id": f"U{i:05d}",
            "name": f"User {i}",
            "email": f"user{i}@{domain}",
            "email_domain": domain,
            "phone": "69" + "".join(str(rng.randint(0, 9)) for _ in range(8)),
            "zip_code": rng.randint(10000, 99999),
            "age": age,
            "age_band": age_band(age),
            "city": city,
        })
    return pd.DataFrame(rows)


# --------------------------------------------------------------------------- #
# Mart derivation (mirrors the dbt marts)
# --------------------------------------------------------------------------- #

@dataclass
class Marts:
    users: pd.DataFrame
    by_age_band: pd.DataFrame
    by_city: pd.DataFrame
    by_domain: pd.DataFrame


_AGE_ORDER = ["18-29", "30-44", "45-64", "65+"]


def derive_marts(users: pd.DataFrame) -> Marts:
    """Build the marts from a users frame (mirrors the dbt group-bys)."""
    if "age_band" not in users.columns and "age" in users.columns:
        users = users.assign(age_band=users["age"].apply(age_band))
    if "email_domain" not in users.columns and "email" in users.columns:
        users = users.assign(
            email_domain=users["email"].str.split("@").str[-1].str.lower()
        )

    by_age = (
        users.groupby("age_band").size().reset_index(name="user_count")
        .set_index("age_band").reindex(_AGE_ORDER).fillna(0)
        .astype({"user_count": int}).reset_index()
    )
    by_city = (
        users.groupby("city").size().reset_index(name="user_count")
        .sort_values("user_count", ascending=False).reset_index(drop=True)
    )
    by_domain = (
        users.groupby("email_domain").size().reset_index(name="user_count")
        .sort_values("user_count", ascending=False).reset_index(drop=True)
    )
    return Marts(users=users, by_age_band=by_age, by_city=by_city, by_domain=by_domain)


# --------------------------------------------------------------------------- #
# Postgres (live) source
# --------------------------------------------------------------------------- #

@dataclass
class PgConfig:
    host: str = os.getenv("DB_HOST", "localhost")
    port: int = int(os.getenv("DB_PORT", "5432"))
    dbname: str = os.getenv("TARGET_DB", "user_data")
    user: str = os.getenv("DB_USER", "airflow")
    password: str = os.getenv("DB_PASS", "")


def postgres_available(cfg: PgConfig) -> bool:
    """True if the target Postgres is reachable and has a ``users`` table."""
    try:
        import psycopg2  # lazy import
        conn = psycopg2.connect(
            host=cfg.host, port=cfg.port, dbname=cfg.dbname,
            user=cfg.user, password=cfg.password, connect_timeout=2,
        )
        with conn.cursor() as cur:
            cur.execute("SELECT to_regclass('public.users')")
            exists = cur.fetchone()[0] is not None
        conn.close()
        return exists
    except Exception:
        return False


def read_users(cfg: PgConfig) -> pd.DataFrame:
    """Read the ``users`` table from Postgres and add the stg-derived columns."""
    import psycopg2  # lazy import
    conn = psycopg2.connect(
        host=cfg.host, port=cfg.port, dbname=cfg.dbname,
        user=cfg.user, password=cfg.password, connect_timeout=3,
    )
    try:
        df = pd.read_sql(
            "SELECT user_id, name, email, phone, zip_code, age, city FROM users", conn
        )
    finally:
        conn.close()
    df["age_band"] = df["age"].apply(lambda a: age_band(int(a)) if pd.notna(a) else "65+")
    df["email_domain"] = df["email"].fillna("").str.split("@").str[-1].str.lower()
    return df
