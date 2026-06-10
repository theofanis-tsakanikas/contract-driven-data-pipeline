"""Unit tests for load_to_db_final.load_to_database with a mocked psycopg2.

The CI ``smoke`` job already exercises the happy path against a real
``postgres:13`` (loads a CSV fixture, asserts the row count). These tests cover
the edge cases that a live-DB smoke test can't easily assert: the failure path
(missing file raises) and the empty-CSV short-circuit, the exact INSERT statement
(``ON CONFLICT (user_id) DO NOTHING``), the DataFrame→row-tuple mapping handed
to ``execute_values``, and the CREATE DATABASE branch. psycopg2 and
``execute_values`` are mocked, so no database is touched and the function's
behavior is unchanged.
"""
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

import load_to_db_final


@pytest.fixture
def db_env(monkeypatch):
    """Populate the env vars load_to_database reads (connections are mocked)."""
    for key, val in {
        "DB_HOST": "localhost",
        "DB_PORT": "5432",
        "DEFAULT_DB": "postgres",
        "TARGET_DB": "user_data",
        "DB_USER": "airflow",
        "DB_PASS": "airflow",
        "LOCAL_CLEAN_PATH": "/tmp/clean_data.csv",
    }.items():
        monkeypatch.setenv(key, val)


def _sample_df():
    return pd.DataFrame(
        [
            ("a" * 32, "Wendy Christian", "wendy@example.com", "6912345678", 12345, 30, "Athens"),
            ("b" * 32, "Mark Anthony", "mark@example.com", "6900000000", 54321, 40, "Berlin"),
        ],
        columns=["user_id", "name", "email", "phone", "zip_code", "age", "city"],
    )


# --- Short-circuits: no DB connection should ever be opened ---

def test_missing_file_raises(db_env):
    with patch.object(load_to_db_final.os.path, "exists", return_value=False), \
         patch.object(load_to_db_final, "psycopg2") as mock_pg, \
         patch.object(load_to_db_final.pd, "read_csv") as mock_read:
        with pytest.raises(FileNotFoundError):
            load_to_db_final.load_to_database()
    mock_read.assert_not_called()
    mock_pg.connect.assert_not_called()


def test_empty_csv_short_circuits(db_env):
    empty = pd.DataFrame(columns=["user_id", "name", "email", "phone", "zip_code", "age", "city"])
    with patch.object(load_to_db_final.os.path, "exists", return_value=True), \
         patch.object(load_to_db_final.pd, "read_csv", return_value=empty), \
         patch.object(load_to_db_final, "psycopg2") as mock_pg, \
         patch.object(load_to_db_final, "execute_values") as mock_ev:
        load_to_db_final.load_to_database()
    mock_pg.connect.assert_not_called()
    mock_ev.assert_not_called()


# --- Happy path: statement shape and row-tuple mapping ---

def test_insert_statement_and_row_mapping(db_env):
    df = _sample_df()
    cursor = MagicMock()
    cursor.fetchone.return_value = (1,)  # target DB already exists → no CREATE DATABASE
    cursor.rowcount = 2  # execute_values inserted both rows
    conn = MagicMock()
    conn.cursor.return_value = cursor

    with patch.object(load_to_db_final.os.path, "exists", return_value=True), \
         patch.object(load_to_db_final.pd, "read_csv", return_value=df), \
         patch.object(load_to_db_final.psycopg2, "connect", return_value=conn), \
         patch.object(load_to_db_final, "execute_values") as mock_ev:
        load_to_db_final.load_to_database()

    mock_ev.assert_called_once()
    _, insert_query, data = mock_ev.call_args.args
    assert "INSERT INTO users (user_id, name, email, phone, zip_code, age, city)" in insert_query
    assert "ON CONFLICT (user_id) DO NOTHING" in insert_query
    # Each DataFrame row is mapped to a plain tuple in column order.
    assert data == [tuple(row) for row in df.to_numpy()]
    assert len(data) == 2
    assert all(isinstance(t, tuple) for t in data)


def test_creates_database_when_absent(db_env):
    df = _sample_df()
    cursor = MagicMock()
    cursor.fetchone.return_value = None  # target DB missing → CREATE DATABASE path
    cursor.rowcount = 2
    conn = MagicMock()
    conn.cursor.return_value = cursor

    with patch.object(load_to_db_final.os.path, "exists", return_value=True), \
         patch.object(load_to_db_final.pd, "read_csv", return_value=df), \
         patch.object(load_to_db_final.psycopg2, "connect", return_value=conn), \
         patch.object(load_to_db_final, "execute_values"):
        load_to_db_final.load_to_database()

    from psycopg2 import sql

    composed = [c.args[0] for c in cursor.execute.call_args_list if isinstance(c.args[0], sql.Composed)]
    assert composed, "expected a composed CREATE DATABASE statement"
    assert sql.Identifier("user_data") in list(composed[0])
