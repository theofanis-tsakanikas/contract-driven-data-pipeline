"""Tests for the dirty-data generator's anomaly injection (no S3 / boto3 calls).

generate_dirty_data_S3 builds a boto3 client at import time (conftest sets a
region so that succeeds), but only ``main`` touches S3. These tests exercise
the pure random_* helpers and the CSV writer, asserting the generator actually
injects the malformed values the PySpark transform is built to filter out.
"""
import csv
import random
import re

import generate_dirty_data_S3 as gen

GREEK_PHONE = re.compile(r"^69\d{8}$")
FIVE_DIGIT_ZIP = re.compile(r"^\d{5}$")


def _sample(fn, n=400):
    random.seed(1234)
    return [fn() for _ in range(n)]


def test_phone_injects_valid_and_anomalies():
    vals = _sample(gen.random_phone)
    assert any(GREEK_PHONE.match(v) for v in vals), "expected some valid Greek mobiles"
    assert "" in vals, "expected some empty phones"
    # The Faker branch produces formatted US numbers that the regex rejects.
    assert any(v and not GREEK_PHONE.match(v) for v in vals)


def test_zip_injects_valid_and_anomalies():
    vals = _sample(gen.random_zip)
    assert any(FIVE_DIGIT_ZIP.match(v) for v in vals), "expected valid 5-digit zips"
    assert any(not FIVE_DIGIT_ZIP.match(v) for v in vals), "expected malformed zips"


def test_email_injects_invalid_placeholder():
    random.seed(1234)
    vals = [gen.random_email("Jane Doe") for _ in range(200)]
    assert "invalid-email" in vals
    assert "jane.doe@" in next(v for v in vals if v != "invalid-email")


def test_age_injects_out_of_range_anomalies():
    vals = _sample(gen.random_age)
    assert any(18 <= v <= 90 for v in vals), "expected realistic adult ages"
    assert any(v in (-5, 0, 150) for v in vals), "expected out-of-range anomalies"


def test_name_and_city_inject_empties():
    assert "" in _sample(gen.random_name)
    assert "" in _sample(gen.random_city)


def test_create_dirty_data_writes_configured_rows_with_header(tmp_path, monkeypatch):
    monkeypatch.setenv("N_DIRTY_RECORDS", "100")  # explicit: don't couple to the default
    out = tmp_path / "dirty.csv"
    random.seed(1234)
    gen.create_dirty_data(str(out))

    with open(out, newline="", encoding="utf-8-sig") as f:
        rows = list(csv.reader(f))

    assert rows[0] == ["id", "name", "email", "phone", "zip_code", "age", "city"]
    assert len(rows) == 101  # header + 100 records
    assert [r[0] for r in rows[1:]] == [str(i) for i in range(1, 101)]
