"""Spark tests for rejected-row provenance and the data-quality report."""

from clean_dirty_data_S3 import (
    EXPECTED_SCHEMA,
    clean_dataframe,
    data_quality_report,
    rejected_dataframe,
)

# Column order in EXPECTED_SCHEMA: id, name, email, phone, zip_code, age, city
VALID = ("1", "Wendy Christian", "wendy@example.com", "6912345678", "12345", "30", "Athens")
BAD_EMAIL = ("2", "Mark Anthony", "not-an-email", "6900000000", "54321", "40", "Berlin")
BAD_PHONE = ("3", "Jane Doe", "jane@example.com", "12345", "11111", "25", "Rome")
BAD_ZIP = ("4", "Joe Bloggs", "joe@example.com", "6911111111", "ABCDE", "50", "Paris")
BAD_AGE = ("5", "Old Person", "old@example.com", "6922222222", "22222", "150", "Madrid")
EMPTY_NAME = ("6", "", "x@example.com", "6933333333", "33333", "33", "Lisbon")
EMPTY_CITY = ("7", "No City", "nc@example.com", "6944444444", "44444", "44", "")


def _df(spark, *rows):
    return spark.createDataFrame(list(rows), schema=EXPECTED_SCHEMA)


def test_rejected_reasons(spark):
    df = _df(spark, BAD_EMAIL, BAD_PHONE, BAD_ZIP, BAD_AGE, EMPTY_NAME, EMPTY_CITY)
    by = {r["name"]: r["rejection_reason"] for r in rejected_dataframe(df).collect()}
    assert by["Mark Anthony"] == "invalid_email"
    assert by["Jane Doe"] == "invalid_phone"
    assert by["Joe Bloggs"] == "invalid_zip_code"
    assert by["Old Person"] == "invalid_age"
    assert by[""] == "invalid_name"
    assert by["No City"] == "invalid_city"


def test_valid_row_is_not_rejected(spark):
    assert rejected_dataframe(_df(spark, VALID)).count() == 0


def test_clean_and_rejected_partition_the_input(spark):
    df = _df(spark, VALID, BAD_EMAIL, BAD_PHONE, EMPTY_CITY)
    accepted = clean_dataframe(df).count()
    rejected = rejected_dataframe(df).count()
    assert accepted == 1
    assert rejected == 3
    assert accepted + rejected == 4  # every row lands in exactly one branch


def test_reason_precedence_is_contract_order(spark):
    # a row failing both email and phone is attributed to email (it comes first).
    row = ("9", "Two Fails", "bad-email", "bad-phone", "55555", "30", "Oslo")
    out = rejected_dataframe(_df(spark, row)).collect()
    assert out[0]["rejection_reason"] == "invalid_email"


def test_rejected_carries_provenance_columns(spark):
    cols = rejected_dataframe(_df(spark, BAD_EMAIL)).columns
    # original fields + the reason, so a human can debug the source row
    for c in ("name", "email", "phone", "zip_code", "age", "city", "rejection_reason"):
        assert c in cols


# --- data-quality report ---------------------------------------------------- #


def test_dq_report_counts(spark):
    df = _df(spark, VALID, VALID, BAD_EMAIL, BAD_ZIP, BAD_ZIP)
    report = data_quality_report(df)
    assert report.total == 5
    assert report.accepted == 2
    assert report.rejected == 3
    assert report.by_reason["invalid_zip_code"] == 2
    assert report.by_reason["invalid_email"] == 1
    assert report.accept_rate == 0.4


def test_dq_report_serialisable(spark):
    report = data_quality_report(_df(spark, VALID, BAD_EMAIL))
    d = report.to_dict()
    assert d["total"] == 2 and d["accepted"] == 1
    assert d["accept_rate"] == 0.5
    assert d["rejected_by_reason"] == {"invalid_email": 1}


def test_dq_report_empty_batch(spark):
    report = data_quality_report(_df(spark))
    assert report.total == 0
    assert report.accept_rate == 1.0
