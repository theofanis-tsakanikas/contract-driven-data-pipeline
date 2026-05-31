"""Unit tests for the pure PySpark transformation in clean_dirty_data_S3.clean_dataframe.

These exercise the data contract (the same rules described in the README) without
any S3 or filesystem I/O.
"""
from clean_dirty_data_S3 import EXPECTED_SCHEMA, clean_dataframe

# Column order in EXPECTED_SCHEMA: id, name, email, phone, zip_code, age, city
A_VALID_ROW = ("1", "Wendy Christian", "wendy.christian@example.com", "6912345678", "12345", "30", "Athens")

CLEAN_COLUMNS = ["user_id", "name", "email", "phone", "zip_code", "age", "city"]


def _df(spark, *rows):
    return spark.createDataFrame(list(rows), schema=EXPECTED_SCHEMA)


def test_valid_row_passes(spark):
    out = clean_dataframe(_df(spark, A_VALID_ROW)).collect()
    assert len(out) == 1
    row = out[0]
    # id is dropped; user_id (md5 hex) is added; age is cast to int.
    assert row["age"] == 30
    assert len(row["user_id"]) == 32
    assert row["name"] == "Wendy Christian"


def test_output_columns_and_order(spark):
    df = clean_dataframe(_df(spark, A_VALID_ROW))
    assert df.columns == CLEAN_COLUMNS


def test_whitespace_is_trimmed(spark):
    row = ("2", "  Mark Anthony  ", "  mark@example.com  ", "  6900000000  ", " 54321 ", "40", "  Berlin  ")
    out = clean_dataframe(_df(spark, row)).collect()
    assert len(out) == 1
    assert out[0]["name"] == "Mark Anthony"
    assert out[0]["city"] == "Berlin"
    assert out[0]["email"] == "mark@example.com"


def test_invalid_rows_are_dropped(spark):
    bad_email = ("3", "Bad Email", "invalid-email", "6912345678", "12345", "30", "Athens")
    bad_phone = ("4", "Bad Phone", "a@b.com", "12345", "12345", "30", "Athens")
    bad_zip = ("5", "Bad Zip", "a@b.com", "6912345678", "ABCDE", "30", "Athens")
    underage = ("6", "Too Young", "a@b.com", "6912345678", "12345", "10", "Athens")
    overage = ("7", "Too Old", "a@b.com", "6912345678", "12345", "150", "Athens")
    empty_name = ("8", "", "a@b.com", "6912345678", "12345", "30", "Athens")
    empty_city = ("9", "No City", "a@b.com", "6912345678", "12345", "30", "")

    out = clean_dataframe(
        _df(spark, bad_email, bad_phone, bad_zip, underage, overage, empty_name, empty_city)
    ).collect()
    assert out == []


def test_user_id_is_deterministic(spark):
    first = clean_dataframe(_df(spark, A_VALID_ROW)).collect()[0]["user_id"]
    second = clean_dataframe(_df(spark, A_VALID_ROW)).collect()[0]["user_id"]
    assert first == second


def test_age_boundaries_are_inclusive(spark):
    edge_low = ("10", "Edge Low", "low@example.com", "6911111111", "11111", "18", "Athens")
    edge_high = ("11", "Edge High", "high@example.com", "6922222222", "22222", "99", "Athens")
    out = clean_dataframe(_df(spark, edge_low, edge_high)).collect()
    assert len(out) == 2
