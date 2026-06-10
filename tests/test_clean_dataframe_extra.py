"""Additional coverage for clean_dirty_data_S3.clean_dataframe.

Complements test_clean_dataframe.py by filling the gaps the audit found:
the exact MD5 user_id composition, Greek-phone / zip regex edge cases, the
just-outside-the-boundary ages (17/100), NULL (not just empty) name/city, the
age string→int cast type, and that the source ``id`` column is pruned. The
happy path, trimming, the basic invalid-row drops, the 18/99 inclusive
boundaries, and column order already live in test_clean_dataframe.py and are
not repeated here.
"""
import hashlib

from clean_dirty_data_S3 import EXPECTED_SCHEMA, clean_dataframe

# EXPECTED_SCHEMA order: id, name, email, phone, zip_code, age, city
A_VALID_ROW = ("1", "Wendy Christian", "wendy.christian@example.com", "6912345678", "12345", "30", "Athens")


def _df(spark, *rows):
    return spark.createDataFrame(list(rows), schema=EXPECTED_SCHEMA)


# --- MD5 user_id composition (not just determinism) ---

def test_user_id_is_md5_of_name_email_phone(spark):
    """user_id == md5(name || email || phone) with '||' as the separator."""
    out = clean_dataframe(_df(spark, A_VALID_ROW)).collect()[0]
    expected = hashlib.md5("||".join(["Wendy Christian", "wendy.christian@example.com", "6912345678"]).encode()).hexdigest()
    assert out["user_id"] == expected


def test_user_id_uses_trimmed_values(spark):
    """The hash is computed from the trimmed fields, so padded input hashes the same."""
    padded = ("9", "  Wendy Christian  ", "  wendy.christian@example.com ", " 6912345678 ", "12345", "30", "Athens")
    out = clean_dataframe(_df(spark, padded)).collect()[0]
    expected = hashlib.md5("||".join(["Wendy Christian", "wendy.christian@example.com", "6912345678"]).encode()).hexdigest()
    assert out["user_id"] == expected


def test_distinct_inputs_yield_distinct_user_ids(spark):
    other = ("2", "Mark Anthony", "mark@example.com", "6900000000", "54321", "40", "Berlin")
    ids = {r["user_id"] for r in clean_dataframe(_df(spark, A_VALID_ROW, other)).collect()}
    assert len(ids) == 2


# --- Greek mobile phone regex: ^69\d{8}$ ---

def test_valid_greek_phone_passes(spark):
    assert len(clean_dataframe(_df(spark, ("1", "A B", "a@b.com", "6900000000", "12345", "30", "X"))).collect()) == 1


def test_phone_wrong_prefix_dropped(spark):
    # starts with 68, not 69
    assert clean_dataframe(_df(spark, ("1", "A B", "a@b.com", "6800000000", "12345", "30", "X"))).collect() == []


def test_phone_too_few_digits_dropped(spark):
    # 69 + 7 digits = 9 chars
    assert clean_dataframe(_df(spark, ("1", "A B", "a@b.com", "690000000", "12345", "30", "X"))).collect() == []


def test_phone_too_many_digits_dropped(spark):
    # 69 + 9 digits = 11 chars
    assert clean_dataframe(_df(spark, ("1", "A B", "a@b.com", "6900000000123"[:11], "12345", "30", "X"))).collect() == []


# --- Zip code regex: ^\d{5}$ ---

def test_valid_zip_passes(spark):
    assert len(clean_dataframe(_df(spark, ("1", "A B", "a@b.com", "6900000000", "00000", "30", "X"))).collect()) == 1


def test_four_digit_zip_dropped(spark):
    assert clean_dataframe(_df(spark, ("1", "A B", "a@b.com", "6900000000", "1234", "30", "X"))).collect() == []


def test_six_digit_zip_dropped(spark):
    assert clean_dataframe(_df(spark, ("1", "A B", "a@b.com", "6900000000", "123456", "30", "X"))).collect() == []


# --- Age boundaries: just outside 18..99 ---

def test_age_17_dropped(spark):
    assert clean_dataframe(_df(spark, ("1", "A B", "a@b.com", "6900000000", "12345", "17", "X"))).collect() == []


def test_age_100_dropped(spark):
    assert clean_dataframe(_df(spark, ("1", "A B", "a@b.com", "6900000000", "12345", "100", "X"))).collect() == []


def test_age_is_cast_to_int_type(spark):
    out = clean_dataframe(_df(spark, A_VALID_ROW))
    assert dict(out.dtypes)["age"] == "int"


# --- NULL (not just empty-string) name / city are dropped ---

def test_null_name_dropped(spark):
    assert clean_dataframe(_df(spark, ("1", None, "a@b.com", "6900000000", "12345", "30", "X"))).collect() == []


def test_null_city_dropped(spark):
    assert clean_dataframe(_df(spark, ("1", "A B", "a@b.com", "6900000000", "12345", "30", None))).collect() == []


# --- Column pruning: source id never survives ---

def test_source_id_column_is_dropped(spark):
    assert "id" not in clean_dataframe(_df(spark, A_VALID_ROW)).columns
