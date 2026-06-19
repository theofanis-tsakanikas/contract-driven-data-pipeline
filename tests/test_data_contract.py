"""Pure tests for the data contract + generated data dictionary (no Spark)."""

from pathlib import Path

import data_contract as dc

REPO_ROOT = Path(__file__).resolve().parents[1]


def test_contract_fields_and_order():
    assert [r.field for r in dc.CONTRACT] == ["name", "email", "phone", "zip_code", "age", "city"]


def test_reasons_are_unique_and_ordered():
    reasons = dc.reasons()
    assert reasons == (
        "invalid_name",
        "invalid_email",
        "invalid_phone",
        "invalid_zip_code",
        "invalid_age",
        "invalid_city",
    )
    assert len(set(reasons)) == len(reasons)


def test_pii_fields_are_the_direct_identifiers():
    assert dc.pii_fields() == ("name", "email", "phone")


def test_surrogate_is_built_from_direct_identifiers():
    # the pseudonymised key must derive from exactly the direct identifiers
    assert set(dc.SURROGATE_SOURCES) == set(dc.pii_fields())
    assert dc.SURROGATE_KEY == "user_id"


def test_clean_columns_lead_with_surrogate():
    assert dc.CLEAN_COLUMNS[0] == dc.SURROGATE_KEY
    assert set(dc.CLEAN_COLUMNS) == {"user_id", "name", "email", "phone", "zip_code", "age", "city"}


def test_data_dictionary_documents_pseudonymisation_and_pii():
    doc = dc.render_data_dictionary()
    assert "MD5 pseudonym" in doc
    assert "direct_identifier" in doc and "quasi_identifier" in doc
    for reason in dc.reasons():
        assert f"`{reason}`" in doc


def test_committed_data_dictionary_in_sync():
    rc = dc.main(["--check", "--root", str(REPO_ROOT)])
    assert rc == 0, "docs/governance/DATA_DICTIONARY.md is stale — run `python scripts/data_contract.py`"
