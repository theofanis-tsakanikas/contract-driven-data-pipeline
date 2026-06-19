"""The user data contract — single source of truth for validation, lineage, and PII.

The cleaning stage enforces a contract on every ingested row: required fields, formats
(email, Greek mobile, 5-digit postcode), and an adult age range. Until now those rules
lived inline in ``clean_dataframe`` and a rejected row simply vanished — no record of why
it was dropped. This module makes the contract **explicit and declarative** so three things
can be built from one definition and can never disagree:

* the accept filter (``clean_dataframe``),
* the rejection reason for every dropped row (lineage / provenance — `rejected_dataframe`),
* the PII classification and the generated data dictionary.

It is pure (no Spark, no I/O) so the contract, the reasons, and the documentation are all
unit-testable in isolation. Maps to Readiness-Framework dimension 1 (Data quality &
lineage): *schema contracts on every ingestion boundary, enforced — not assumed; sensitive
fields classified at the data layer.*
"""

from __future__ import annotations

from dataclasses import dataclass

# Rule kinds the Spark layer knows how to compile (see clean_dirty_data_S3._rule_ok).
NON_EMPTY = "non_empty"  # trimmed string, length > 0
REGEX = "regex"  # trimmed string matches `pattern`
INT_RANGE = "int_range"  # cast to int, within [minimum, maximum]

# PII classifications (data-layer sensitivity).
DIRECT_IDENTIFIER = "direct_identifier"  # identifies a person on its own (name, email, phone)
QUASI_IDENTIFIER = "quasi_identifier"  # identifying in combination (zip, age, city)
PSEUDONYMISED = "pseudonymised"  # surrogate derived from PII (user_id)

# The email pattern enforced by the contract (unchanged from the original cleaning logic).
EMAIL_REGEX = r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"
# Greek mobile: 69 followed by 8 digits.
PHONE_REGEX = r"^69\d{8}$"
# Exactly five digits.
ZIP_REGEX = r"^\d{5}$"


@dataclass(frozen=True)
class FieldRule:
    """One field's validation rule, rejection reason, and PII classification."""

    field: str
    kind: str
    reason: str  # rejection_reason emitted when this rule fails
    description: str
    pii: str
    pattern: str | None = None
    minimum: int | None = None
    maximum: int | None = None


# The contract, in the order rules are evaluated. The order is the rejection-reason
# precedence: a row failing several rules is attributed to the first one here.
CONTRACT: tuple[FieldRule, ...] = (
    FieldRule("name", NON_EMPTY, "invalid_name", "Full name (required, non-empty).", DIRECT_IDENTIFIER),
    FieldRule(
        "email", REGEX, "invalid_email", "Email address (RFC-ish format).", DIRECT_IDENTIFIER, pattern=EMAIL_REGEX
    ),
    FieldRule(
        "phone", REGEX, "invalid_phone", "Greek mobile number (69 + 8 digits).", DIRECT_IDENTIFIER, pattern=PHONE_REGEX
    ),
    FieldRule(
        "zip_code", REGEX, "invalid_zip_code", "5-digit postcode.", QUASI_IDENTIFIER, pattern=ZIP_REGEX
    ),
    FieldRule("age", INT_RANGE, "invalid_age", "Adult age in [18, 99].", QUASI_IDENTIFIER, minimum=18, maximum=99),
    FieldRule("city", NON_EMPTY, "invalid_city", "City (required, non-empty).", QUASI_IDENTIFIER),
)

# The surrogate key derived from the direct identifiers (documented as a control).
SURROGATE_KEY = "user_id"
SURROGATE_SOURCES = ("name", "email", "phone")

# Output column order produced by clean_dataframe.
CLEAN_COLUMNS: tuple[str, ...] = (SURROGATE_KEY, "name", "email", "phone", "zip_code", "age", "city")


def reasons() -> tuple[str, ...]:
    """All rejection-reason labels, in contract order."""
    return tuple(r.reason for r in CONTRACT)


def pii_fields() -> tuple[str, ...]:
    return tuple(r.field for r in CONTRACT if r.pii == DIRECT_IDENTIFIER)


# --------------------------------------------------------------------------- #
# Data dictionary (generated documentation)
# --------------------------------------------------------------------------- #


def render_data_dictionary() -> str:
    """Render the data contract + PII classification as Markdown (generated doc)."""
    rule_text = {
        NON_EMPTY: "required, non-empty (trimmed)",
        REGEX: "matches pattern",
        INT_RANGE: "integer in range",
    }
    rows = []
    for r in CONTRACT:
        if r.kind == REGEX:
            constraint = f"`{r.pattern}`"
        elif r.kind == INT_RANGE:
            constraint = f"[{r.minimum}, {r.maximum}]"
        else:
            constraint = "—"
        rows.append(f"| `{r.field}` | {r.pii} | {rule_text[r.kind]} | {constraint} | `{r.reason}` |")

    out = [
        "# Data Dictionary & Contract — users",
        "",
        "> Generated from `scripts/data_contract.py` by `python scripts/data_contract.py`. "
        "Do not edit by hand.",
        "",
        "## Validation contract",
        "",
        "Every ingested row must satisfy all of the rules below; a row that fails is quarantined "
        "to the rejects output tagged with the first rule it violated (lineage), and the run's "
        "data-quality report records the counts.",
        "",
        "| Field | PII class | Rule | Constraint | Rejection reason |",
        "| --- | --- | --- | --- | --- |",
        *rows,
        "",
        "## Personal data handling",
        "",
        f"- **Direct identifiers** ({', '.join(f'`{f}`' for f in pii_fields())}) are never exposed as a "
        f"raw key. The loaded `{SURROGATE_KEY}` is a deterministic **MD5 pseudonym** of "
        f"`{' || '.join(SURROGATE_SOURCES)}` — the same person maps to the same surrogate without "
        "storing a natural key.",
        "- **Quasi-identifiers** (`zip_code`, `age`, `city`) are retained for analytics; combined they "
        "can be re-identifying, so downstream marts aggregate rather than expose row-level joins.",
        "- The raw S3 object is retained in a date-partitioned key for auditability; rejects are "
        "quarantined with their reason rather than silently dropped.",
        "",
    ]
    return "\n".join(out) + "\n"


def main(argv: list[str] | None = None) -> int:
    import argparse
    from pathlib import Path

    parser = argparse.ArgumentParser(description="Generate / check the users data dictionary.")
    parser.add_argument("--check", action="store_true", help="fail if the committed doc is stale")
    parser.add_argument(
        "--root", default=str(Path(__file__).resolve().parent.parent), help="repo root"
    )
    args = parser.parse_args(argv)

    doc_path = Path(args.root) / "docs" / "governance" / "DATA_DICTIONARY.md"
    content = render_data_dictionary()

    if args.check:
        existing = doc_path.read_text(encoding="utf-8") if doc_path.is_file() else ""
        if existing != content:
            print(f"STALE data dictionary (run `python scripts/data_contract.py`): {doc_path}")
            return 1
        print("data dictionary is up to date.")
        return 0

    doc_path.parent.mkdir(parents=True, exist_ok=True)
    doc_path.write_text(content, encoding="utf-8")
    print(f"wrote {doc_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
