# Data Dictionary & Contract — users

> Generated from `scripts/data_contract.py` by `python scripts/data_contract.py`. Do not edit by hand.

## Validation contract

Every ingested row must satisfy all of the rules below; a row that fails is quarantined to the rejects output tagged with the first rule it violated (lineage), and the run's data-quality report records the counts.

| Field | PII class | Rule | Constraint | Rejection reason |
| --- | --- | --- | --- | --- |
| `name` | direct_identifier | required, non-empty (trimmed) | — | `invalid_name` |
| `email` | direct_identifier | matches pattern | `^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$` | `invalid_email` |
| `phone` | direct_identifier | matches pattern | `^69\d{8}$` | `invalid_phone` |
| `zip_code` | quasi_identifier | matches pattern | `^\d{5}$` | `invalid_zip_code` |
| `age` | quasi_identifier | integer in range | [18, 99] | `invalid_age` |
| `city` | quasi_identifier | required, non-empty (trimmed) | — | `invalid_city` |

## Personal data handling

- **Direct identifiers** (`name`, `email`, `phone`) are never exposed as a raw key. The loaded `user_id` is a deterministic **MD5 pseudonym** of `name || email || phone` — the same person maps to the same surrogate without storing a natural key.
- **Quasi-identifiers** (`zip_code`, `age`, `city`) are retained for analytics; combined they can be re-identifying, so downstream marts aggregate rather than expose row-level joins.
- The raw S3 object is retained in a date-partitioned key for auditability; rejects are quarantined with their reason rather than silently dropped.

