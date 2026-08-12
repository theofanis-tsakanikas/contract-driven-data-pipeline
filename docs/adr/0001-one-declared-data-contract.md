# ADR-0001: One declared data contract as the single source of truth

- **Status:** Accepted
- **Date:** 2026-06 (recorded 2026-08)

## Context

The pipeline has to answer four separate questions about the same seven fields:

1. Which rows are accepted? (the Spark filter)
2. Why was a row rejected? (the `rejection_reason` written to the quarantine zone)
3. Which fields are personal data, and of what kind? (the PII classification)
4. What does the published data dictionary say?

The default way these get written is four places: a chain of `.filter()` calls in the Spark job,
a set of string literals for the reasons, a comment somewhere about PII, and a hand-maintained
Markdown table. Every one of those is correct on the day it is written. Within a month they
disagree, and the documentation is the one that is wrong — silently, because nothing checks it.

## Decision

Declare each field once, as data, in [`scripts/data_contract.py`](../../scripts/data_contract.py):

```python
FieldRule("age", INT_RANGE, "invalid_age", "Adult age in [18, 99].",
          QUASI_IDENTIFIER, minimum=18, maximum=99)
```

One `FieldRule` carries the field, the validation predicate, the rejection reason, the human
description and the PII class. Six rules describe the whole contract. Everything downstream is
**derived** from that list:

- `clean_dataframe()` builds the accept filter from it.
- `rejected_dataframe()` tags each failing row with the first rule it violated.
- `docs/governance/DATA_DICTIONARY.md` is *generated* by `python scripts/data_contract.py`.
- CI's `lint` job runs `python scripts/data_contract.py --check` and **fails the build** if the
  committed dictionary no longer matches the declared rules.

The module is pure stdlib — no Spark, no pandas — so the `--check` gate runs in a second and the
rules are unit-testable on their own.

## Alternatives rejected

- **Great Expectations / Soda.** A configuration surface and a runtime dependency far heavier than
  six field rules justify. They would also own the *detection* while the rejection reasons, the PII
  classification and the dictionary stayed somewhere else — which is the problem, not the solution.
- **A JSON/YAML contract file.** Attractive, and it is what a multi-team platform should do. Rejected
  here because a Python dataclass gives type checking and a generator function for free, and the
  contract has exactly one consumer team.
- **Documentation by convention.** Rejected on the grounds that undocumented drift is the failure
  mode this ADR exists to prevent.

## Consequences

- Adding a field is one line, and the dictionary regenerates. Forgetting to regenerate turns CI red.
- The data dictionary is trustworthy because it cannot be stale — it is not written, it is derived.
- The contract is the natural place to hang anything else field-shaped later (masking, retention
  class, ownership) without touching the Spark job.
- The coupling is real: a change to the contract is a change to the accepted data. That is the point,
  and it is why the `--check` gate exists.
