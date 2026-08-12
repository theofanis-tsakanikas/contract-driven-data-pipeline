# ADR-0003: An MD5 surrogate key instead of a natural key

- **Status:** Accepted
- **Date:** 2026-06 (recorded 2026-08)

## Context

The `users` table needs a stable key for the idempotent upsert (`ON CONFLICT ... DO NOTHING`), so a
re-run of the same day's data does not duplicate rows. The obvious candidates are all direct
identifiers: email, phone, or name + postcode.

Making a direct identifier the primary key means it is indexed, it appears in every foreign key, and
it is the value people paste into tickets and screenshots. It is the single worst place to put
personal data.

## Decision

Derive a surrogate key in the Spark transform:

```python
df.withColumn(SURROGATE_KEY, md5(concat_ws("||", *SURROGATE_SOURCES)))
```

where `SURROGATE_SOURCES` is `name ‖ email ‖ phone`. The loaded table carries `user_id` as its unique
key; the direct identifiers are still loaded as ordinary columns, but nothing keys on them. The
derivation is documented as a control in the contract's PII classification, where `user_id` is
classified `PSEUDONYMISED`.

Because the hash is deterministic, the same person hashes to the same key across runs, which is what
makes `ON CONFLICT (user_id) DO NOTHING` idempotent.

## Alternatives rejected

- **Email as the primary key.** Idempotent and free, but it puts a direct identifier in every index
  and every join. Also wrong on the merits: email is not immutable.
- **A UUID per row.** Would break idempotency outright — a re-run generates new UUIDs, `ON CONFLICT`
  never fires, and every run duplicates the table.
- **A database `SERIAL` alone.** Same problem: no natural way to recognise a row already loaded.
  (`id SERIAL PRIMARY KEY` does exist on the table, but as a surrogate ordering key, not as the
  conflict target.)
- **HMAC-SHA256 with a secret key.** Cryptographically the right answer, and what a deployment
  should use. Rejected *here* because the key would become a real secret to store and rotate, for
  data that is synthetic. The gap is stated explicitly in [SECURITY.md](../../SECURITY.md#4-md5-is-pseudonymisation-not-anonymisation).

## Consequences

- The primary key carries no direct identifier, and the load is idempotent — one decision buys both.
- **This is pseudonymisation, not anonymisation.** The hash is unsalted and MD5 is fast; over a
  realistic candidate space the mapping is reversible by brute force, and under GDPR the result is
  still personal data. Claiming otherwise would be the kind of overstatement that makes every other
  claim in this repository suspect.
- Changing `SURROGATE_SOURCES` changes every key, so it is a breaking change to the table, not a
  refactor.
