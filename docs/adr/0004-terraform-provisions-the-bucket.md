# ADR-0004: Terraform provisions the bucket, the pipeline only writes objects

- **Status:** Accepted
- **Date:** 2026-06 (recorded 2026-08)

## Context

The first version of the ingestion task called `boto3`'s `create_bucket()` if the bucket did not
exist. It is a common pattern and it works on the first run.

It also means the pipeline's IAM identity must hold `s3:CreateBucket` — an account-wide permission,
because bucket creation is not scopeable to a name that does not exist yet. A credential that can
create buckets can create *any* bucket in the account. And a bucket created imperatively at runtime
has whatever defaults the SDK gives it: no encryption configuration, no public-access block, no
versioning, no lifecycle rules, and no record anywhere of how it came to exist.

## Decision

Move bucket creation into [`infra/terraform/s3.tf`](../../infra/terraform/s3.tf), and reduce the
pipeline's IAM policy to exactly what running the pipeline needs:

- `s3:ListBucket` + `s3:GetBucketLocation` on the lake bucket
- `s3:GetObject` + `s3:PutObject` on its objects
- **no** `s3:CreateBucket`, **no** `s3:DeleteObject`

Declaring the bucket in Terraform means it also gets, in the same commit that creates it: a full
public-access block, `AES256` server-side encryption, versioning, a lifecycle rule expiring the `raw/`
prefix after 30 days (with a 7-day non-current-version tail), and an abort rule for incomplete
multipart uploads. Athena query results go to a **separate** bucket with its own shorter expiry, so
query output never lands in a data zone.

Deletion is deliberately absent from the pipeline's policy: retention is the lifecycle rules' job.

## Alternatives rejected

- **`create_bucket()` with a narrow policy.** Not possible — the permission cannot be scoped to a
  bucket that does not exist.
- **Create the bucket by hand in the console, once.** Removes the permission problem and replaces it
  with an undocumented, unreproducible resource whose settings nobody can review.
- **One bucket for data and Athena results.** Simpler, and wrong: query results would accumulate
  inside the zones the Glue crawler catalogues, and the crawler would start cataloguing them.

## Consequences

- A compromised pipeline credential can write objects into one bucket and read them back. It cannot
  create buckets, cannot delete history, and cannot reach anything else in the account.
- The bucket's security posture is reviewable as a diff, not discoverable by clicking through the
  console.
- The raw zone stops being the "objects retained forever" cost trap, because the lifecycle rule ships
  with the bucket rather than being remembered later.
- `terraform destroy` now genuinely returns the project to zero cost, since Terraform owns everything
  it created.
