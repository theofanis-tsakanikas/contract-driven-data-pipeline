# Terraform — data lake, IAM, Glue & Athena

Infrastructure-as-code for the AWS side of **s3-spark-pg-etl**. It provisions the
resources the pipeline used to create imperatively (the S3 bucket) plus the
governance/analytics layer on top of the lake (Glue + Athena), declaratively and
with least privilege.

## What it creates

| Resource | File | Purpose |
| :-- | :-- | :-- |
| Data-lake S3 bucket | `s3.tf` | the bucket the ETL writes `raw/`, `rejects/`, `quality/` into — encrypted, private, versioned |
| Lifecycle rules | `s3.tf` | expire the `raw/` zone after N days (fixes the "raw objects accumulate" cost issue) |
| Athena results bucket | `s3.tf` | separate bucket for query output, expires quickly |
| Pipeline IAM user + key | `iam.tf` | least-privilege creds for the ETL — list bucket + read/write objects, **no** `CreateBucket` |
| Glue Catalog database | `glue.tf` | catalog the crawler populates / Athena queries |
| Glue CSV classifier | `glue.tf` | forces header detection so columns are named, not `col0…` |
| Glue Crawler | `glue.tf` | scans the three zones → `raw` / `rejects` / `quality` tables with `dt=` partitions |
| Athena workgroup | `athena.tf` | managed results location + engine version |
| Athena saved queries | `athena.tf` | ready-made: rejections by reason, accept-rate over time, raw sample |

## Layout

- `bootstrap/` — run **once, locally**: creates the remote-state bucket + lock
  table and the GitHub OIDC deployer role. See `bootstrap/README.md`.
- this dir (main config) — the data lake + Glue + Athena, using the **remote
  backend**. Applied locally (`make tf-apply`) or by the **Terraform** GitHub
  Action (`.github/workflows/terraform.yml`).

## Prerequisites

- Terraform >= 1.5 and AWS credentials with permission to create these resources
  (an admin/bootstrap profile — **not** the least-privilege pipeline user this
  creates). Set `AWS_PROFILE` / `AWS_ACCESS_KEY_ID` etc. in your shell.
- The `bootstrap/` config applied, and `backend.hcl` created from
  `backend.hcl.example` (filled with the bootstrap outputs).

## Usage

```bash
# one-time: cd bootstrap && terraform init && terraform apply   (see bootstrap/README.md)

cd infra/terraform
cp terraform.tfvars.example terraform.tfvars   # region + a globally-unique data-lake bucket name
cp backend.hcl.example backend.hcl             # bucket + lock table from the bootstrap outputs

terraform init -backend-config=backend.hcl
terraform plan
terraform apply
```

> Prefer the Makefile: `make bootstrap-apply`, then `make tf-init tf-apply`. Or
> let CI do it — open a PR for the `plan`, then run the **Terraform** workflow with
> `apply` (approval-gated). For a quick local check without remote state:
> `terraform init -backend=false`.

### Wire the outputs into the pipeline

After `apply`, feed the generated values into the project's root `.env`:

```bash
terraform output data_lake_bucket                 # -> S3_BUCKET_NAME
terraform output pipeline_access_key_id           # -> AWS_ACCESS_KEY_ID
terraform output -raw pipeline_secret_access_key  # -> AWS_SECRET_ACCESS_KEY (sensitive)
```

> Because the bucket now exists ahead of time, the pipeline never calls
> `create_bucket` (its `head_bucket` check just succeeds) — so the least-privilege
> user without `s3:CreateBucket` is enough. **Apply this Terraform before triggering
> the DAG.**

### Query the lake

1. Run a DAG pass so there are objects in `raw/`, `rejects/`, `quality/`.
2. Run the crawler:
   ```bash
   aws glue start-crawler --name "$(terraform output -raw glue_crawler_name)"
   ```
3. In the **Athena console**, pick the workgroup from
   `terraform output athena_workgroup`, select the database from
   `terraform output glue_database`, and run the saved queries (Saved queries tab):
   `rejections_by_reason`, `accept_rate_over_time`, `raw_sample`.

## Notes

- **State holds a secret.** The IAM secret access key lives in Terraform state, so
  `*.tfstate` and `terraform.tfvars` are gitignored. For a team setup, switch to the
  S3 backend stub in `versions.tf` (remote state + locking).
- **Teardown:** `terraform destroy`. The buckets use lifecycle/`force_destroy`
  where appropriate; empty the data-lake bucket first if it still holds objects.
- **Next steps (not included here):** an EventBridge rule on `dq_report.json`
  uploads → Lambda → SNS/Slack alert when the accept-rate drops; or QuickSight /
  the Streamlit app reading Athena for a lake dashboard.
```
