# Terraform bootstrap (run once, locally)

Creates the resources the main config and CI depend on but cannot create
themselves:

- **S3 state bucket** + **DynamoDB lock table** → the remote backend for the main
  config.
- **GitHub OIDC provider** + **deployer IAM role** → lets GitHub Actions run
  Terraform without any stored AWS keys.

This config uses **local state** (it's the chicken that lays the remote-state egg)
and is applied **once**, from your machine, with admin credentials.

## Run

```bash
cd infra/terraform/bootstrap
cp terraform.tfvars.example terraform.tfvars   # set region, github_repository, state_bucket_name

terraform init
terraform apply
```

## Then wire up the main config + GitHub

From the outputs:

```bash
terraform output            # state_bucket, lock_table, deployer_role_arn
```

1. **Main config backend** — create `../backend.hcl` from `../backend.hcl.example`,
   filling `bucket` = `state_bucket` and `dynamodb_table` = `lock_table`.
2. **GitHub repo → Settings → Secrets and variables → Actions → Variables**, add:
   - `AWS_REGION` = your region
   - `AWS_DEPLOY_ROLE_ARN` = `deployer_role_arn`
   - `TF_STATE_BUCKET` = `state_bucket`
   - `TF_LOCK_TABLE` = `lock_table`
   - `DATA_LAKE_BUCKET_NAME` = your data-lake bucket name
   With OIDC there are **no secrets to set** — only Variables.

After that, the `Terraform` GitHub Action can `plan` on PRs and `apply` from the
manual **Run workflow** button.

## Notes

- The state bucket has `prevent_destroy` — `terraform destroy` here will refuse to
  delete it. Remove that lifecycle block deliberately if you really mean to.
- Keep the local `bootstrap/terraform.tfstate` safe (it's gitignored). It only
  tracks these few bootstrap resources.
```
