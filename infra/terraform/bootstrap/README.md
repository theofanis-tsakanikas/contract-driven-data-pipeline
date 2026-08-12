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

- **Check for an existing GitHub OIDC provider before the first apply.** It is an
  account-level singleton — one per AWS account, shared by every repository that
  federates into it, and AWS refuses a second with `EntityAlreadyExists`:

  ```bash
  aws iam list-open-id-connect-providers
  ```

  If `token.actions.githubusercontent.com` is already there, set
  `create_oidc_provider = false` in `terraform.tfvars`. This bootstrap then
  *references* it rather than owning it — which matters most at teardown: owning a
  provider you did not create means `terraform destroy` here deletes it and silently
  breaks the OIDC federation of every other repository in the account. The
  `oidc_provider_owned_here` output records which side of that line this state is on.
- The deployer role's trust policy names three subjects exactly (`pull_request`,
  `ref:refs/heads/<github_default_branch>`, `environment:production`). A workflow
  dispatched from a feature branch is refused by AWS — dispatch from the default
  branch, or open a pull request.
- The state bucket has `prevent_destroy` — `terraform destroy` here will refuse to
  delete it. Remove that lifecycle block deliberately if you really mean to.
- Keep the local `bootstrap/terraform.tfstate` safe (it's gitignored). It only
  tracks these few bootstrap resources.
```
