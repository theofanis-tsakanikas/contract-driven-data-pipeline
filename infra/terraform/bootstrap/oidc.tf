# --------------------------------------------------------------------------- #
# GitHub Actions → AWS via OIDC (no long-lived keys in GitHub).
#
# GitHub presents a signed OIDC token; AWS trusts this provider and lets the
# workflow assume the deployer role, scoped to THIS repository. The role carries
# the permissions Terraform needs to manage the main config (S3 / Glue / Athena /
# the pipeline IAM user + crawler role).
# --------------------------------------------------------------------------- #

data "aws_caller_identity" "current" {}

# The GitHub OIDC provider is an **account-level singleton**: one per AWS account,
# shared by every repository that federates into it. AWS refuses a second one with
# `EntityAlreadyExists`.
#
# So this bootstrap can either OWN it (a virgin account — `create_oidc_provider =
# true`, the default) or REFERENCE one another project already created
# (`create_oidc_provider = false`). Owning a provider you did not create is the
# dangerous case: `terraform destroy` here would delete it and silently break the
# federation of every other repository in the account.
resource "aws_iam_openid_connect_provider" "github" {
  count = var.create_oidc_provider ? 1 : 0

  url            = "https://token.actions.githubusercontent.com"
  client_id_list = ["sts.amazonaws.com"]
  # GitHub's OIDC thumbprint. AWS no longer verifies it for this provider, but the
  # argument is still accepted; kept for completeness.
  thumbprint_list = ["6938fd4d98bab03faadb97b34396831e3780aea1"]
}

data "aws_iam_openid_connect_provider" "github" {
  count = var.create_oidc_provider ? 0 : 1

  url = "https://token.actions.githubusercontent.com"
}

locals {
  # Exactly one of the two above exists; this resolves whichever it is.
  github_oidc_provider_arn = var.create_oidc_provider ? one(aws_iam_openid_connect_provider.github[*].arn) : one(data.aws_iam_openid_connect_provider.github[*].arn)
}

# --- Trust policy: only this repo's workflows may assume the role ---

data "aws_iam_policy_document" "deployer_assume" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRoleWithWebIdentity"]

    principals {
      type        = "Federated"
      identifiers = [local.github_oidc_provider_arn]
    }

    condition {
      test     = "StringEquals"
      variable = "token.actions.githubusercontent.com:aud"
      values   = ["sts.amazonaws.com"]
    }

    # Scoped to the three subjects this repository's Terraform workflow actually
    # presents — NOT "repo:<repo>:*", which would let a workflow on any branch or
    # any fork-less PR head assume the deployer role:
    #
    #   pull_request               → the read-only `plan` job on a PR
    #   ref:refs/heads/<default>   → a manual "plan" dispatch, from the default branch only
    #   environment:production     → the `apply` job, behind the environment's approval gate
    #
    # Consequence, by design: a manual dispatch from a feature branch is refused.
    # Dispatch from ${var.github_default_branch}, or open a PR.
    condition {
      test     = "StringEquals"
      variable = "token.actions.githubusercontent.com:sub"
      values = [
        "repo:${var.github_repository}:pull_request",
        "repo:${var.github_repository}:ref:refs/heads/${var.github_default_branch}",
        "repo:${var.github_repository}:environment:production",
      ]
    }
  }
}

resource "aws_iam_role" "deployer" {
  name               = "${var.project_name}-gha-deployer"
  description        = "Assumed by GitHub Actions (OIDC) to run Terraform for ${var.github_repository}."
  assume_role_policy = data.aws_iam_policy_document.deployer_assume.json
}

# --- Permissions the deployer needs to run the main config ---

# Remote-state access (read/write state object + acquire the DynamoDB lock).
data "aws_iam_policy_document" "deployer_state" {
  statement {
    sid       = "StateBucket"
    effect    = "Allow"
    actions   = ["s3:ListBucket", "s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
    resources = [aws_s3_bucket.tf_state.arn, "${aws_s3_bucket.tf_state.arn}/*"]
  }
  statement {
    sid       = "StateLock"
    effect    = "Allow"
    actions   = ["dynamodb:GetItem", "dynamodb:PutItem", "dynamodb:DeleteItem"]
    resources = [aws_dynamodb_table.tf_lock.arn]
  }
}

resource "aws_iam_role_policy" "deployer_state" {
  name   = "tf-remote-state"
  role   = aws_iam_role.deployer.id
  policy = data.aws_iam_policy_document.deployer_state.json
}

# Resource provisioning. PowerUserAccess covers S3 / Glue / Athena / DynamoDB but
# NOT IAM; the inline policy adds the IAM actions the main config performs (create
# the pipeline user + the Glue crawler role). Broad on purpose for a deployer, and
# still far better than static admin keys living in GitHub.
resource "aws_iam_role_policy_attachment" "deployer_poweruser" {
  role       = aws_iam_role.deployer.name
  policy_arn = "arn:aws:iam::aws:policy/PowerUserAccess"
}

data "aws_iam_policy_document" "deployer_iam" {
  statement {
    sid    = "ManageProjectIam"
    effect = "Allow"
    actions = [
      "iam:CreateUser", "iam:DeleteUser", "iam:GetUser", "iam:TagUser", "iam:ListUserTags",
      "iam:CreateAccessKey", "iam:DeleteAccessKey", "iam:ListAccessKeys",
      "iam:PutUserPolicy", "iam:GetUserPolicy", "iam:DeleteUserPolicy", "iam:ListUserPolicies",
      "iam:CreateRole", "iam:DeleteRole", "iam:GetRole", "iam:TagRole", "iam:PassRole",
      "iam:AttachRolePolicy", "iam:DetachRolePolicy", "iam:ListAttachedRolePolicies",
      "iam:PutRolePolicy", "iam:GetRolePolicy", "iam:DeleteRolePolicy", "iam:ListRolePolicies",
      "iam:ListInstanceProfilesForRole",
    ]
    # Scope to the resources the main config names (project-prefixed).
    resources = [
      "arn:aws:iam::${data.aws_caller_identity.current.account_id}:user/${var.project_name}-*",
      "arn:aws:iam::${data.aws_caller_identity.current.account_id}:role/${var.project_name}-*",
    ]
  }
}

resource "aws_iam_role_policy" "deployer_iam" {
  name   = "manage-project-iam"
  role   = aws_iam_role.deployer.id
  policy = data.aws_iam_policy_document.deployer_iam.json
}
