variable "aws_region" {
  description = "AWS region for the bootstrap resources (use the same region as the main config)."
  type        = string
  default     = "eu-central-1"
}

variable "project_name" {
  description = "Name prefix for bootstrap resources."
  type        = string
  default     = "s3-spark-pg-etl"
}

variable "github_repository" {
  description = "GitHub repo allowed to assume the deployer role, as \"owner/repo\" (e.g. theofanis-tsakanikas/s3-spark-pg-etl)."
  type        = string
}

variable "create_oidc_provider" {
  description = "Create the GitHub OIDC provider (true) or reference one another project already created (false). It is an account-level singleton: AWS allows exactly one per account, so set this to false if `aws iam list-open-id-connect-providers` already lists token.actions.githubusercontent.com."
  type        = bool
  default     = true
}

variable "github_default_branch" {
  description = "Branch a manual \"plan\" dispatch is allowed to run from (the apply dispatch is scoped to the production environment instead)."
  type        = string
  default     = "main"
}

variable "state_bucket_name" {
  description = "Globally-unique name for the Terraform remote-state bucket."
  type        = string

  validation {
    condition     = can(regex("^[a-z0-9.-]{3,63}$", var.state_bucket_name))
    error_message = "Bucket names must be 3-63 chars, lowercase letters, digits, dots and hyphens only."
  }
}

variable "lock_table_name" {
  description = "DynamoDB table name used for Terraform state locking."
  type        = string
  default     = "s3-spark-pg-etl-tf-lock"
}
