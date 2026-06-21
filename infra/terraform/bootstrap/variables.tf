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
