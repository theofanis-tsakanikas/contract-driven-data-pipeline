variable "aws_region" {
  description = "AWS region for all resources. Must match AWS_DEFAULT_REGION in the project's .env."
  type        = string
  default     = "eu-central-1"
}

variable "project_name" {
  description = "Name prefix applied to created resources (IAM, Glue, Athena)."
  type        = string
  default     = "s3-spark-pg-etl"
}

variable "data_lake_bucket_name" {
  description = <<-EOT
    Globally-unique name for the data-lake bucket. This is the SAME value you put
    in S3_BUCKET_NAME in the project's .env — the ETL pipeline writes raw/, rejects/
    and quality/ objects into it, and Glue/Athena read them back.
  EOT
  type        = string

  validation {
    condition     = can(regex("^[a-z0-9.-]{3,63}$", var.data_lake_bucket_name))
    error_message = "Bucket names must be 3-63 chars, lowercase letters, digits, dots and hyphens only."
  }
}

variable "raw_zone_expiration_days" {
  description = "Days after which objects under raw/ expire (the raw zone accumulates per run). Set to 0 to disable expiry."
  type        = number
  default     = 30
}

variable "athena_results_expiration_days" {
  description = "Days after which Athena query results are deleted (they are reproducible, so kept short)."
  type        = number
  default     = 14
}

variable "create_pipeline_user" {
  description = "Create a dedicated least-privilege IAM user + access key for the ETL pipeline. Disable if you supply credentials another way (e.g. OIDC role)."
  type        = bool
  default     = true
}

variable "glue_crawler_schedule" {
  description = "Cron schedule for the Glue crawler (UTC), e.g. \"cron(0 6 * * ? *)\". Empty string = on-demand only."
  type        = string
  default     = ""
}

variable "enable_quicksight" {
  description = "Create the QuickSight Athena data source + dataset (see quicksight.tf). PAID service; requires an active QuickSight subscription. Off by default."
  type        = bool
  default     = false
}

variable "quicksight_principal_arn" {
  description = "QuickSight user/group ARN that owns the data source/dataset (only used when enable_quicksight = true)."
  type        = string
  default     = ""
}
