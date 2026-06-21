output "data_lake_bucket" {
  description = "Data-lake bucket name — put this in S3_BUCKET_NAME in the project's .env."
  value       = aws_s3_bucket.data_lake.bucket
}

output "athena_results_bucket" {
  description = "Bucket holding Athena query results."
  value       = aws_s3_bucket.athena_results.bucket
}

output "glue_database" {
  description = "Glue Data Catalog database the crawler populates / Athena queries."
  value       = aws_glue_catalog_database.lake.name
}

output "glue_crawler_name" {
  description = "Run it on demand: aws glue start-crawler --name <this>."
  value       = aws_glue_crawler.lake.name
}

output "athena_workgroup" {
  description = "Select this workgroup in the Athena console to use the saved queries."
  value       = aws_athena_workgroup.lake.name
}

output "pipeline_access_key_id" {
  description = "AWS_ACCESS_KEY_ID for the pipeline IAM user (put in .env)."
  value       = try(aws_iam_access_key.pipeline[0].id, null)
}

output "pipeline_secret_access_key" {
  description = "AWS_SECRET_ACCESS_KEY for the pipeline IAM user (put in .env). Read with: terraform output -raw pipeline_secret_access_key"
  value       = try(aws_iam_access_key.pipeline[0].secret, null)
  sensitive   = true
}
