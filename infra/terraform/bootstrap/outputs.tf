output "state_bucket" {
  description = "Remote-state bucket → backend.hcl `bucket` and GitHub Variable TF_STATE_BUCKET."
  value       = aws_s3_bucket.tf_state.bucket
}

output "lock_table" {
  description = "State-lock table → backend.hcl `dynamodb_table` and GitHub Variable TF_LOCK_TABLE."
  value       = aws_dynamodb_table.tf_lock.name
}

output "deployer_role_arn" {
  description = "Role GitHub Actions assumes via OIDC → GitHub Variable AWS_DEPLOY_ROLE_ARN."
  value       = aws_iam_role.deployer.arn
}

output "oidc_provider_arn" {
  description = "The GitHub OIDC provider ARN (for reference)."
  value       = aws_iam_openid_connect_provider.github.arn
}
