terraform {
  required_version = ">= 1.5.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  # Remote state in S3 with DynamoDB locking. Configured as a PARTIAL backend:
  # the account-specific values live in backend.hcl (gitignored) and are passed at
  # init time via `terraform init -backend-config=backend.hcl`. The backing bucket
  # and lock table are created once by ./bootstrap. For a quick local-only run
  # without remote state, `terraform init -backend=false`.
  backend "s3" {
    key     = "s3-spark-pg-etl/terraform.tfstate"
    encrypt = true
  }
}
