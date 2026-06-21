# --------------------------------------------------------------------------- #
# Remote-state backing store for the MAIN terraform config:
#   * an S3 bucket holding terraform.tfstate (versioned + encrypted), and
#   * a DynamoDB table for state locking (prevents concurrent applies).
# Created once here, then referenced by the main config's backend "s3" {} block
# (see ../versions.tf and ../backend.hcl.example).
# --------------------------------------------------------------------------- #

resource "aws_s3_bucket" "tf_state" {
  bucket = var.state_bucket_name

  # State is precious: never let `terraform destroy` of the bootstrap nuke it
  # by accident.
  lifecycle {
    prevent_destroy = true
  }
}

resource "aws_s3_bucket_versioning" "tf_state" {
  bucket = aws_s3_bucket.tf_state.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "tf_state" {
  bucket = aws_s3_bucket.tf_state.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "tf_state" {
  bucket                  = aws_s3_bucket.tf_state.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_dynamodb_table" "tf_lock" {
  name         = var.lock_table_name
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "LockID"

  attribute {
    name = "LockID"
    type = "S"
  }
}
