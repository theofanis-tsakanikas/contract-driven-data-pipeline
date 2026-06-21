# --------------------------------------------------------------------------- #
# Data-lake bucket — the single bucket the ETL pipeline reads from / writes to.
# Provisioning it here (instead of create_bucket() inside the ingestion task)
# means the pipeline no longer needs s3:CreateBucket, and the bucket gets proper
# encryption, public-access blocking and lifecycle rules declaratively.
# --------------------------------------------------------------------------- #

resource "aws_s3_bucket" "data_lake" {
  bucket = var.data_lake_bucket_name
}

resource "aws_s3_bucket_public_access_block" "data_lake" {
  bucket                  = aws_s3_bucket.data_lake.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_versioning" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id

  # Expire the accumulating raw zone (solves the "raw objects are retained
  # forever" cost gotcha). Disabled when raw_zone_expiration_days = 0.
  dynamic "rule" {
    for_each = var.raw_zone_expiration_days > 0 ? [1] : []
    content {
      id     = "expire-raw-zone"
      status = "Enabled"
      filter {
        prefix = "raw/"
      }
      expiration {
        days = var.raw_zone_expiration_days
      }
      noncurrent_version_expiration {
        noncurrent_days = 7
      }
    }
  }

  # Always clean up failed multipart uploads.
  rule {
    id     = "abort-incomplete-multipart"
    status = "Enabled"
    filter {}
    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }
}

# --------------------------------------------------------------------------- #
# Athena query-results bucket — kept separate from the lake so query output never
# pollutes the data zones, and expires quickly (results are reproducible).
# --------------------------------------------------------------------------- #

resource "aws_s3_bucket" "athena_results" {
  bucket = "${var.data_lake_bucket_name}-athena-results"
}

resource "aws_s3_bucket_public_access_block" "athena_results" {
  bucket                  = aws_s3_bucket.athena_results.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "athena_results" {
  bucket = aws_s3_bucket.athena_results.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "athena_results" {
  bucket = aws_s3_bucket.athena_results.id
  rule {
    id     = "expire-query-results"
    status = "Enabled"
    filter {}
    expiration {
      days = var.athena_results_expiration_days
    }
  }
}
