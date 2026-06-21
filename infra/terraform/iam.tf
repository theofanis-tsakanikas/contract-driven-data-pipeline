# --------------------------------------------------------------------------- #
# Least-privilege IAM user for the ETL pipeline.
#
# Replaces the broad ad-hoc credentials the pipeline used to need (including
# s3:CreateBucket). It can only list the lake bucket and read/write objects in
# it — no bucket creation, no access to anything else in the account. Drop the
# AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY outputs into the project's .env.
# --------------------------------------------------------------------------- #

resource "aws_iam_user" "pipeline" {
  count = var.create_pipeline_user ? 1 : 0
  name  = "${var.project_name}-pipeline"
}

resource "aws_iam_access_key" "pipeline" {
  count = var.create_pipeline_user ? 1 : 0
  user  = aws_iam_user.pipeline[0].name
}

data "aws_iam_policy_document" "pipeline_s3" {
  # List only this bucket.
  statement {
    sid       = "ListLakeBucket"
    effect    = "Allow"
    actions   = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = [aws_s3_bucket.data_lake.arn]
  }

  # Read and write objects within it (raw/, rejects/, quality/). No DeleteObject:
  # retention/cleanup is handled by the lifecycle rules, not the pipeline.
  statement {
    sid       = "ReadWriteLakeObjects"
    effect    = "Allow"
    actions   = ["s3:GetObject", "s3:PutObject"]
    resources = ["${aws_s3_bucket.data_lake.arn}/*"]
  }
}

resource "aws_iam_user_policy" "pipeline_s3" {
  count  = var.create_pipeline_user ? 1 : 0
  name   = "${var.project_name}-pipeline-s3"
  user   = aws_iam_user.pipeline[0].name
  policy = data.aws_iam_policy_document.pipeline_s3.json
}
