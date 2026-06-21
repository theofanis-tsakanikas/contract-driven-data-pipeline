# --------------------------------------------------------------------------- #
# Glue Data Catalog + Crawler — turns the raw files in S3 into queryable tables.
#
# The crawler scans the three lake zones and registers a table for each, with
# dt=<date> auto-detected as a partition. Once it has run, Athena can query the
# lake with plain SQL (see athena.tf).
# --------------------------------------------------------------------------- #

resource "aws_glue_catalog_database" "lake" {
  name        = replace("${var.project_name}_lake", "-", "_")
  description = "Catalog for the S3 data lake (raw / rejects / quality zones)."
}

# Force "first row is the header" for every CSV the crawler classifies. Without
# this, Glue's heuristic often names columns col0, col1, ... With no `header`
# list given, each file's own first row is used — so it works for both the 7-col
# raw CSV and the 8-col rejects CSV. The JSON quality reports are classified by
# Glue's built-in JSON classifier instead.
resource "aws_glue_classifier" "lake_csv" {
  name = "${var.project_name}-csv-header"
  csv_classifier {
    contains_header = "PRESENT"
    delimiter       = ","
    quote_symbol    = "\""
  }
}

# --- Crawler IAM role ---

data "aws_iam_policy_document" "glue_assume" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "glue_crawler" {
  name               = "${var.project_name}-glue-crawler"
  assume_role_policy = data.aws_iam_policy_document.glue_assume.json
}

resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue_crawler.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

# AWSGlueServiceRole does not grant bucket access; add read-only on the lake.
data "aws_iam_policy_document" "glue_s3_read" {
  statement {
    effect    = "Allow"
    actions   = ["s3:GetObject"]
    resources = ["${aws_s3_bucket.data_lake.arn}/*"]
  }
  statement {
    effect    = "Allow"
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.data_lake.arn]
  }
}

resource "aws_iam_role_policy" "glue_s3_read" {
  name   = "${var.project_name}-glue-s3-read"
  role   = aws_iam_role.glue_crawler.id
  policy = data.aws_iam_policy_document.glue_s3_read.json
}

# --- The crawler ---

resource "aws_glue_crawler" "lake" {
  name          = "${var.project_name}-lake-crawler"
  role          = aws_iam_role.glue_crawler.arn
  database_name = aws_glue_catalog_database.lake.name
  description   = "Catalogs the raw / rejects / quality zones of the data lake."
  classifiers   = [aws_glue_classifier.lake_csv.name]

  # One explicit target per zone → one table each (raw, rejects, quality),
  # with the dt= prefixes detected as partitions.
  s3_target {
    path = "s3://${aws_s3_bucket.data_lake.bucket}/raw/"
  }
  s3_target {
    path = "s3://${aws_s3_bucket.data_lake.bucket}/rejects/"
  }
  s3_target {
    path = "s3://${aws_s3_bucket.data_lake.bucket}/quality/"
  }

  # On-demand by default; set glue_crawler_schedule to enable a cron run.
  schedule = var.glue_crawler_schedule != "" ? var.glue_crawler_schedule : null

  schema_change_policy {
    update_behavior = "UPDATE_IN_DATABASE"
    delete_behavior = "LOG"
  }

  configuration = jsonencode({
    Version = 1.0
    CrawlerOutput = {
      Partitions = { AddOrUpdateBehavior = "InheritFromTable" }
    }
  })
}
