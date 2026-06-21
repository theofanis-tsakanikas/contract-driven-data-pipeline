# --------------------------------------------------------------------------- #
# Athena workgroup + saved queries — the SQL "window" into the data lake.
#
# The workgroup enforces a managed results location and engine version. The
# named queries are ready-to-run examples against the tables the Glue crawler
# creates (raw / rejects / quality). Run the crawler once before querying.
# --------------------------------------------------------------------------- #

resource "aws_athena_workgroup" "lake" {
  name = "${var.project_name}-wg"

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true

    result_configuration {
      output_location = "s3://${aws_s3_bucket.athena_results.bucket}/output/"
      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
  }

  # Drop the workgroup even if it still has saved queries / history.
  force_destroy = true
}

# Distribution of rejected rows by the contract rule they violated — the
# headline data-quality cut, straight from the rejects zone.
resource "aws_athena_named_query" "rejections_by_reason" {
  name        = "rejections_by_reason"
  description = "Count of quarantined rows grouped by rejection_reason."
  database    = aws_glue_catalog_database.lake.name
  workgroup   = aws_athena_workgroup.lake.name
  query       = <<-SQL
    SELECT rejection_reason,
           count(*) AS rejected_rows
    FROM rejects
    GROUP BY rejection_reason
    ORDER BY rejected_rows DESC;
  SQL
}

# Accept-rate trend over time, read from the per-run dq_report.json objects in
# the quality zone (dt is the run's logical date).
resource "aws_athena_named_query" "accept_rate_over_time" {
  name        = "accept_rate_over_time"
  description = "Per-run total / accepted / rejected and accept_rate by date."
  database    = aws_glue_catalog_database.lake.name
  workgroup   = aws_athena_workgroup.lake.name
  query       = <<-SQL
    SELECT dt,
           total,
           accepted,
           rejected,
           accept_rate
    FROM quality
    ORDER BY dt;
  SQL
}

# Sanity peek at the raw zone for a given partition.
resource "aws_athena_named_query" "raw_sample" {
  name        = "raw_sample"
  description = "First 100 raw rows for the most recent partition."
  database    = aws_glue_catalog_database.lake.name
  workgroup   = aws_athena_workgroup.lake.name
  query       = <<-SQL
    SELECT *
    FROM raw
    ORDER BY dt DESC
    LIMIT 100;
  SQL
}
