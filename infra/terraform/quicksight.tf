# --------------------------------------------------------------------------- #
# Amazon QuickSight — AWS-native BI over the data lake (OPT-IN, disabled by default).
#
# IMPORTANT — read before enabling (set enable_quicksight = true):
#   1. COST. QuickSight is a paid service (Standard/Enterprise; ~$9-18/user/month
#      or session-capacity pricing). These resources assume the QuickSight
#      *subscription* is already enabled in the account — that one-time signup is a
#      console/billing step, not Terraform, and choosing an edition starts billing.
#   2. PRINCIPAL. Set quicksight_principal_arn to your QuickSight user/group ARN
#      (e.g. arn:aws:quicksight:<region>:<acct>:user/default/<you>).
#   3. SERVICE ROLE. In the QuickSight console, grant it access to Athena + the
#      data-lake S3 bucket (creates the aws-quicksight-service-role) so the Athena
#      data source can query.
#   4. WHY ATHENA, NOT THE MARTS. QuickSight is a cloud service and cannot reach the
#      local Dockerised Postgres where dbt builds the marts. So it sits on **Athena**
#      over the lake's `quality` / `rejects` zones — i.e. a data-quality BI dashboard
#      (accept-rate over time, rejections by reason). Run the Glue crawler first so
#      those tables exist. (For mart BI, use the Streamlit app, which reads Postgres.)
#
# Authoring the visuals themselves is best done in the QuickSight console on top of
# the dataset below; the data source + dataset are the part worth codifying.
# --------------------------------------------------------------------------- #

resource "aws_quicksight_data_source" "athena" {
  count          = var.enable_quicksight ? 1 : 0
  data_source_id = "${var.project_name}-athena"
  name           = "${var.project_name} — Athena (data lake)"
  type           = "ATHENA"

  parameters {
    athena {
      work_group = aws_athena_workgroup.lake.name
    }
  }

  permission {
    principal = var.quicksight_principal_arn
    actions = [
      "quicksight:DescribeDataSource",
      "quicksight:DescribeDataSourcePermissions",
      "quicksight:PassDataSource",
      "quicksight:UpdateDataSource",
      "quicksight:DeleteDataSource",
      "quicksight:UpdateDataSourcePermissions",
    ]
  }
}

# Dataset over the per-run data-quality reports (quality/dt=.../dq_report.json),
# catalogued by the Glue crawler as the `quality` table.
resource "aws_quicksight_data_set" "quality" {
  count       = var.enable_quicksight ? 1 : 0
  data_set_id = "${var.project_name}-quality"
  name        = "${var.project_name} — data quality"
  import_mode = "DIRECT_QUERY"

  physical_table_map {
    physical_table_map_id = "quality"
    relational_table {
      data_source_arn = aws_quicksight_data_source.athena[0].arn
      catalog         = "AwsDataCatalog"
      schema          = aws_glue_catalog_database.lake.name
      name            = "quality"

      input_columns {
        name = "total"
        type = "INTEGER"
      }
      input_columns {
        name = "accepted"
        type = "INTEGER"
      }
      input_columns {
        name = "rejected"
        type = "INTEGER"
      }
      input_columns {
        name = "accept_rate"
        type = "DECIMAL"
      }
      input_columns {
        name = "dt"
        type = "STRING"
      }
    }
  }

  permissions {
    principal = var.quicksight_principal_arn
    actions = [
      "quicksight:DescribeDataSet",
      "quicksight:DescribeDataSetPermissions",
      "quicksight:PassDataSet",
      "quicksight:DescribeIngestion",
      "quicksight:ListIngestions",
      "quicksight:UpdateDataSet",
      "quicksight:DeleteDataSet",
      "quicksight:CreateIngestion",
      "quicksight:CancelIngestion",
      "quicksight:UpdateDataSetPermissions",
    ]
  }
}
