provider "aws" {
  region = var.aws_region

  # Every resource created here is tagged for cost attribution and so the whole
  # stack is easy to find / clean up in the console.
  default_tags {
    tags = {
      Project   = var.project_name
      ManagedBy = "terraform"
      Component = "data-lake"
    }
  }
}
