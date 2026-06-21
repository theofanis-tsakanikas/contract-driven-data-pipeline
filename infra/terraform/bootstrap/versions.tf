terraform {
  required_version = ">= 1.5.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  # Bootstrap uses LOCAL state on purpose: it creates the very resources the main
  # config's remote backend depends on (the state bucket + lock table), so it
  # cannot itself live in that backend. Run it once, locally, with admin creds.
}
