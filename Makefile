# s3-spark-pg-etl — developer ergonomics.
#
# Two planes:
#   * Infrastructure (Terraform) — `make bootstrap-*` once, then `make tf-*`.
#   * Data pipeline (Airflow)     — `make up`, then trigger with `make run`
#                                   (or press ▶ in the Airflow UI). No schedule.
#
# Run `make` with no target for this help.

COMPOSE := docker compose --env-file .env -f infra/docker-compose.yml
TF      := terraform -chdir=infra/terraform
TF_BOOT := terraform -chdir=infra/terraform/bootstrap
DAG_ID  := s3-to-postgres-etl

.DEFAULT_GOAL := help

# --- Quality -------------------------------------------------------------- #

.PHONY: lint
lint: ## Ruff lint + data-dictionary drift check
	ruff check .
	python scripts/data_contract.py --check

.PHONY: test
test: ## Run the pytest suite (PySpark transform, loader, contract)
	AWS_DEFAULT_REGION=us-east-1 pytest tests/ -q

# --- Terraform: bootstrap (run once, locally, with admin creds) ----------- #

.PHONY: bootstrap-init
bootstrap-init: ## terraform init for the bootstrap (state bucket / lock / OIDC)
	$(TF_BOOT) init

.PHONY: bootstrap-apply
bootstrap-apply: ## Create the state bucket, lock table and GitHub OIDC role
	$(TF_BOOT) apply

# --- Terraform: main infra (uses the remote backend) ---------------------- #

.PHONY: tf-fmt
tf-fmt: ## Format all Terraform files
	terraform -chdir=infra/terraform fmt -recursive

.PHONY: tf-init
tf-init: ## terraform init with the remote backend (needs infra/terraform/backend.hcl)
	$(TF) init -backend-config=backend.hcl

.PHONY: tf-validate
tf-validate: ## Validate the main config (no backend needed)
	$(TF) validate

.PHONY: tf-plan
tf-plan: ## Show the infra plan
	$(TF) plan

.PHONY: tf-apply
tf-apply: ## Apply the infra (bucket + IAM + Glue + Athena) locally
	$(TF) apply

.PHONY: tf-output
tf-output: ## Print the outputs to copy into .env
	$(TF) output

# --- Docker stack --------------------------------------------------------- #

.PHONY: up
up: ## Build + start the whole stack (Airflow, Postgres, Spark, pgAdmin, ...)
	$(COMPOSE) up --build -d

.PHONY: down
down: ## Stop the stack (keeps volumes)
	$(COMPOSE) down

.PHONY: ps
ps: ## Show service status
	$(COMPOSE) ps

.PHONY: logs
logs: ## Tail the worker logs (where the ETL + dbt run)
	$(COMPOSE) logs -f airflow-worker

# --- Pipeline + lake ------------------------------------------------------ #

.PHONY: run
run: ## Trigger one DAG run (same as pressing ▶ in the UI)
	$(COMPOSE) exec airflow-scheduler airflow dags trigger $(DAG_ID)

.PHONY: runs
runs: ## List recent DAG runs and their state
	$(COMPOSE) exec airflow-scheduler airflow dags list-runs -d $(DAG_ID)

.PHONY: crawler
crawler: ## Run the Glue crawler so Athena sees the latest lake data
	aws glue start-crawler --name "$$($(TF) output -raw glue_crawler_name)"

.PHONY: clean
clean: ## Stop the stack and remove volumes (DESTROYS local DB data)
	$(COMPOSE) down -v

.PHONY: help
help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'
