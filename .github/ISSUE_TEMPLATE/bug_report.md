---
name: Bug report
about: Report a problem with the ETL pipeline
title: "[Bug] "
labels: bug
assignees: ''
---

## Description

A clear and concise description of what the bug is.

## Affected stage

Which part of the pipeline is affected?

- [ ] Ingestion (`run_ingestion` / `generate_dirty_data_S3.py`)
- [ ] Transform (`spark-clean-task` / `clean_dirty_data_S3.py`)
- [ ] Load (`run_loading` / `load_to_db_final.py`)
- [ ] dbt (`run_dbt` / models)
- [ ] Infra (Docker Compose / Airflow / Spark)
- [ ] CI / tests
- [ ] Other

## Steps to reproduce

1. ...
2. ...
3. ...

## Expected behaviour

What you expected to happen.

## Actual behaviour

What actually happened. Include the failing task and any error message.

## Logs

```
Paste the relevant Airflow task log / container log here.
```

## Environment

- OS:
- Docker / Docker Compose version:
- Branch / commit:
- Relevant `.env` values (redact secrets):

## Additional context

Anything else that helps explain the problem.
