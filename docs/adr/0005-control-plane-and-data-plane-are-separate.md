# ADR-0005: The control plane and the data plane are separate

- **Status:** Accepted
- **Date:** 2026-06 (recorded 2026-08)

## Context

Both provisioning and running the pipeline can be automated with GitHub Actions, and it is tempting to
do so: one place to press buttons, one set of credentials, one log. Many portfolio projects run their
ETL from CI for exactly that reason — it makes a green check mark appear.

It also conflates two things with nothing in common. Provisioning changes rarely, needs broad
permissions over the account, and must be gated by a human. Running the ETL happens routinely, needs
only object read/write on one bucket, and should be re-runnable without an approval. Giving one
identity both jobs means the routine one carries the dangerous one's permissions.

## Decision

Two planes, two tools, two identities, and no overlap.

**Control plane — Terraform, driven by GitHub Actions.** Every PR touching `infra/terraform/**` gets
a read-only `plan`. The `apply` is a manual *Run workflow* dispatch gated by the `production`
environment's approval, authenticating over OIDC as the deployer role. No long-lived AWS key exists
in GitHub.

**Data plane — Airflow.** The DAG is `schedule=None`, triggered by ▶ in the UI or `make run`. It
authenticates as the least-privilege pipeline user ([ADR-0004](0004-terraform-provisions-the-bucket.md)).
**It is never triggered from CI.**

The OIDC trust policy makes the split structural rather than conventional: it names three subjects
(`pull_request`, `ref:refs/heads/main`, `environment:production`) with `StringEquals`, so nothing else
in the repository can assume the deployer role even by accident.

## Alternatives rejected

- **Run the ETL from GitHub Actions.** Would put pipeline execution behind CI credentials and CI
  scheduling, and would make Airflow — the orchestrator this project exists to demonstrate — a
  decoration. It also gives a routine, frequently-triggered workflow a path to the deployer role.
- **`terraform apply` on merge to main.** Removes the human gate from the only operation that can
  destroy state. The plan-on-PR / apply-on-dispatch split keeps the review without the automation.
- **A cron schedule on the DAG.** Rejected because this pipeline demonstrates a contract, not a
  service. A schedule would accumulate cost and lake objects for nothing.

## Consequences

- The blast radius of each credential matches its job. A compromised pipeline key cannot provision;
  a compromised workflow cannot run without passing the environment gate.
- The Terraform state is protected by an approval, not by convention.
- A manual dispatch from a feature branch is refused by AWS. Dispatch from `main`, or open a PR —
  documented in [SECURITY.md](../../SECURITY.md).
- There is no automated end-to-end "the ETL ran today" signal. The CI `smoke` job covers the loader
  against a real PostgreSQL instead, which is the part worth guarding automatically.
