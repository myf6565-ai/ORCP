# ORCP

**Online Real-time Computing Platform** — a minimum-production, landable
real-time computing system built on **Spring Cloud + Apache Kafka +
Apache Flink + OceanBase** (MySQL mode).

## Status

Stages A–G complete. The minimum-viable production slice is in place:
infra bootstrap scripts, schemas, orcp-ingest, orcp-flink-job, orcp-admin,
observability stack, and the §8.8 acceptance harness.

## Technology stack (locked for JDK 8 production constraint)

- JDK: Temurin **8u402**
- Spring Boot **2.7.18** + Spring Cloud **2021.0.9** + Spring Cloud Alibaba **2021.0.5.0**
- Apache Kafka **3.5.2** (ZooKeeper mode) with Apache ZooKeeper **3.7.2**
- Apache Flink **1.17.2** (standalone, systemd-managed)
- OceanBase **3.2.3** (MySQL mode) via `oceanbase-client` **2.4.14**
- MySQL 8.0 (local detail / dim source), MyBatis-Plus **3.5.5**
- Nacos **2.2.3** for registry + config
- Prometheus + Grafana for observability

## Repository layout

```
orcp-common/       shared DTOs, constants, utilities (library)
orcp-ingest/       Spring Boot: Kafka -> MySQL detail + forward topic
orcp-flink-job/    Flink 1.17.2 job jar (Kafka + MySQL dim + OceanBase sink)
orcp-admin/        minimal control plane (submit/cancel Flink + health)
deploy/            CentOS bootstrap scripts, systemd units, flink-conf
docs/              DEV_SPEC.md, OPS_RUNBOOK.md, SQL schemas
scripts/           job lifecycle helpers
Makefile           one-stop build and deploy entry points
```

## Quick start (developers)

```bash
# Prerequisites: JDK 8, Maven 3.8.x
java -version         # must print 1.8.x
mvn -version

# Build all modules
make build

# Run the ingest service locally
java -jar orcp-ingest/target/orcp-ingest.jar

# Produce test events (kafka-python required)
pip install -r scripts/requirements.txt
make gen-events ARGS='--count 100 --rate 50'
```

## Deployment

Full install instructions live in [`docs/OPS_RUNBOOK.md`](./docs/OPS_RUNBOOK.md).
The short version:

```bash
# On every node: bootstrap + JDK 8 + ZooKeeper + Kafka + Flink
sudo bash deploy/centos/{00_bootstrap,10_install_jdk8,15_install_zookeeper,
                        20_install_kafka_zk,30_install_flink}.sh

# On node-1 only: MySQL + Nacos
sudo bash deploy/centos/{40_install_mysql,50_install_nacos}.sh

# On node-2 only: Prometheus + Grafana
sudo bash deploy/centos/60_install_prom_grafana.sh

# From the bastion: build, deploy Spring services, submit the Flink job
make build
make deploy-systemd deploy-ingest deploy-admin
make submit-flink
```

## Acceptance test

`scripts/acceptance_test.sh` runs the DEV_SPEC §8.8 checklist end-to-end
against a live cluster and prints PASS/FAIL per gate. Results go into
[`docs/ACCEPTANCE_REPORT.md`](./docs/ACCEPTANCE_REPORT.md).

```bash
# safe (non-destructive) gates:
bash scripts/acceptance_test.sh

# full suite including TaskManager kill and JobManager outage:
TM_HOST=node-3 JM_HOST=node-1 INGEST_HOST=node-3 \
    bash scripts/acceptance_test.sh --all
```

## Documentation

| File                                 | Purpose                                          |
|--------------------------------------|--------------------------------------------------|
| `docs/DEV_SPEC.md`                   | architecture, version matrix, stage-by-stage plan |
| `docs/OPS_RUNBOOK.md`                | first install + daily ops + troubleshooting     |
| `docs/ACCEPTANCE_REPORT.md`          | §8.8 evidence template (copy per run)           |
| `docs/SQL/README.md`                 | schema load instructions                         |
| `deploy/grafana/README.md`           | Grafana dashboard import                         |

## Contributing

1. Branch off `main` using `feat/*`, `fix/*`, `docs/*`, `chore/*`.
2. Each PR should target a single stage section of `docs/DEV_SPEC.md`.
3. All Java code must be JDK 8 source/target; see DEV_SPEC appendix B
   for the forbidden API/syntax list.
