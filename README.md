# ORCP

**Online Real-time Computing Platform** — a minimum-production, landable
real-time computing system built on **Spring Cloud + Apache Kafka +
Apache Flink + OceanBase** (MySQL mode).

## Status

Stage A (repository skeleton) — Maven multi-module layout, JDK 8 version
matrix and placeholder entry points. Business logic is added in the
later stages described in the spec.

## Technology stack (locked for JDK 8 production constraint)

- JDK: Temurin **8u402**
- Spring Boot **2.7.18** + Spring Cloud **2021.0.9** + Spring Cloud Alibaba **2021.0.5.0**
- Apache Kafka **3.5.2** (ZooKeeper mode) with Apache ZooKeeper **3.7.2**
- Apache Flink **1.17.2** (standalone, systemd-managed)
- OceanBase **3.2.3** (MySQL mode) via `oceanbase-client` **2.4.7**
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

# Run the ingest service locally (after Stage D lands business logic)
java -jar orcp-ingest/target/orcp-ingest.jar
```

## Deployment

See `docs/DEV_SPEC.md` section 5 for the CentOS environment bootstrap
plan and section 10 for the staged delivery roadmap.

## Contributing

1. Branch off `main` using `feat/*`, `fix/*`, `docs/*`, `chore/*`.
2. Each PR should target a single stage section of `docs/DEV_SPEC.md`.
3. All Java code must be JDK 8 source/target; see DEV_SPEC appendix B
   for the forbidden API/syntax list.
