# ORCP top-level Makefile.
# Targets are designed so they work both on the developer workstation
# and on the deploy bastion host. No docker is required in Stage A-G.

SHELL := /bin/bash

MVN        ?= mvn
MVN_FLAGS  ?= -T 1C -DskipTests
JAVA_HOME  ?= /opt/jdk-8

# ---------------------------------------------------------------------
# Build
# ---------------------------------------------------------------------

.PHONY: help
help:
	@echo "ORCP build targets:"
	@echo "  make build            - mvn clean package (skip tests)"
	@echo "  make verify           - mvn verify (runs tests)"
	@echo "  make clean            - mvn clean"
	@echo "  make tree             - mvn dependency:tree"
	@echo "  make deploy-ingest    - scp orcp-ingest jar and restart systemd (Stage D+)"
	@echo "  make deploy-admin     - scp orcp-admin  jar and restart systemd (Stage F+)"
	@echo "  make submit-flink     - run scripts/submit_job.sh           (Stage E+)"
	@echo "  make savepoint JOB=X  - run scripts/savepoint.sh JOB=X      (Stage E+)"
	@echo "  make cancel   JOB=X   - run scripts/cancel_job.sh JOB=X     (Stage E+)"
	@echo "  make status           - print systemd status on all nodes   (Stage F+)"

.PHONY: build
build:
	$(MVN) $(MVN_FLAGS) clean package

.PHONY: verify
verify:
	$(MVN) -T 1C clean verify

.PHONY: clean
clean:
	$(MVN) clean

.PHONY: tree
tree:
	$(MVN) dependency:tree

# ---------------------------------------------------------------------
# Deployment shims (real implementations land in later stages)
# ---------------------------------------------------------------------

.PHONY: deploy-ingest
deploy-ingest:
	@echo "[TODO Stage D] scp orcp-ingest/target/orcp-ingest.jar + systemctl restart orcp-ingest"

.PHONY: deploy-admin
deploy-admin:
	@echo "[TODO Stage F] scp orcp-admin/target/orcp-admin.jar + systemctl restart orcp-admin"

.PHONY: submit-flink
submit-flink:
	bash scripts/submit_job.sh

.PHONY: savepoint
savepoint:
	bash scripts/savepoint.sh $(JOB)

.PHONY: cancel
cancel:
	bash scripts/cancel_job.sh $(JOB)

.PHONY: status
status:
	@echo "[TODO Stage F] ssh each node and print: systemctl status zookeeper kafka flink-jobmanager flink-taskmanager orcp-ingest orcp-admin"
