# ORCP top-level Makefile.
# Targets are designed so they work both on the developer workstation
# and on the deploy bastion host.  No docker is required in Stage A-G.

SHELL := /bin/bash

MVN        ?= mvn
MVN_FLAGS  ?= -T 1C -DskipTests
JAVA_HOME  ?= /opt/jdk-8

# Deploy targets source cluster topology from /etc/orcp/cluster.env on the
# bastion (or from $ORCP_CLUSTER_ENV).  NODE_1_HOST is where the admin +
# ingest services run in the reference topology (3-node minimum layout from
# DEV_SPEC §3).  Override any of these at the command line if needed.
INGEST_HOST ?= $(shell . $${ORCP_CLUSTER_ENV:-/etc/orcp/cluster.env} 2>/dev/null && echo $$NODE_3_HOST)
ADMIN_HOST  ?= $(shell . $${ORCP_CLUSTER_ENV:-/etc/orcp/cluster.env} 2>/dev/null && echo $$NODE_3_HOST)
SSH_USER    ?= orcp
SSH         ?= ssh -o StrictHostKeyChecking=accept-new

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
	@echo ""
	@echo "Deployment (requires SSH to cluster nodes):"
	@echo "  make deploy-ingest    - rsync orcp-ingest jar to \$$INGEST_HOST and systemctl restart"
	@echo "  make deploy-admin     - rsync orcp-admin  jar to \$$ADMIN_HOST  and systemctl restart"
	@echo "  make deploy-systemd   - install deploy/systemd/*.service on the target host"
	@echo "  make status           - systemctl status across all cluster nodes"
	@echo ""
	@echo "Job lifecycle (requires access to Flink REST):"
	@echo "  make gen-events ARGS='...'       - run scripts/gen_events.py"
	@echo "  make submit-flink                - run scripts/submit_job.sh"
	@echo "  make savepoint JOB=<id>          - savepoint.sh JOB"
	@echo "  make cancel    JOB=<id>          - cancel_job.sh JOB"

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
# Deployment
# ---------------------------------------------------------------------

# Copies the repackaged boot jar and reloads the systemd unit.  Assumes:
#   - target host already has /opt/orcp/{ingest,admin} owned by orcp:orcp
#   - the systemd unit was installed once via `make deploy-systemd`
#   - the orcp user can sudo systemctl (passwordless, installed by 00_bootstrap.sh)
#
# Roll back manually by keeping the previous jar around as orcp-ingest.jar.bak.

.PHONY: deploy-ingest
deploy-ingest: orcp-ingest/target/orcp-ingest.jar
	@test -n "$(INGEST_HOST)" || (echo "INGEST_HOST is unset; set it or edit /etc/orcp/cluster.env"; exit 1)
	@echo "==> deploying orcp-ingest.jar to $(SSH_USER)@$(INGEST_HOST):/opt/orcp/ingest/"
	$(SSH) $(SSH_USER)@$(INGEST_HOST) 'sudo install -d -o orcp -g orcp /opt/orcp/ingest'
	rsync -az --rsync-path='sudo rsync' orcp-ingest/target/orcp-ingest.jar \
	    $(SSH_USER)@$(INGEST_HOST):/opt/orcp/ingest/orcp-ingest.jar.new
	$(SSH) $(SSH_USER)@$(INGEST_HOST) '\
	    sudo mv -f /opt/orcp/ingest/orcp-ingest.jar /opt/orcp/ingest/orcp-ingest.jar.bak 2>/dev/null || true; \
	    sudo mv /opt/orcp/ingest/orcp-ingest.jar.new /opt/orcp/ingest/orcp-ingest.jar; \
	    sudo chown orcp:orcp /opt/orcp/ingest/orcp-ingest.jar; \
	    sudo systemctl restart orcp-ingest.service; \
	    sudo systemctl --no-pager --lines=5 status orcp-ingest.service || true'

.PHONY: deploy-admin
deploy-admin: orcp-admin/target/orcp-admin.jar
	@test -n "$(ADMIN_HOST)" || (echo "ADMIN_HOST is unset; set it or edit /etc/orcp/cluster.env"; exit 1)
	@echo "==> deploying orcp-admin.jar to $(SSH_USER)@$(ADMIN_HOST):/opt/orcp/admin/"
	$(SSH) $(SSH_USER)@$(ADMIN_HOST) 'sudo install -d -o orcp -g orcp /opt/orcp/admin'
	rsync -az --rsync-path='sudo rsync' orcp-admin/target/orcp-admin.jar \
	    $(SSH_USER)@$(ADMIN_HOST):/opt/orcp/admin/orcp-admin.jar.new
	$(SSH) $(SSH_USER)@$(ADMIN_HOST) '\
	    sudo mv -f /opt/orcp/admin/orcp-admin.jar /opt/orcp/admin/orcp-admin.jar.bak 2>/dev/null || true; \
	    sudo mv /opt/orcp/admin/orcp-admin.jar.new /opt/orcp/admin/orcp-admin.jar; \
	    sudo chown orcp:orcp /opt/orcp/admin/orcp-admin.jar; \
	    sudo systemctl restart orcp-admin.service; \
	    sudo systemctl --no-pager --lines=5 status orcp-admin.service || true'

.PHONY: deploy-systemd
deploy-systemd:
	@test -n "$(INGEST_HOST)" || (echo "INGEST_HOST is unset"; exit 1)
	@echo "==> copying systemd units to $(SSH_USER)@$(INGEST_HOST)"
	rsync -az --rsync-path='sudo rsync' \
	    deploy/systemd/orcp-ingest.service deploy/systemd/orcp-admin.service \
	    $(SSH_USER)@$(INGEST_HOST):/etc/systemd/system/
	$(SSH) $(SSH_USER)@$(INGEST_HOST) 'sudo systemctl daemon-reload'

.PHONY: gen-events
gen-events:
	@python3 -c 'import kafka' >/dev/null 2>&1 || pip install -r scripts/requirements.txt
	python3 scripts/gen_events.py $(ARGS)

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
	@if [ -z "$(INGEST_HOST)" ]; then echo "INGEST_HOST unset; skipping remote status"; exit 0; fi
	@for h in $$(. $${ORCP_CLUSTER_ENV:-/etc/orcp/cluster.env} 2>/dev/null && echo "$$NODE_1_HOST $$NODE_2_HOST $$NODE_3_HOST"); do \
	    [ -z "$$h" ] && continue; \
	    echo "===== $$h ====="; \
	    $(SSH) $(SSH_USER)@$$h 'sudo systemctl --no-pager --lines=0 status \
	        zookeeper.service kafka.service \
	        flink-jobmanager.service flink-taskmanager.service \
	        orcp-ingest.service orcp-admin.service 2>/dev/null \
	        | grep -E "● |Active:" || true'; \
	done
