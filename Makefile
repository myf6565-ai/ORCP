# ORCP 顶层 Makefile。
# 所有目标既可在开发者工作站上运行，也可在部署堡垒机上运行。
# 阶段 A-G 无需 Docker。

SHELL := /bin/bash

MVN        ?= mvn
MVN_FLAGS  ?= -T 1C -DskipTests
JAVA_HOME  ?= /opt/jdk-8

# 部署目标从堡垒机上的 /etc/orcp/cluster.env（或 $ORCP_CLUSTER_ENV）读取集群拓扑。
# 在参考部署（DEV_SPEC §3 三节点最小布局）中，admin 和 ingest 服务运行于 NODE_3。
# 需要时可在命令行覆盖任意变量。
INGEST_HOST ?= $(shell . $${ORCP_CLUSTER_ENV:-/etc/orcp/cluster.env} 2>/dev/null && echo $$NODE_3_HOST)
ADMIN_HOST  ?= $(shell . $${ORCP_CLUSTER_ENV:-/etc/orcp/cluster.env} 2>/dev/null && echo $$NODE_3_HOST)
SSH_USER    ?= orcp
SSH         ?= ssh -o StrictHostKeyChecking=accept-new

# ---------------------------------------------------------------------
# 构建
# ---------------------------------------------------------------------

.PHONY: help
help:
	@echo "ORCP 构建目标："
	@echo "  make build            - mvn clean package（跳过测试）"
	@echo "  make verify           - mvn verify（运行测试）"
	@echo "  make clean            - mvn clean"
	@echo "  make tree             - mvn dependency:tree"
	@echo ""
	@echo "部署（需要 SSH 到集群节点）："
	@echo "  make deploy-ingest    - rsync orcp-ingest jar 到 \$$INGEST_HOST 并重启"
	@echo "  make deploy-admin     - rsync orcp-admin  jar 到 \$$ADMIN_HOST  并重启"
	@echo "  make deploy-systemd   - 在目标主机安装 deploy/systemd/*.service"
	@echo "  make status           - 显示所有集群节点的 systemctl status"
	@echo ""
	@echo "作业生命周期（需要访问 Flink REST）："
	@echo "  make gen-events ARGS='...'       - 运行 scripts/gen_events.py"
	@echo "  make submit-flink                - 运行 scripts/submit_job.sh"
	@echo "  make savepoint JOB=<id>          - 触发 savepoint"
	@echo "  make cancel    JOB=<id>          - 带 savepoint 优雅取消"
	@echo ""
	@echo "验收测试："
	@echo "  make accept ARGS='--all'         - 运行 scripts/acceptance_test.sh"

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
# 部署
# ---------------------------------------------------------------------

# 将重新打包后的 boot jar 复制到目标节点并重新加载 systemd 单元。前提条件：
#   - 目标主机已有 /opt/orcp/{ingest,admin}，属主为 orcp:orcp
#   - 曾通过 `make deploy-systemd` 安装过 systemd 单元
#   - orcp 用户已配置免密 sudo（由 00_bootstrap.sh 安装）
#
# 手动回滚：保留上一个 jar 的 .bak 文件，将其改回原名即可。

.PHONY: deploy-ingest
deploy-ingest: orcp-ingest/target/orcp-ingest.jar
	@test -n "$(INGEST_HOST)" || (echo "INGEST_HOST 未设置；请设置该变量或编辑 /etc/orcp/cluster.env"; exit 1)
	@echo "==> 部署 orcp-ingest.jar 到 $(SSH_USER)@$(INGEST_HOST):/opt/orcp/ingest/"
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
	@test -n "$(ADMIN_HOST)" || (echo "ADMIN_HOST 未设置；请设置该变量或编辑 /etc/orcp/cluster.env"; exit 1)
	@echo "==> 部署 orcp-admin.jar 到 $(SSH_USER)@$(ADMIN_HOST):/opt/orcp/admin/"
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
	@test -n "$(INGEST_HOST)" || (echo "INGEST_HOST 未设置"; exit 1)
	@echo "==> 将 systemd 单元文件复制到 $(SSH_USER)@$(INGEST_HOST)"
	rsync -az --rsync-path='sudo rsync' \
	    deploy/systemd/orcp-ingest.service deploy/systemd/orcp-admin.service \
	    $(SSH_USER)@$(INGEST_HOST):/etc/systemd/system/
	$(SSH) $(SSH_USER)@$(INGEST_HOST) 'sudo systemctl daemon-reload'

.PHONY: gen-events
gen-events:
	@python3 -c 'import kafka' >/dev/null 2>&1 || pip install -r scripts/requirements.txt
	python3 scripts/gen_events.py $(ARGS)

.PHONY: accept
accept:
	bash scripts/acceptance_test.sh $(ARGS)

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
	@if [ -z "$(INGEST_HOST)" ]; then echo "INGEST_HOST 未设置，跳过远程状态查询"; exit 0; fi
	@for h in $$(. $${ORCP_CLUSTER_ENV:-/etc/orcp/cluster.env} 2>/dev/null && echo "$$NODE_1_HOST $$NODE_2_HOST $$NODE_3_HOST"); do \
	    [ -z "$$h" ] && continue; \
	    echo "===== $$h ====="; \
	    $(SSH) $(SSH_USER)@$$h 'sudo systemctl --no-pager --lines=0 status \
	        zookeeper.service kafka.service \
	        flink-jobmanager.service flink-taskmanager.service \
	        orcp-ingest.service orcp-admin.service 2>/dev/null \
	        | grep -E "● |Active:" || true'; \
	done
