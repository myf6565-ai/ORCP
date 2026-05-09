# ORCP 运维手册

> 本文档是 [`DEV_SPEC.md`](./DEV_SPEC.md) 的配套运维指南。DEV_SPEC 说明"构建了什么"，
> 本文档说明"如何运行"：首次安装、日常运维，以及遇到问题时该怎么做。

所有命令均假设您以 `orcp` 用户身份（由 `deploy/centos/00_bootstrap.sh` 创建）
在对应节点上执行。该用户已配置免密 `sudo`。

---

## 0. 参考拓扑

DEV_SPEC §3 中的默认三节点布局：

| 节点   | 服务                                                              |
|--------|-------------------------------------------------------------------|
| node-1 | ZooKeeper、Kafka broker、Flink JobManager、Nacos、MySQL（明细库） |
| node-2 | ZooKeeper、Kafka broker、Flink TaskManager、Prometheus、Grafana   |
| node-3 | ZooKeeper、Kafka broker、Flink TaskManager、orcp-ingest、orcp-admin |
| OB     | OceanBase 3.2.3（外部托管）                                       |

每个节点的 `cluster.env` 必须设置 `NODE_ID`，否则 `deploy/centos/` 中的安装脚本将拒绝运行。

---

## 1. 首次安装（冷启动）

按顺序在对应节点执行每个步骤。如某步骤失败，请先排查再继续——
这些脚本设计为可重复运行，但顺序错乱会导致状态不一致。

### 1.1 初始化每个节点

```bash
# 在每个节点（node-1、node-2、node-3）上执行：
sudo install -d /etc/orcp
sudo cp deploy/centos/cluster.env.example /etc/orcp/cluster.env
sudo vi /etc/orcp/cluster.env          # 为每台主机设置 NODE_ID
sudo bash deploy/centos/00_bootstrap.sh
sudo bash deploy/centos/10_install_jdk8.sh
```

`00_bootstrap.sh` 创建 `orcp` 用户，提升 `nofile`/`nproc` 限制，
关闭 THP/swap，安装 chrony，并为三个节点写入 `/etc/hosts` 条目。
`10_install_jdk8.sh` 将 Temurin 8u402-b06 安装到 `/opt/jdk-8`。

### 1.2 ZooKeeper + Kafka（每个节点）

```bash
sudo bash deploy/centos/15_install_zookeeper.sh
sudo bash deploy/centos/20_install_kafka_zk.sh
```

`20_install_kafka_zk.sh` 会在 node-1 上自动创建 `orcp.src.demo` 和
`orcp.mid.events`。在任意节点验证：

```bash
kafka-topics.sh --bootstrap-server node-1:9092 --list
# 期望看到：orcp.src.demo、orcp.mid.events、__consumer_offsets、__transaction_state
```

### 1.3 Flink（每个节点）

```bash
sudo bash deploy/centos/30_install_flink.sh
```

角色由 `NODE_ID` 决定：1 运行 JobManager，2+ 运行 TaskManager。
脚本将 `deploy/flink-conf/{flink-conf.yaml,log4j.properties,metrics.yaml}`
复制到 `/opt/flink/conf/`，并将 4 个三方 JAR 放入 `/opt/flink/lib/`。

```bash
curl -s http://node-1:8081/overview | python3 -m json.tool | head
# 期望看到："flink-version": "1.17.2", "taskmanagers": >= 1
```

### 1.4 MySQL（仅 node-1）和 OceanBase（外部）

```bash
# node-1：
sudo bash deploy/centos/40_install_mysql.sh
mysql -uroot -p < docs/SQL/mysql_detail_schema.sql
# 立即修改占位密码：
mysql -uroot -p -e "ALTER USER 'orcp_rw'@'%' IDENTIFIED BY '<真实密码>';
                    ALTER USER 'orcp_ro'@'%' IDENTIFIED BY '<真实密码>'; FLUSH PRIVILEGES"
```

OceanBase 在任意装有 `obclient` 的机器上执行：

```bash
obclient -h${OB_HOST} -P2881 -u"orcp_rw@tenant#cluster" -p < docs/SQL/oceanbase_dw_schema.sql
obclient -h${OB_HOST} -P2881 -u"orcp_rw@tenant#cluster" -p -e "SHOW TABLES" orcp_dw
# 期望看到：agg_order_1min、agg_order_daily
```

### 1.5 Nacos（仅 node-1）

```bash
sudo bash deploy/centos/50_install_nacos.sh
curl -fsS http://node-1:8848/nacos/v1/console/health/readiness   # 期望返回 200 OK
```

访问 `http://node-1:8848/nacos`（默认 nacos/nacos）登录后：

1. 创建命名空间 `dev`（或 `prod`）。
2. 上传配置 `orcp-ingest.yaml`，填入生产 MySQL + Kafka 端点及真实 `datasource.password`。
3. 上传配置 `orcp-admin.yaml`，填入真实 `orcp.admin.password`、OB 凭据及后续配置的 webhook URL。

### 1.6 Prometheus + Grafana（仅 node-2）

```bash
sudo bash deploy/centos/60_install_prom_grafana.sh
curl -fsS http://node-2:9090/-/ready      # Prometheus
curl -fsS http://node-2:3000/api/health   # Grafana
```

安装告警规则和看板：

```bash
# 在 node-2 上：
sudo cp deploy/prometheus/orcp-alerts.yml /opt/prometheus/orcp-alerts.yml
sudo bash -c "grep -q orcp-alerts.yml /opt/prometheus/prometheus.yml || \
    sed -i '/^scrape_configs:/i\\rule_files:\\n  - /opt/prometheus/orcp-alerts.yml\\n' \
    /opt/prometheus/prometheus.yml"
sudo systemctl reload prometheus

# Grafana：Dashboards -> Import -> 粘贴 deploy/grafana/orcp-overview-dashboard.json
```

### 1.7 构建并部署 Spring 服务

在开发者工作站或堡垒机上：

```bash
make build
INGEST_HOST=node-3 ADMIN_HOST=node-3 make deploy-systemd
INGEST_HOST=node-3 make deploy-ingest
ADMIN_HOST=node-3 make deploy-admin
```

`deploy-systemd` 将两个 `.service` 单元文件复制到 `/etc/systemd/system/` 并重新加载。
`deploy-ingest` / `deploy-admin` 将重新打包的 JAR rsync 到 `/opt/orcp/{ingest,admin}/` 并重启服务。
上一个 JAR 保留为 `orcp-ingest.jar.bak`，可用于手动回滚。

首次重启前，在 node-3 上：

```bash
sudo install -d -o orcp -g orcp /etc/orcp
sudo install -m 0640 -o root -g orcp /dev/stdin /etc/orcp/ingest.env <<'EOF'
KAFKA_BOOTSTRAP=node-1:9092,node-2:9092,node-3:9092
MYSQL_URL=jdbc:mysql://node-1:3306/orcp_detail?useSSL=false&serverTimezone=Asia/Shanghai
MYSQL_USER=orcp_rw
MYSQL_PASSWORD=<真实 rw 密码>
NACOS_SERVER=node-1:8848
NACOS_NAMESPACE=dev
EOF
sudo install -m 0640 -o root -g orcp /dev/stdin /etc/orcp/admin.env <<'EOF'
FLINK_REST=http://node-1:8081
KAFKA_BOOTSTRAP=node-1:9092,node-2:9092,node-3:9092
MYSQL_URL=jdbc:mysql://node-1:3306/orcp_detail?useSSL=false&serverTimezone=Asia/Shanghai
MYSQL_USER=orcp_ro
MYSQL_PASSWORD=<真实 ro 密码>
OB_URL=jdbc:mysql://ob-host:2881/orcp_dw?useUnicode=true&characterEncoding=utf8&serverTimezone=Asia/Shanghai
OB_USER=orcp_rw@tenant#cluster
OB_PASSWORD=<真实 ob 密码>
ORCP_ADMIN_USER=orcp-admin
ORCP_ADMIN_PASSWORD=<真实 admin 密码>
NACOS_SERVER=node-1:8848
NACOS_NAMESPACE=dev
EOF
```

`EnvironmentFile=` 设置为 `-/etc/orcp/*.env`（可选），缺失文件不影响启动，
但凭据会使用占位默认值。

### 1.8 提交 Flink 作业

```bash
# 以下两种方式均可。
make submit-flink                              # 调用 scripts/submit_job.sh
# -- 或通过管控服务（同时验证 REST 控制面）--
curl -u "${ORCP_ADMIN_USER}:${ORCP_ADMIN_PASSWORD}" \
     -F "jar=@orcp-flink-job/target/orcp-flink-job.jar" \
     -F "programArgs=--mysql.password ${MYSQL_PW} --ob.password ${OB_PW}" \
     http://node-3:8081/api/jobs/submit
```

验证：

```bash
curl -s http://node-1:8081/jobs/overview | python3 -m json.tool
# jobs[].state 应显示 "RUNNING"
```

---

## 2. 日常运维

### 2.1 生命周期命令

| 目标 | 命令 |
|------|------|
| 集群状态一览 | `make status` |
| 重部署 ingest | `make deploy-ingest` |
| 重部署 admin | `make deploy-admin` |
| 全新提交 Flink 作业 | `make submit-flink` |
| 计划停止（带 savepoint） | `make cancel JOB=<id>` |
| 临时触发 savepoint | `make savepoint JOB=<id>` |
| 从 savepoint 恢复 | `bash scripts/restore_from_savepoint.sh <path>` |
| 发送 100 条测试事件 | `make gen-events ARGS='--count 100'` |

`cancel` 和 `savepoint` 会在 stdout 输出 savepoint 路径，可直接管道给 `restore_from_savepoint.sh`。

### 2.2 通过管控服务操作（需 Basic 认证）

所有写操作端点位于 `POST /api/jobs`，使用 `/etc/orcp/admin.env` 中设置的凭据保护。
`/api/health` 等只读端点匿名开放，供 Prometheus 抓取。

```bash
# 聚合健康检查（200 UP 或 503 DOWN）：
curl -s http://node-3:8081/api/health | python3 -m json.tool

# 作业列表（需认证）：
curl -u $USER:$PW http://node-3:8081/api/jobs | python3 -m json.tool

# 提交（上传 + 运行一步完成）：
curl -u $USER:$PW \
     -F "jar=@orcp-flink-job/target/orcp-flink-job.jar" \
     http://node-3:8081/api/jobs/submit

# 触发 savepoint 并停止：
curl -u $USER:$PW -X POST \
     "http://node-3:8081/api/jobs/<jobId>/cancel?drain=false"
```

### 2.3 验收测试

执行 §8.8 端到端验收清单。默认非破坏性；通过命令行参数启用破坏性验收门：

```bash
bash scripts/acceptance_test.sh                    # 验收门 1、3、4
INGEST_HOST=node-3 bash scripts/acceptance_test.sh # 增加门 3 的重启
TM_HOST=node-3  bash scripts/acceptance_test.sh --with-self-heal
JM_HOST=node-1  bash scripts/acceptance_test.sh --with-alerts
bash scripts/acceptance_test.sh --all              # 全部五个门
```

所有选中验收门通过则退出码为 0。将运行结果填入
[`docs/ACCEPTANCE_REPORT.md`](./ACCEPTANCE_REPORT.md)。

---

## 3. 故障排查手册

下表将值班人员可能看到的症状映射到最快的排查路径。

### 3.1 `OrcpFlinkJobDown` 或 `flink_jobmanager_job_uptime == 0`

```bash
# 1. JobManager 进程是否存活？
sudo systemctl status flink-jobmanager      # 在 node-1 上

# 2. 作业是否因上游（Kafka/MySQL/OB）故障而失败？
curl -s http://node-3:8081/api/health | python3 -m json.tool

# 3. 查看 TaskManager 日志中的最后一个异常：
sudo tail -200 /var/log/orcp/flink/flink-*-taskmanager-*.log | \
    grep -A5 -E 'ERROR|Caused by' | head -80
```

如作业因重启次数耗尽已放弃，找到最新保留的检查点恢复：

```bash
ls -lt /data/flink/checkpoints | head
bash scripts/restore_from_savepoint.sh file:///data/flink/checkpoints/<id>/chk-<n>
```

### 3.2 `OrcpFlinkJobRestartingRepeatedly`（频繁重启）

5 分钟内 3 次重启说明 fixed-delay 策略掩盖了持续性错误。常见原因：

1. **Schema 漂移** —— 阶段 D 的 `ForwardService` wire 格式已变更，但 `01_source_kafka.sql` 中的 Flink SQL 未同步更新。
   症状：日志中出现 `Failed to deserialize JSON field`。
2. **OceanBase 认证失败** —— Nacos 中密码已轮换，但 Flink 作业使用旧值提交。
   症状：`Access denied for user 'orcp_rw'@'...'`。
3. **MySQL 连接池耗尽** —— JDBC lookup 缓存冷启动，大量并发查 MySQL。
   验证：在 node-1 上执行 `SHOW PROCESSLIST`。

取消后以正确参数重新提交：

```bash
make cancel JOB=<id>        # 输出 savepoint 路径
make submit-flink           # 重新读取 job.properties / CLI 参数
```

### 3.3 `OrcpKafkaConsumerLagHigh`（积压 > 1 万条）

```bash
# 找出积压集中在哪个分区：
kafka-consumer-groups.sh --bootstrap-server node-1:9092 \
    --group orcp-flink-job --describe
```

如某分区明显落后，通常是某个 TaskManager 处理慢。检查：

- `flink_taskmanager_Status_JVM_Memory_Heap_Used` —— 某 TM 是否接近 OOM？
- `flink_taskmanager_Status_JVM_CPU_Load` —— 某 TM 是否 CPU 满载？

临时缓解：在 node-2/node-3 上增加 TaskManager 数量（多出 slot → 多个分区可并行处理）。

### 3.4 `OrcpIngestFailureRateHigh`

`grep ERROR /var/log/orcp/orcp-ingest.log | head -20` 基本能直接定位原因。
压测中遇到的三个真实原因：

- MySQL 宕机 → `CommunicationsException` → `/api/health` 显示 MySQL DOWN
- Kafka broker 重启 → `forward()` 抛出 `TimeoutException` —— 瞬时故障
- `t_dedup` 不断增长 → node-1 磁盘即将打满

第三种情况需要在 node-1 上添加定时清理任务（参见 §4.1）。

### 3.5 `/api/health` 仅 OceanBase 显示 DOWN

```bash
# 从 node-3（orcp-admin 所在节点）直接验证连通性：
mysql -h"${OB_HOST}" -P2881 -u"orcp_rw@tenant#cluster" -p"${OB_PW}" \
      -e "SELECT 1" orcp_dw
```

常见原因（按频率排序）：

1. 网络 —— node-3 到 OB 的防火墙规则丢失。用 `nc -zv ${OB_HOST} 2881` 测试。
2. 租户用户格式错误 —— 直连 2881 端口使用 `user@tenant#cluster`，走 OBProxy 2883 端口使用 `user@tenant`。少一个 `#` 就会返回 `Access denied`。
3. JDBC URL 缺少 `serverTimezone=` —— 不是网络问题，而是驱动层问题；探针实际上未到达 OB。

### 3.6 摄入持续重投相同消息

如事件日志中出现 `eventId already present in t_dedup, skipping`，说明去重层工作正常。
如果发现 `t_order` 出现**真实重复行**（即同一事件写入两次），可能原因：

- `t_dedup.event_id` 在 OB 侧并非真正主键。验证：
  `SHOW CREATE TABLE orcp_detail.t_dedup` —— 必须包含 `PRIMARY KEY (event_id)`。
- Caffeine 缓存掩盖了 DB 查询 —— 单独来看不影响正确性，但如果 DB 主键缺失，缓存只能提供概率去重。

---

## 4. 日常维护

### 4.1 数据保留策略

| 数据 | 默认保留 | 控制方式 |
|------|----------|----------|
| Kafka topic 数据 | 72 小时 | Kafka 配置中的 `log.retention.hours` |
| Flink 检查点 | 取消时保留 | `flink-conf.yaml` |
| `t_dedup` | 7 天（手动清理） | 在 node-1 定期运行下方 SQL |
| 服务日志 `/var/log/orcp/*` | 14 天、500MB 上限 | logback-spring.xml 滚动策略 |

唯一需要手动处理的是 `t_dedup`，在 node-1 以 MySQL admin 身份执行：

```sql
-- 通过 cron 或 MySQL 事件调度每日执行：
DELETE FROM orcp_detail.t_dedup WHERE created_at < NOW() - INTERVAL 7 DAY LIMIT 10000;
```

`LIMIT` 可控制繁忙表上的锁窗口大小。

### 4.2 密码轮换

Nacos 是配置的权威来源。在 Nacos 中更新 `orcp-ingest.yaml` 或 `orcp-admin.yaml`，
然后重启对应服务：

```bash
sudo systemctl restart orcp-ingest       # 在 node-3 上
sudo systemctl restart orcp-admin        # 在 node-3 上
```

Flink 作业的密码通过提交时的 `programArgs` 注入，因此轮换 OB 密码还需要：
`make cancel JOB=<id>` + 以新密码重新执行 `make submit-flink`。

### 4.3 版本升级

Java 依赖升级只需修改根 `pom.xml` 中的一行；基础设施升级修改对应的 `deploy/centos/*_install_*.sh` 脚本。
`DEV_SPEC.md` §2 中的版本矩阵是变更的权威定义。

整体升级顺序：

1. ZooKeeper —— 滚动升级，一次一个节点。
2. Kafka broker —— 滚动升级。
3. Flink —— 先用 savepoint 停止作业，升级 JM + TM，再从 savepoint 恢复。之后运行验收测试。
4. Spring 服务 —— `make deploy-ingest` / `deploy-admin` 原地重启。

---

## 5. 值班速查卡（可打印）

```
健康检查                  curl -s http://node-3:8081/api/health | jq .status
Flink UI                 http://node-1:8081
Grafana                  http://node-2:3000        （首次登录 admin/admin，请立即修改）
Alertmanager             http://node-2:9093
Nacos                    http://node-1:8848/nacos  （默认 nacos/nacos）

作业列表                  curl -s $FLINK_REST/jobs/overview | jq
触发 savepoint            make savepoint JOB=<id>
取消 + savepoint          make cancel JOB=<id>
从 savepoint 恢复         bash scripts/restore_from_savepoint.sh <path>

摄入日志                  tail -f /var/log/orcp/orcp-ingest.log       （node-3）
管控日志                  tail -f /var/log/orcp/orcp-admin.log        （node-3）
Flink JM 日志             tail -f /var/log/orcp/flink/flink-*-jobmanager-*.log  （node-1）

验收测试（安全）          bash scripts/acceptance_test.sh
验收测试（完整）          TM_HOST=node-3 JM_HOST=node-1 INGEST_HOST=node-3 \
                          bash scripts/acceptance_test.sh --all
```

以上涵盖了全部内容。如有遗漏，请作为 Bug 提 PR 到 `docs/OPS_RUNBOOK.md`。
