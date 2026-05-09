# ORCP Operations Runbook

> Companion to [`DEV_SPEC.md`](./DEV_SPEC.md). Where DEV_SPEC defines *what*
> was built, this document defines *how to run it*: first install, daily
> lifecycle, and what to do when something breaks.

Every command assumes you are the `orcp` user (created by
`deploy/centos/00_bootstrap.sh`) on the node where the command makes sense.
`sudo` is passwordless for that user.

---

## 0. Reference topology

The default 3-node layout from DEV_SPEC §3:

| Node     | Services                                                            |
|----------|---------------------------------------------------------------------|
| node-1   | ZooKeeper, Kafka broker, Flink JobManager, Nacos, MySQL (detail)    |
| node-2   | ZooKeeper, Kafka broker, Flink TaskManager, Prometheus, Grafana     |
| node-3   | ZooKeeper, Kafka broker, Flink TaskManager, orcp-ingest, orcp-admin |
| OB       | OceanBase 3.2.3 (managed externally)                                |

`cluster.env` on every node must set `NODE_ID`; otherwise the install
scripts in `deploy/centos/` refuse to run.

---

## 1. First install (cold bring-up)

Run each numbered step on the nodes listed. Stop and inspect if a step
fails — they are designed to be rerunnable, but running 30 out of order
will produce subtly wrong state.

### 1.1 Prepare every node

```bash
# On every node (node-1, node-2, node-3):
sudo install -d /etc/orcp
sudo cp deploy/centos/cluster.env.example /etc/orcp/cluster.env
sudo vi /etc/orcp/cluster.env          # set NODE_ID per host
sudo bash deploy/centos/00_bootstrap.sh
sudo bash deploy/centos/10_install_jdk8.sh
```

`00_bootstrap.sh` creates the `orcp` user, raises `nofile`/`nproc`, turns
off THP/swap, installs chrony, and writes `/etc/hosts` entries for all
three nodes. `10_install_jdk8.sh` lays down Temurin 8u402-b06 under
`/opt/jdk-8`.

### 1.2 ZooKeeper + Kafka (every node)

```bash
sudo bash deploy/centos/15_install_zookeeper.sh
sudo bash deploy/centos/20_install_kafka_zk.sh
```

`20_install_kafka_zk.sh` auto-creates `orcp.src.demo` and
`orcp.mid.events` on node-1. Verify from any node:

```bash
kafka-topics.sh --bootstrap-server node-1:9092 --list
# expected: orcp.src.demo, orcp.mid.events, __consumer_offsets, __transaction_state
```

### 1.3 Flink (every node)

```bash
sudo bash deploy/centos/30_install_flink.sh
```

Role is decided by `NODE_ID`: 1 runs JobManager, 2+ run TaskManagers. The
script copies `deploy/flink-conf/{flink-conf.yaml,log4j.properties,metrics.yaml}`
into `/opt/flink/conf/` and drops the 4 third-party JARs into `/opt/flink/lib/`.

```bash
curl -s http://node-1:8081/overview | python3 -m json.tool | head
# expect: "flink-version": "1.17.2", "taskmanagers": >= 1
```

### 1.4 MySQL (node-1 only), OceanBase (external)

```bash
# node-1:
sudo bash deploy/centos/40_install_mysql.sh
mysql -uroot -p < docs/SQL/mysql_detail_schema.sql
# rotate the placeholder passwords right away:
mysql -uroot -p -e "ALTER USER 'orcp_rw'@'%' IDENTIFIED BY '<real>';
                    ALTER USER 'orcp_ro'@'%' IDENTIFIED BY '<real>'; FLUSH PRIVILEGES"
```

For OceanBase, on any box with `obclient`:

```bash
obclient -h${OB_HOST} -P2881 -u"orcp_rw@tenant#cluster" -p < docs/SQL/oceanbase_dw_schema.sql
obclient -h${OB_HOST} -P2881 -u"orcp_rw@tenant#cluster" -p -e "SHOW TABLES" orcp_dw
# expect: agg_order_1min, agg_order_daily
```

### 1.5 Nacos (node-1)

```bash
sudo bash deploy/centos/50_install_nacos.sh
curl -fsS http://node-1:8848/nacos/v1/console/health/readiness   # expect 200 OK
```

Log in at `http://node-1:8848/nacos` (default nacos/nacos), then:

1. Create namespace `dev` (or `prod`).
2. Upload config `orcp-ingest.yaml` with the production MySQL + Kafka
   endpoints and a real `datasource.password`.
3. Upload config `orcp-admin.yaml` with the real `orcp.admin.password`,
   OB credentials, and any webhook URL you wire later.

### 1.6 Prometheus + Grafana (node-2)

```bash
sudo bash deploy/centos/60_install_prom_grafana.sh
curl -fsS http://node-2:9090/-/ready      # Prometheus
curl -fsS http://node-2:3000/api/health   # Grafana
```

Now install the alert rules and dashboards:

```bash
# node-2:
sudo cp deploy/prometheus/orcp-alerts.yml /opt/prometheus/orcp-alerts.yml
sudo bash -c "grep -q orcp-alerts.yml /opt/prometheus/prometheus.yml || \
    sed -i '/^scrape_configs:/i\\rule_files:\\n  - /opt/prometheus/orcp-alerts.yml\\n' \
    /opt/prometheus/prometheus.yml"
sudo systemctl reload prometheus

# Grafana: Dashboards -> Import -> paste deploy/grafana/orcp-overview-dashboard.json
```

### 1.7 Build and deploy the Spring services

From a developer workstation or the bastion:

```bash
make build
INGEST_HOST=node-3 ADMIN_HOST=node-3 make deploy-systemd
INGEST_HOST=node-3 make deploy-ingest
ADMIN_HOST=node-3 make deploy-admin
```

`deploy-systemd` copies the two `.service` units into
`/etc/systemd/system/` and reloads. `deploy-ingest` / `deploy-admin` rsync
the repackaged JARs into `/opt/orcp/{ingest,admin}/` and restart the
service. The previous JAR is kept as `orcp-ingest.jar.bak` for a manual
roll back.

Before the first restart, on node-3:

```bash
sudo install -d -o orcp -g orcp /etc/orcp
sudo install -m 0640 -o root -g orcp /dev/stdin /etc/orcp/ingest.env <<'EOF'
KAFKA_BOOTSTRAP=node-1:9092,node-2:9092,node-3:9092
MYSQL_URL=jdbc:mysql://node-1:3306/orcp_detail?useSSL=false&serverTimezone=Asia/Shanghai
MYSQL_USER=orcp_rw
MYSQL_PASSWORD=<real rw password>
NACOS_SERVER=node-1:8848
NACOS_NAMESPACE=dev
EOF
sudo install -m 0640 -o root -g orcp /dev/stdin /etc/orcp/admin.env <<'EOF'
FLINK_REST=http://node-1:8081
KAFKA_BOOTSTRAP=node-1:9092,node-2:9092,node-3:9092
MYSQL_URL=jdbc:mysql://node-1:3306/orcp_detail?useSSL=false&serverTimezone=Asia/Shanghai
MYSQL_USER=orcp_ro
MYSQL_PASSWORD=<real ro password>
OB_URL=jdbc:mysql://ob-host:2881/orcp_dw?useUnicode=true&characterEncoding=utf8&serverTimezone=Asia/Shanghai
OB_USER=orcp_rw@tenant#cluster
OB_PASSWORD=<real ob password>
ORCP_ADMIN_USER=orcp-admin
ORCP_ADMIN_PASSWORD=<real admin password>
NACOS_SERVER=node-1:8848
NACOS_NAMESPACE=dev
EOF
```

`EnvironmentFile=` is set to `-/etc/orcp/*.env` (optional), so a missing
file is not fatal but credentials WILL default to placeholders.

### 1.8 Submit the Flink job

```bash
# Either path works.
make submit-flink                              # uses scripts/submit_job.sh
# -- OR via the admin service (exercises the REST control plane) --
curl -u "${ORCP_ADMIN_USER}:${ORCP_ADMIN_PASSWORD}" \
     -F "jar=@orcp-flink-job/target/orcp-flink-job.jar" \
     -F "programArgs=--mysql.password ${MYSQL_PW} --ob.password ${OB_PW}" \
     http://node-3:8081/api/jobs/submit
```

Then verify:

```bash
curl -s http://node-1:8081/jobs/overview | python3 -m json.tool
# jobs[].state should say "RUNNING"
```

---

## 2. Daily operations

### 2.1 Lifecycle commands

| Goal                           | Command                                             |
|--------------------------------|-----------------------------------------------------|
| cluster status one-liner       | `make status`                                       |
| redeploy ingest                | `make deploy-ingest`                                |
| redeploy admin                 | `make deploy-admin`                                 |
| fresh Flink submit             | `make submit-flink`                                 |
| planned Flink stop (savepoint) | `make cancel JOB=<id>`                              |
| ad-hoc savepoint               | `make savepoint JOB=<id>`                           |
| resume from savepoint          | `bash scripts/restore_from_savepoint.sh <path>`     |
| test load (100 events)         | `make gen-events ARGS='--count 100'`                |

`cancel` and `savepoint` print the resulting savepoint path on stdout —
pipe it straight into `restore_from_savepoint.sh`.

### 2.2 Via the admin service (requires Basic auth)

All write endpoints are under `POST /api/jobs`, protected by the credentials
you set in `/etc/orcp/admin.env`. Read-only endpoints like `/api/health`
are anonymous so Prometheus can scrape them.

```bash
# Health aggregate (returns 200 UP or 503 DOWN):
curl -s http://node-3:8081/api/health | python3 -m json.tool

# Jobs list (auth required):
curl -u $USER:$PW http://node-3:8081/api/jobs | python3 -m json.tool

# Submit (upload + run in one step):
curl -u $USER:$PW \
     -F "jar=@orcp-flink-job/target/orcp-flink-job.jar" \
     http://node-3:8081/api/jobs/submit

# Savepoint + stop:
curl -u $USER:$PW -X POST \
     "http://node-3:8081/api/jobs/<jobId>/cancel?drain=false"
```

### 2.3 Acceptance test

Runs the §8.8 checklist end-to-end. Non-destructive by default; opt in to
the destructive gates with flags:

```bash
bash scripts/acceptance_test.sh                    # gates 1, 3, 4
INGEST_HOST=node-3 bash scripts/acceptance_test.sh # adds gate 3's restart
TM_HOST=node-3  bash scripts/acceptance_test.sh --with-self-heal
JM_HOST=node-1  bash scripts/acceptance_test.sh --with-alerts
bash scripts/acceptance_test.sh --all              # all five gates
```

Exit code 0 iff every selected gate PASSed. Fill
[`docs/ACCEPTANCE_REPORT.md`](./ACCEPTANCE_REPORT.md) with the run output.

---

## 3. Troubleshooting playbook

The symptom table maps the thing an on-call would see to the fastest
diagnostic.

### 3.1 `OrcpFlinkJobDown` or `flink_jobmanager_job_uptime == 0`

```bash
# 1. Is the JobManager process alive?
sudo systemctl status flink-jobmanager      # on node-1

# 2. Did the job fail because of an upstream (Kafka/MySQL/OB) outage?
curl -s http://node-3:8081/api/health | python3 -m json.tool

# 3. Tail the TaskManager log for the last exception:
sudo tail -200 /var/log/orcp/flink/flink-*-taskmanager-*.log | \
    grep -A5 -E 'ERROR|Caused by' | head -80
```

If the job simply failed too many times and the restart strategy gave
up, find the latest retained checkpoint and resume:

```bash
ls -lt /data/flink/checkpoints | head
bash scripts/restore_from_savepoint.sh file:///data/flink/checkpoints/<id>/chk-<n>
```

### 3.2 `OrcpFlinkJobRestartingRepeatedly` (flapping)

Three restarts in five minutes means the delay strategy is papering over
a persistent error. Usual suspects:

1. **Schema drift** — Stage D's `ForwardService` wire format changed but
   the Flink SQL in `01_source_kafka.sql` wasn't updated. Symptom: log
   contains `Failed to deserialize JSON field`.
2. **OceanBase auth** — password rotated in Nacos but the Flink job was
   submitted with the old value. Symptom: `Access denied for user
   'orcp_rw'@'...'`.
3. **MySQL connection pool exhausted** — the JDBC lookup cache is cold
   and hammering MySQL. Verify with `SHOW PROCESSLIST` on node-1.

Cancel + resubmit with corrected params:

```bash
make cancel JOB=<id>        # prints a savepoint path
make submit-flink           # re-reads job.properties / CLI args
```

### 3.3 `OrcpKafkaConsumerLagHigh` (> 10k pending records)

```bash
# Where is the lag concentrated?
kafka-consumer-groups.sh --bootstrap-server node-1:9092 \
    --group orcp-flink-job --describe
```

If one partition is far behind, likely a slow TaskManager. Check:

- `flink_taskmanager_Status_JVM_Memory_Heap_Used` — is a TM near OOM?
- `flink_taskmanager_Status_JVM_CPU_Load` — is one CPU-bound?

Temporary mitigation: bump TaskManager count on node-2/node-3 (2+ more
slots → 2+ more partitions can run in parallel).

### 3.4 `OrcpIngestFailureRateHigh`

`grep ERROR /var/log/orcp/orcp-ingest.log | head -20` almost always tells
you. The three real causes seen in soak tests:

- MySQL is down → `CommunicationsException` → `/api/health` shows MySQL DOWN
- Kafka broker bounced → `TimeoutException` on `forward()` — transient
- `t_dedup` growing unbounded → disk on node-1 nearing full

The third one wants a cron entry on node-1 (not shipped — see §4.1).

### 3.5 `/api/health` DOWN for OceanBase only

```bash
# Direct connectivity check from node-3 (where orcp-admin runs):
mysql -h"${OB_HOST}" -P2881 -u"orcp_rw@tenant#cluster" -p"${OB_PW}" \
      -e "SELECT 1" orcp_dw
```

Common reasons (from most to least frequent):

1. Network — firewall between node-3 and OB lost the rule. Test with
   `nc -zv ${OB_HOST} 2881`.
2. Tenant user typo — user format is `user@tenant#cluster` for direct
   port 2881, `user@tenant` for OBProxy port 2883. One `#` off and you
   get `Access denied`.
3. `serverTimezone=` missing from the JDBC URL — not a connectivity
   issue but a driver-level one; the probe never actually reaches OB.

### 3.6 Ingest keeps redelivering the same message

The dedup layer is doing its job if the event is logged as
`eventId already present in t_dedup, skipping`. If you see a *real*
replay (i.e. the event does land in `t_order` twice), it is one of:

- `t_dedup.event_id` is not actually the primary key on OB side. Check:
  `SHOW CREATE TABLE orcp_detail.t_dedup` — `PRIMARY KEY (event_id)`
  must be present.
- The Caffeine cache is masking the DB check — not a correctness issue
  on its own, but if the DB primary key is missing the cache gives you
  probabilistic dedup only.

---

## 4. Maintenance

### 4.1 Retention

| Thing                          | Default                    | Controlled by                      |
|--------------------------------|----------------------------|------------------------------------|
| Kafka topic data               | 72 hours                   | `log.retention.hours` in Kafka cfg |
| Flink checkpoints              | RETAIN_ON_CANCELLATION     | `flink-conf.yaml`                  |
| `t_dedup`                      | 7 days (manual cleanup)    | run the SQL below from cron        |
| Service logs `/var/log/orcp/*` | 14 days, 500MB cap         | logback-spring.xml, rolling policy |

The only missing piece is `t_dedup`. On node-1 as the MySQL admin:

```sql
-- Nightly via cron or a scheduled event:
DELETE FROM orcp_detail.t_dedup WHERE created_at < NOW() - INTERVAL 7 DAY LIMIT 10000;
```

`LIMIT` keeps the lock window small on a busy table.

### 4.2 Password rotation

Nacos is the source of truth. Update `orcp-ingest.yaml` or
`orcp-admin.yaml` in Nacos, then restart the affected service(s):

```bash
sudo systemctl restart orcp-ingest       # on node-3
sudo systemctl restart orcp-admin        # on node-3
```

The Flink job reads its passwords from `programArgs` at submit time, so
rotating the OB password also requires `make cancel JOB=<id>` + fresh
`make submit-flink` with the new value.

### 4.3 Version upgrades

Component upgrades change a single line in the root `pom.xml` (for Java
deps) or the `deploy/centos/*_install_*.sh` scripts (for infra). The
version matrix in `DEV_SPEC.md` §2 is the definition of record.

Upgrade order when bumping everything together:

1. ZooKeeper — rolling, one node at a time.
2. Kafka brokers — rolling.
3. Flink — stop the job with savepoint, upgrade JM + TMs, resume from
   savepoint. Run the acceptance test afterwards.
4. Spring services — `make deploy-ingest` / `deploy-admin` restart in
   place.

---

## 5. On-call cheat sheet (printable)

```
HEALTH                   curl -s http://node-3:8081/api/health | jq .status
FLINK UI                 http://node-1:8081
GRAFANA                  http://node-2:3000        (admin/admin first login)
ALERTMANAGER             http://node-2:9093
NACOS                    http://node-1:8848/nacos  (nacos/nacos default)

JOB LIST                 curl -s $FLINK_REST/jobs/overview | jq
CURRENT SAVEPOINT        make savepoint JOB=<id>
CANCEL + SAVEPOINT       make cancel  JOB=<id>
RESUME                   bash scripts/restore_from_savepoint.sh <path>

INGEST LOGS              tail -f /var/log/orcp/orcp-ingest.log       (node-3)
ADMIN LOGS               tail -f /var/log/orcp/orcp-admin.log        (node-3)
FLINK JM LOG             tail -f /var/log/orcp/flink/flink-*-jobmanager-*.log   (node-1)

ACCEPTANCE (safe)        bash scripts/acceptance_test.sh
ACCEPTANCE (all)         TM_HOST=node-3 JM_HOST=node-1 INGEST_HOST=node-3 \
                          bash scripts/acceptance_test.sh --all
```

That is the whole thing. Anything not covered above is a bug in this
document — file it as a PR against `docs/OPS_RUNBOOK.md`.
