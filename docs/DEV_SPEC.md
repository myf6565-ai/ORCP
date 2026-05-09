# ORCP 实时计算系统 —— 开发需求与落地规范 (v1.1 · JDK8 / OceanBase 3.2.3)

> 技术栈：Spring Cloud + Apache Kafka + Apache Flink + OceanBase 3.2.3 (MySQL 模式)
> 运行时约束：**JDK 8（生产固定）**
> 目标：**最小可生产、可落地**的实时计算系统，不引入非必要组件。
> 本文档按 Claude Code / Kiro Autonomous 可直接执行的任务清单 + 明确命令 + 目录契约 组织。

> v1.1 变更说明：因生产环境要求 JDK 8 + OceanBase 3.2.3，所有组件已按"最近一个仍稳定兼容 JDK 8"的原则整体下调版本，并将 Kafka 切回 ZooKeeper 模式，OceanBase 接入改为官方 Connector/J 驱动。

---

## 0. 阅读约定

- 所有命令块默认在 **CentOS 7.9 / 8 / Stream 9** 的普通用户 `orcp`（具备 sudo）下执行。
- 所有代码路径以仓库根 `ORCP/` 为基准。
- 任务清单使用 `- [ ]` 表示待办、`- [x]` 表示完成。
- 版本选型在 §2 锁定，后续 pom / Dockerfile / 脚本必须严格对齐。
- **Java 代码强制 source/target = 1.8**，禁止使用 record / var / text blocks / sealed / switch 表达式。

---

## 1. 项目目标（边界）

必须实现（MUST）：

1. 从纯净 CentOS 云服务器开始，**脚本化**搭建：JDK 8、ZooKeeper、Kafka（ZK 模式）、Flink 1.17 Standalone 集群、OceanBase 3.2.3 连接可达性验证。
2. 基于 Spring Cloud 开发一个 Flink 驱动的应用：
   - 消费外部 Kafka topic 消息；
   - 写入**本地 MySQL 明细库**（作为维表/事实表来源）；
   - 使用 **Flink SQL** 对本地库 + Kafka 流做多表关联聚合；
   - 聚合结果**落库 OceanBase 3.2.3（MySQL 模式）**，使用 OceanBase 官方 JDBC 驱动。
3. 具备最小生产辅助能力：配置管理、服务注册与发现、健康检查、指标与日志采集、作业提交与恢复脚本、异常告警通道。

明确不做（OUT OF SCOPE）：

- 不引入 Kubernetes / Flink on K8s（仅保留 Standalone + systemd）；
- 不做多租户、多环境 CI/CD 平台（仅给出 Shell 部署脚本与 Makefile）；
- 不做前端管理 UI（Flink Web UI + Spring Boot Actuator 即可）；
- 不做实时数仓分层建模（仅一条端到端链路跑通）。

---

## 2. 技术栈与版本选型（锁定 · JDK 8 版）

| 组件 | 版本 | 选型说明 |
|---|---|---|
| OS | CentOS 7.9 / CentOS Stream 8 | 脚本两端兼容 |
| **JDK** | **OpenJDK 8u402（Temurin 8）** | 生产约束 |
| ZooKeeper | 3.7.2 | 与 Kafka 3.5.x 官方推荐一致 |
| **Apache Kafka** | **3.5.2（ZooKeeper 模式）** | 最后一个完整支持 JDK 8 broker、同时 KRaft 已成熟的线路；选 ZK 模式以匹配保守生产运维 |
| **Apache Flink** | **1.17.2** | JDK 8 完整支持的最后稳定版本；1.18 起 JDK 8 已 deprecated |
| **Spring Boot** | **2.7.18** | 2.x 最终 LTS，完整支持 JDK 8 |
| **Spring Cloud** | **2021.0.9 (Jubilee)** | 与 Spring Boot 2.7 对齐 |
| **Spring Cloud Alibaba** | **2021.0.5.0** | 与 Spring Cloud 2021.0.x 对齐；内置 Nacos / Sentinel / Seata 依赖管理 |
| Nacos Server | 2.2.3 | JDK 8 运行时兼容，稳定版 |
| Nacos Client | 2.2.3 | 随 Spring Cloud Alibaba 2021.0.5.0 引入 |
| MySQL（本地明细） | 8.0.36 | 作为 Flink JDBC 维表源；也可降至 5.7.44 |
| **OceanBase** | **3.2.3（MySQL 模式）** | 生产约束 |
| **OceanBase JDBC 驱动** | **com.oceanbase:oceanbase-client:2.4.7** | OB 3.x 官方推荐；兼容性优于纯 MySQL 驱动 |
| MySQL JDBC 驱动 | mysql-connector-java 8.0.28 | 用于本地 MySQL；与 JDK 8 / Flink 1.17 匹配 |
| Flink Kafka Connector | flink-connector-kafka 1.17.2（jar: `flink-sql-connector-kafka-1.17.2.jar`） | 与 Flink 主版本绑定 |
| Flink JDBC Connector | flink-connector-jdbc 3.1.2-1.17 | 支持自定义 JDBC Driver（OceanBase 可用） |
| MyBatis-Plus | 3.5.5 | 与 Spring Boot 2.7 匹配 |
| 构建工具 | Maven 3.8.8 | JDK 8 兼容的稳定版本 |
| Lombok | 1.18.30 | JDK 8 兼容 |
| 监控 | Prometheus 2.45.x + Grafana 10.1.x + Flink PrometheusReporter | Prometheus/Grafana 不受 JDK 限制 |

> 选型原则：**每个组件选取"仍对 JDK 8 提供官方支持 + 在对应大版本线中最后一个稳定 patch"**，避免使用已宣布废弃 JDK 8 的新大版本。

---

## 3. 总体架构

```
                ┌───────────────────────┐
                │   External Kafka      │
                │   topic: orcp.src.*   │
                └──────────┬────────────┘
                           │ consume
                           ▼
   ┌─────────────────────────────────────────────────────┐
   │            orcp-ingest (Spring Boot 2.7 App)        │
   │  - KafkaListener 消费消息                            │
   │  - 校验 / 反序列化 / 去重                            │
   │  - 写入本地 MySQL 明细表 (orcp_detail.*)             │
   │  - 同步转发到内部 topic: orcp.mid.events             │
   └─────────────────────────────┬───────────────────────┘
                                 │
                                 ▼
   ┌─────────────────────────────────────────────────────┐
   │        orcp-flink-job (Flink 1.17.2 Job)            │
   │  - Source: Kafka(orcp.mid.events)                   │
   │  - Dim:    JDBC Lookup (MySQL orcp_detail.*)        │
   │  - SQL:    多表 JOIN + 窗口聚合                      │
   │  - Sink:   JDBC → OceanBase 3.2.3 (orcp_dw.*)       │
   │           driver: com.oceanbase.jdbc.Driver          │
   └─────────────────────────────┬───────────────────────┘
                                 │
                                 ▼
                   ┌─────────────────────────┐
                   │ OceanBase 3.2.3         │
                   │ MySQL 模式 / schema:    │
                   │ orcp_dw                 │
                   └─────────────────────────┘

   ┌─────────────────────────────────────────────────────┐
   │   orcp-admin (Spring Boot 2.7 App) 【最小管控】       │
   │  - 提交/取消 Flink 作业（REST 调用 Flink REST API）  │
   │  - 健康聚合、配置热更新、Prometheus 指标暴露         │
   └─────────────────────────────────────────────────────┘

   支撑：ZooKeeper、Nacos、Prometheus+Grafana、
         Logback 文件滚动、钉钉/飞书 Webhook 告警
```

部署物理拓扑（最小生产 3 台 4C8G）：

- **node-1**：ZooKeeper、Kafka Broker、Flink JobManager、Nacos、MySQL（本地明细）
- **node-2**：ZooKeeper、Kafka Broker、Flink TaskManager、Prometheus、Grafana
- **node-3**：ZooKeeper、Kafka Broker、Flink TaskManager、orcp-ingest、orcp-admin
- **OceanBase 3.2.3**：独立集群或云托管，网络可达即可

> 单机 POC 可用 1 台 8C16G 跑全部组件。

---

## 4. 仓库与代码结构

### 4.1 仓库目录契约

```
ORCP/
├── README.md
├── Makefile
├── docs/
│   ├── DEV_SPEC.md                       # 本文档
│   ├── OPS_RUNBOOK.md
│   └── SQL/
│       ├── mysql_detail_schema.sql
│       └── oceanbase_dw_schema.sql       # 注意：OceanBase 3.2.3 DDL 兼容 MySQL 5.7 语法
├── deploy/
│   ├── centos/
│   │   ├── 00_bootstrap.sh
│   │   ├── 10_install_jdk8.sh            # ← 原 jdk17
│   │   ├── 15_install_zookeeper.sh       # ← 新增
│   │   ├── 20_install_kafka_zk.sh        # ← ZK 模式
│   │   ├── 30_install_flink.sh
│   │   ├── 40_install_mysql.sh
│   │   ├── 50_install_nacos.sh
│   │   ├── 60_install_prom_grafana.sh
│   │   └── 99_healthcheck.sh
│   ├── systemd/
│   │   ├── zookeeper.service
│   │   ├── kafka.service
│   │   ├── flink-jobmanager.service
│   │   ├── flink-taskmanager.service
│   │   └── nacos.service
│   └── flink-conf/
│       ├── flink-conf.yaml
│       ├── log4j.properties
│       └── metrics.yaml
├── orcp-common/
├── orcp-ingest/
├── orcp-flink-job/
├── orcp-admin/
└── scripts/
    ├── submit_job.sh
    ├── cancel_job.sh
    ├── savepoint.sh
    └── restore_from_savepoint.sh
```

### 4.2 Maven 父 POM 约束（关键属性）

```xml
<properties>
    <java.version>1.8</java.version>
    <maven.compiler.source>1.8</maven.compiler.source>
    <maven.compiler.target>1.8</maven.compiler.target>
    <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>

    <spring-boot.version>2.7.18</spring-boot.version>
    <spring-cloud.version>2021.0.9</spring-cloud.version>
    <spring-cloud-alibaba.version>2021.0.5.0</spring-cloud-alibaba.version>

    <flink.version>1.17.2</flink.version>
    <flink.scala.binary.version>2.12</flink.scala.binary.version>
    <flink-connector-kafka.version>1.17.2</flink-connector-kafka.version>
    <flink-connector-jdbc.version>3.1.2-1.17</flink-connector-jdbc.version>

    <kafka-clients.version>3.5.2</kafka-clients.version>
    <mysql.connector.version>8.0.28</mysql.connector.version>
    <oceanbase.client.version>2.4.7</oceanbase.client.version>
    <mybatis-plus.version>3.5.5</mybatis-plus.version>
    <lombok.version>1.18.30</lombok.version>
</properties>

<dependencyManagement>
    <dependencies>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-dependencies</artifactId>
            <version>${spring-boot.version}</version>
            <type>pom</type><scope>import</scope>
        </dependency>
        <dependency>
            <groupId>org.springframework.cloud</groupId>
            <artifactId>spring-cloud-dependencies</artifactId>
            <version>${spring-cloud.version}</version>
            <type>pom</type><scope>import</scope>
        </dependency>
        <dependency>
            <groupId>com.alibaba.cloud</groupId>
            <artifactId>spring-cloud-alibaba-dependencies</artifactId>
            <version>${spring-cloud-alibaba.version}</version>
            <type>pom</type><scope>import</scope>
        </dependency>
    </dependencies>
</dependencyManagement>
```

关键约束：

- Flink 作业 jar 中 `flink-streaming-java` / `flink-table-*` / `flink-clients` / `flink-table-planner-loader` / `flink-table-runtime` 均用 **`<scope>provided</scope>`**，防止 ClassLoader 冲突。
- Spring Boot 2.7 默认 `spring.config.import` 优先，Nacos 接入需**额外显式引入** `spring-cloud-starter-bootstrap`（2020 版以后默认关闭 bootstrap）。
- 依赖中 `jakarta.*` 相关包一律**不要**引入（那是 Boot 3.x 才用到的）；统一使用 `javax.*`。

---

## 5. 环境搭建方案（CentOS 从零）

### 5.1 前置准备（所有节点）

脚本：`deploy/centos/00_bootstrap.sh`

- [ ] 创建部署用户 `orcp` 并加入 sudoers
- [ ] 关闭 SELinux / firewalld，或按 §5.8 放通端口
- [ ] 配置 NTP 时钟同步
- [ ] 配置 `/etc/hosts` 使用 `node-1 / node-2 / node-3` 短名
- [ ] 提升文件句柄：`/etc/security/limits.d/99-orcp.conf` 设 `nofile=655360 nproc=655360`
- [ ] 关闭 THP / swap（Kafka、Flink 建议）

### 5.2 安装 JDK 8

脚本：`deploy/centos/10_install_jdk8.sh`

- [ ] 下载 Eclipse Temurin 8u402 tar.gz 到 `/opt/jdk-8`
- [ ] 写 `/etc/profile.d/jdk.sh`：
  ```bash
  export JAVA_HOME=/opt/jdk-8
  export PATH=$JAVA_HOME/bin:$PATH
  ```
- [ ] 校验：`java -version` 输出 `1.8.0_402` 且 vendor 为 Temurin
- [ ] 为 Flink / Kafka 设定 `JAVA_TOOL_OPTIONS=-Dfile.encoding=UTF-8`

### 5.3 安装 ZooKeeper 3.7.2（3 节点）

脚本：`deploy/centos/15_install_zookeeper.sh`

- [ ] 下载 `apache-zookeeper-3.7.2-bin.tar.gz` 解压到 `/opt/zookeeper`
- [ ] `conf/zoo.cfg` 关键项：
  ```
  tickTime=2000
  initLimit=10
  syncLimit=5
  dataDir=/data/zk/data
  dataLogDir=/data/zk/log
  clientPort=2181
  4lw.commands.whitelist=stat,ruok,conf,mntr
  server.1=node-1:2888:3888
  server.2=node-2:2888:3888
  server.3=node-3:2888:3888
  ```
- [ ] 每节点 `echo <id> > /data/zk/data/myid`
- [ ] systemd 单元：`deploy/systemd/zookeeper.service`
- [ ] 验证：`echo ruok | nc node-1 2181` → `imok`

### 5.4 安装 Kafka 3.5.2（ZooKeeper 模式）

脚本：`deploy/centos/20_install_kafka_zk.sh`

- [ ] 下载 `kafka_2.13-3.5.2.tgz` 解压到 `/opt/kafka`
- [ ] `config/server.properties` 关键项：
  ```
  broker.id=1                                      # 每节点递增
  listeners=PLAINTEXT://0.0.0.0:9092
  advertised.listeners=PLAINTEXT://node-1:9092     # 每节点相应修改
  log.dirs=/data/kafka-logs
  num.partitions=3
  default.replication.factor=3
  min.insync.replicas=2
  offsets.topic.replication.factor=3
  transaction.state.log.replication.factor=3
  transaction.state.log.min.isr=2
  log.retention.hours=72
  zookeeper.connect=node-1:2181,node-2:2181,node-3:2181/kafka
  auto.create.topics.enable=false
  ```
- [ ] `systemctl enable --now kafka`
- [ ] 验证：
  ```bash
  kafka-topics.sh --bootstrap-server node-1:9092 --create \
      --topic orcp.src.demo --partitions 3 --replication-factor 3
  kafka-topics.sh --bootstrap-server node-1:9092 --create \
      --topic orcp.mid.events --partitions 3 --replication-factor 3
  ```

### 5.5 安装 Flink 1.17.2 Standalone 集群

脚本：`deploy/centos/30_install_flink.sh`

- [ ] 下载 `flink-1.17.2-bin-scala_2.12.tgz` 解压到 `/opt/flink`
- [ ] 放入 `/opt/flink/lib`（**三方 jar 与 Flink 主版本严格对齐**）：
  - `flink-sql-connector-kafka-1.17.2.jar`
  - `flink-connector-jdbc-3.1.2-1.17.jar`
  - `mysql-connector-java-8.0.28.jar`
  - `oceanbase-client-2.4.7.jar`
- [ ] `conf/flink-conf.yaml`：
  ```yaml
  jobmanager.rpc.address: node-1
  jobmanager.memory.process.size: 2g
  taskmanager.memory.process.size: 4g
  taskmanager.numberOfTaskSlots: 4
  parallelism.default: 2

  state.backend: rocksdb
  state.backend.incremental: true
  state.checkpoints.dir: file:///data/flink/checkpoints
  state.savepoints.dir: file:///data/flink/savepoints
  execution.checkpointing.interval: 60 s
  execution.checkpointing.mode: EXACTLY_ONCE
  execution.checkpointing.min-pause: 30 s
  execution.checkpointing.timeout: 10 min

  restart-strategy: fixed-delay
  restart-strategy.fixed-delay.attempts: 10
  restart-strategy.fixed-delay.delay: 30 s

  metrics.reporter.prom.factory.class: org.apache.flink.metrics.prometheus.PrometheusReporterFactory
  metrics.reporter.prom.port: 9250-9260

  env.java.opts: "-XX:+UseG1GC -XX:MaxGCPauseMillis=200 -Dfile.encoding=UTF-8"
  ```
- [ ] 同步将 `flink-metrics-prometheus-1.17.2.jar` 从 `opt/` 移到 `lib/` 下启用 reporter
- [ ] `conf/workers`：`node-2 / node-3`；`conf/masters`：`node-1:8081`
- [ ] systemd：`flink-jobmanager.service` / `flink-taskmanager.service`
- [ ] 在 node-1：`bin/start-cluster.sh`，访问 http://node-1:8081 验证

### 5.6 安装本地 MySQL 8.0（明细库）

脚本：`deploy/centos/40_install_mysql.sh`

- [ ] 使用 MySQL 8.0 官方 YUM 源安装
- [ ] 建库 `orcp_detail`、账号 `orcp_rw` / `orcp_ro`
- [ ] 执行 `docs/SQL/mysql_detail_schema.sql`
- [ ] `my.cnf`：`character-set-server=utf8mb4 / default-time-zone='+08:00'`

### 5.7 OceanBase 3.2.3 接入（不在此集群自建）

- [ ] 确认 OceanBase 3.2.3 目标集群：OBServer 地址、SQL 端口（默认 2883 via OBProxy 或 2881 直连）、租户 `tenant`、集群 `cluster`、用户 `orcp_rw`
- [ ] 执行 `docs/SQL/oceanbase_dw_schema.sql`（使用 **MySQL 5.7 兼容语法**，避免 MySQL 8.0 专有特性如 CHECK 约束、JSON 函数新语法、`INVISIBLE` 索引等）
- [ ] **连通性验证（推荐两种方式任一可通即可）**：

  方式 A：使用 OceanBase 官方 obclient
  ```bash
  obclient -h${OB_HOST} -P${OB_PORT} \
      -u'orcp_rw@tenant#cluster' -p${OB_PWD} -e 'select 1'
  ```

  方式 B：使用 OceanBase Connector/J（Flink / Spring 都会走这个）
  ```
  jdbc:oceanbase://${OB_HOST}:${OB_PORT}/orcp_dw?useUnicode=true&characterEncoding=utf8&useSSL=false&serverTimezone=Asia/Shanghai&rewriteBatchedStatements=true
  user  = orcp_rw@tenant#cluster      (或：orcp_rw@tenant   若通过 OBProxy)
  driver= com.oceanbase.jdbc.Driver
  ```

- [ ] **注意**：OB 3.2.3 与 MySQL 驱动某些边界行为（如 batch rewrite、last_insert_id、`ON DUPLICATE KEY UPDATE` 在分区表上的语义）有差异。本项目所有写入**必须**走 Flink JDBC 的 upsert 模式，由 connector 基于主键做 INSERT + UPDATE 分离，避免依赖 MySQL 特定语法。

### 5.8 端口一览

| 组件 | 端口 |
|---|---|
| ZooKeeper | 2181 / 2888 / 3888 |
| Kafka Broker | 9092 |
| Flink JobManager REST/UI | 8081 |
| Flink RPC / Blob / Query | 6123 / 6124 / 6125 |
| Flink Metrics (Prometheus) | 9250–9260 |
| MySQL | 3306 |
| OceanBase | 2881 / 2883 |
| Nacos | 8848 / 9848 |
| Prometheus | 9090 |
| Grafana | 3000 |

---

## 6. 核心应用开发任务

### 6.1 orcp-common（共享模块）

- [ ] 定义统一消息契约 `SourceEvent`：`eventId / bizType / bizKey / payload / eventTime / traceId`
- [ ] 定义聚合结果 DTO `AggResult`
- [ ] Jackson 配置：时间格式 `yyyy-MM-dd HH:mm:ss`，`WRITE_DATES_AS_TIMESTAMPS=false`；注意使用 **`com.fasterxml.jackson.datatype:jackson-datatype-jsr310`** 处理 `LocalDateTime`（JDK 8 time API）
- [ ] 统一常量：topic / 表名 / 配置 key

### 6.2 orcp-ingest（Spring Boot 2.7 消费服务）

目标：消费外部 Kafka → 入本地 MySQL 明细表 → 转发到内部 topic。

#### 依赖清单（`pom.xml`）

```xml
<dependencies>
    <dependency>
        <groupId>com.orcp</groupId><artifactId>orcp-common</artifactId>
    </dependency>

    <!-- Web / Actuator -->
    <dependency>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter-web</artifactId>
    </dependency>
    <dependency>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter-actuator</artifactId>
    </dependency>

    <!-- Kafka -->
    <dependency>
        <groupId>org.springframework.kafka</groupId>
        <artifactId>spring-kafka</artifactId>
    </dependency>

    <!-- 本地 MySQL -->
    <dependency>
        <groupId>com.baomidou</groupId>
        <artifactId>mybatis-plus-boot-starter</artifactId>
        <version>${mybatis-plus.version}</version>
    </dependency>
    <dependency>
        <groupId>mysql</groupId>
        <artifactId>mysql-connector-java</artifactId>
        <version>${mysql.connector.version}</version>
    </dependency>

    <!-- Nacos 注册 + 配置（Spring Cloud Alibaba 2021.0.5.0） -->
    <dependency>
        <groupId>com.alibaba.cloud</groupId>
        <artifactId>spring-cloud-starter-alibaba-nacos-discovery</artifactId>
    </dependency>
    <dependency>
        <groupId>com.alibaba.cloud</groupId>
        <artifactId>spring-cloud-starter-alibaba-nacos-config</artifactId>
    </dependency>
    <!-- Spring Boot 2.4+ 默认关闭 bootstrap，必须显式引入 -->
    <dependency>
        <groupId>org.springframework.cloud</groupId>
        <artifactId>spring-cloud-starter-bootstrap</artifactId>
    </dependency>

    <!-- Prometheus -->
    <dependency>
        <groupId>io.micrometer</groupId>
        <artifactId>micrometer-registry-prometheus</artifactId>
    </dependency>

    <!-- Lombok -->
    <dependency>
        <groupId>org.projectlombok</groupId>
        <artifactId>lombok</artifactId>
        <version>${lombok.version}</version>
        <scope>provided</scope>
    </dependency>
</dependencies>
```

#### 配置要点

- [ ] `bootstrap.yml`：Nacos server + 命名空间 + `group=ORCP_GROUP`；服务名 `orcp-ingest`
- [ ] `application.yml`：
  - Kafka consumer：`group-id=orcp-ingest`、`enable-auto-commit=false`、`max-poll-records=500`、`isolation-level=read_committed`、`ack-mode=MANUAL_IMMEDIATE`
  - Kafka producer：`acks=all`、`enable.idempotence=true`、`retries=10`、`max.in.flight.requests.per.connection=5`
  - 本地 MySQL 数据源：HikariCP `maximum-pool-size=10`
- [ ] `KafkaSourceListener`：
  - `@KafkaListener(topics = "${orcp.kafka.src-topic}")`
  - 基于 `eventId` 通过 **本地 Caffeine LRU（1 万条, TTL 10 min）+ MySQL `t_dedup`** 双层去重
  - 写入明细表（MyBatis-Plus）后，由 `ForwardService` 发送到内部 `orcp.mid.events`；失败回滚当前消息，**不 ack**，依赖 Kafka 重投
  - 手动 `acknowledge()` 提交位点
- [ ] Actuator 暴露：`health,info,metrics,prometheus`
- [ ] 日志：Logback JSON，路径 `/var/log/orcp/orcp-ingest.log`，按天滚动 14 天

#### Java 包结构

```
com.orcp.ingest
├── IngestApplication.java
├── config/
│   ├── KafkaConfig.java
│   └── MybatisConfig.java
├── listener/
│   └── SourceEventListener.java
├── service/
│   ├── DedupService.java
│   ├── DetailWriteService.java
│   └── ForwardService.java
├── mapper/            (MyBatis-Plus Mapper)
├── entity/
└── exception/
```

### 6.3 orcp-flink-job（Flink 1.17.2 作业）

目标：Kafka source → MySQL JDBC Lookup 维表 → Flink SQL 窗口聚合 → OceanBase 3.2.3 JDBC sink。

#### pom 要点

```xml
<properties>
    <flink.version>1.17.2</flink.version>
</properties>

<dependencies>
    <!-- provided，不打进 fat jar -->
    <dependency>
        <groupId>org.apache.flink</groupId>
        <artifactId>flink-streaming-java</artifactId>
        <version>${flink.version}</version>
        <scope>provided</scope>
    </dependency>
    <dependency>
        <groupId>org.apache.flink</groupId>
        <artifactId>flink-clients</artifactId>
        <version>${flink.version}</version>
        <scope>provided</scope>
    </dependency>
    <dependency>
        <groupId>org.apache.flink</groupId>
        <artifactId>flink-table-api-java-bridge</artifactId>
        <version>${flink.version}</version>
        <scope>provided</scope>
    </dependency>
    <dependency>
        <groupId>org.apache.flink</groupId>
        <artifactId>flink-table-planner-loader</artifactId>
        <version>${flink.version}</version>
        <scope>provided</scope>
    </dependency>
    <dependency>
        <groupId>org.apache.flink</groupId>
        <artifactId>flink-table-runtime</artifactId>
        <version>${flink.version}</version>
        <scope>provided</scope>
    </dependency>
    <dependency>
        <groupId>org.apache.flink</groupId>
        <artifactId>flink-json</artifactId>
        <version>${flink.version}</version>
        <scope>provided</scope>
    </dependency>

    <!-- 打进 fat jar 或放 Flink lib/（二选一） -->
    <dependency>
        <groupId>org.apache.flink</groupId>
        <artifactId>flink-connector-kafka</artifactId>
        <version>${flink.version}</version>
    </dependency>
    <dependency>
        <groupId>org.apache.flink</groupId>
        <artifactId>flink-connector-jdbc</artifactId>
        <version>${flink-connector-jdbc.version}</version>
    </dependency>
    <dependency>
        <groupId>mysql</groupId>
        <artifactId>mysql-connector-java</artifactId>
        <version>${mysql.connector.version}</version>
    </dependency>
    <dependency>
        <groupId>com.oceanbase</groupId>
        <artifactId>oceanbase-client</artifactId>
        <version>${oceanbase.client.version}</version>
    </dependency>
</dependencies>

<build>
    <plugins>
        <plugin>
            <groupId>org.apache.maven.plugins</groupId>
            <artifactId>maven-shade-plugin</artifactId>
            <version>3.5.0</version>
            <executions>
                <execution>
                    <phase>package</phase>
                    <goals><goal>shade</goal></goals>
                    <configuration>
                        <createDependencyReducedPom>false</createDependencyReducedPom>
                        <filters>
                            <filter>
                                <artifact>*:*</artifact>
                                <excludes>
                                    <exclude>META-INF/*.SF</exclude>
                                    <exclude>META-INF/*.DSA</exclude>
                                    <exclude>META-INF/*.RSA</exclude>
                                </excludes>
                            </filter>
                        </filters>
                        <transformers>
                            <transformer implementation="org.apache.maven.plugins.shade.resource.ServicesResourceTransformer"/>
                            <transformer implementation="org.apache.maven.plugins.shade.resource.ManifestResourceTransformer">
                                <mainClass>com.orcp.flink.OrcpFlinkJob</mainClass>
                            </transformer>
                        </transformers>
                    </configuration>
                </execution>
            </executions>
        </plugin>
    </plugins>
</build>
```

#### 作业入口（**JDK 8 语法兼容**）

```java
public class OrcpFlinkJob {
    public static void main(String[] args) throws Exception {
        ParameterTool p = ParameterTool.fromPropertiesFile(
            OrcpFlinkJob.class.getResourceAsStream("/job.properties"));
        p = p.mergeWith(ParameterTool.fromArgs(args));

        StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(60_000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(30_000L);
        env.getCheckpointConfig().setCheckpointTimeout(600_000L);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
            10, Time.seconds(30)));

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        SqlLoader.loadAndExecute(tEnv, p,
            "/sql/01_source_kafka.sql",
            "/sql/02_dim_mysql.sql",
            "/sql/03_sink_oceanbase.sql",
            "/sql/10_pipeline.sql");
    }
}
```

> `SqlLoader` 负责：加载 SQL 文件 → 用 `${var}` 做变量替换 → 按 `;` 切分逐句 `tEnv.executeSql()`，最后一条是 `INSERT INTO sink ...` 来触发作业。避免使用 JDK 11+ 的 `Files.readString`，用 `IOUtils.toString(in, UTF_8)` 即可。

#### 外置 SQL（要点）

`01_source_kafka.sql`

```sql
CREATE TABLE src_events (
    event_id    STRING,
    biz_type    STRING,
    biz_key     STRING,
    customer_id BIGINT,
    amount      DECIMAL(18,4),
    event_time  TIMESTAMP_LTZ(3),
    proc_time   AS PROCTIME(),
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND,
    PRIMARY KEY (event_id) NOT ENFORCED
) WITH (
    'connector' = 'kafka',
    'topic' = 'orcp.mid.events',
    'properties.bootstrap.servers' = '${kafka.bootstrap}',
    'properties.group.id' = 'orcp-flink-job',
    'scan.startup.mode' = 'group-offsets',
    'properties.isolation.level' = 'read_committed',
    'format' = 'json',
    'json.ignore-parse-errors' = 'true'
);
```

`02_dim_mysql.sql`

```sql
CREATE TABLE dim_customer (
    customer_id BIGINT,
    name        STRING,
    level       STRING,
    region      STRING,
    updated_at  TIMESTAMP(3),
    PRIMARY KEY (customer_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = '${mysql.url}',
    'driver' = 'com.mysql.cj.jdbc.Driver',
    'table-name' = 't_customer',
    'username' = '${mysql.user}',
    'password' = '${mysql.password}',
    'lookup.cache' = 'PARTIAL',
    'lookup.partial-cache.max-rows' = '100000',
    'lookup.partial-cache.expire-after-write' = '10 min',
    'lookup.max-retries' = '3'
);
```

`03_sink_oceanbase.sql`（**使用 OceanBase 官方 JDBC 驱动**）

```sql
CREATE TABLE sink_agg_1min (
    window_start TIMESTAMP(3),
    biz_type     STRING,
    customer_id  BIGINT,
    order_cnt    BIGINT,
    amount_sum   DECIMAL(18,4),
    PRIMARY KEY (window_start, biz_type, customer_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = '${ob.url}',
    'driver' = 'com.oceanbase.jdbc.Driver',
    'table-name' = 'agg_order_1min',
    'username' = '${ob.user}',
    'password' = '${ob.password}',
    'sink.buffer-flush.max-rows' = '1000',
    'sink.buffer-flush.interval' = '2s',
    'sink.max-retries' = '3',
    'sink.parallelism' = '2'
);
```

`10_pipeline.sql`

```sql
INSERT INTO sink_agg_1min
SELECT
    window_start,
    biz_type,
    e.customer_id,
    COUNT(*)        AS order_cnt,
    SUM(e.amount)   AS amount_sum
FROM TABLE(
    TUMBLE(TABLE src_events, DESCRIPTOR(event_time), INTERVAL '1' MINUTE)
) e
LEFT JOIN dim_customer FOR SYSTEM_TIME AS OF e.proc_time AS c
    ON e.customer_id = c.customer_id
GROUP BY window_start, biz_type, e.customer_id;
```

#### `job.properties` 示例（参数化）

```properties
kafka.bootstrap=node-1:9092,node-2:9092,node-3:9092
mysql.url=jdbc:mysql://node-1:3306/orcp_detail?useSSL=false&serverTimezone=Asia/Shanghai
mysql.user=orcp_ro
mysql.password=***
ob.url=jdbc:oceanbase://ob-host:2883/orcp_dw?useUnicode=true&characterEncoding=utf8&serverTimezone=Asia/Shanghai
ob.user=orcp_rw@tenant#cluster
ob.password=***
```

> 生产环境敏感参数由 Nacos 或 `--conf` 覆盖，**不要**把真实密码提交到仓库。

### 6.4 orcp-admin（最小管控服务）

- [ ] REST：
  - `POST /api/jobs/submit`：封装 Flink REST `POST /jars/:jarid/run`
  - `POST /api/jobs/{jobId}/cancel`
  - `POST /api/jobs/{jobId}/savepoint`
  - `POST /api/jobs/restore`
  - `GET  /api/health`：聚合 Kafka / Flink / MySQL / OceanBase 可用性
- [ ] 依赖：`spring-boot-starter-web`、`spring-boot-starter-actuator`、`spring-cloud-starter-openfeign`、`spring-cloud-starter-alibaba-nacos-discovery/config`、`spring-cloud-starter-bootstrap`、`micrometer-registry-prometheus`、`mysql-connector-java`、`oceanbase-client`、`spring-kafka`
- [ ] 安全：最小 Basic Auth，账号由 Nacos 下发

---

## 7. 数据库 DDL 契约

### 7.1 本地明细库 `orcp_detail`（MySQL 8.0）

文件：`docs/SQL/mysql_detail_schema.sql`

- [ ] `t_customer (customer_id BIGINT PK, name, level, region, updated_at DATETIME)`
- [ ] `t_order (order_id BIGINT PK, customer_id, biz_type, amount DECIMAL(18,4), status, created_at, updated_at, KEY(customer_id), KEY(biz_type, created_at))`
- [ ] `t_order_item (id BIGINT PK AUTO_INCREMENT, order_id, sku_id, qty, price, KEY(order_id))`
- [ ] `t_dedup (event_id VARCHAR(64) PK, created_at)`（TTL 7 天，定时清理）
- [ ] 字符集统一 `utf8mb4 / utf8mb4_general_ci`

### 7.2 OceanBase 结果库 `orcp_dw`（OB 3.2.3, MySQL 5.7 兼容语法）

文件：`docs/SQL/oceanbase_dw_schema.sql`

```sql
-- OceanBase 3.2.3 / MySQL 模式 / 5.7 兼容语法
CREATE TABLE IF NOT EXISTS agg_order_1min (
    window_start  DATETIME        NOT NULL,
    biz_type      VARCHAR(32)     NOT NULL,
    customer_id   BIGINT          NOT NULL,
    order_cnt     BIGINT          NOT NULL DEFAULT 0,
    amount_sum    DECIMAL(18,4)   NOT NULL DEFAULT 0,
    update_time   TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP
                                    ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (window_start, biz_type, customer_id)
) DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS agg_order_daily (
    stat_date     DATE            NOT NULL,
    biz_type      VARCHAR(32)     NOT NULL,
    order_cnt     BIGINT          NOT NULL DEFAULT 0,
    amount_sum    DECIMAL(18,4)   NOT NULL DEFAULT 0,
    update_time   TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP
                                    ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (stat_date, biz_type)
) DEFAULT CHARSET=utf8mb4;
```

> **避免以下 OB 3.2.3 不支持或语义差异的语法**：
> - MySQL 8.0 的 CHECK 约束、`INVISIBLE` 索引、`FUNCTIONAL INDEX`
> - `utf8mb4_0900_ai_ci` 排序规则（改用 `utf8mb4_general_ci`）
> - JSON 新函数（如 `JSON_TABLE`）—— 本项目未使用
> - 窗口函数 OVER 中的 `IGNORE NULLS`（本项目未使用）

---

## 8. 生产辅助能力（最小集）

### 8.1 配置管理

- [ ] Nacos 命名空间：`dev` / `prod`
- [ ] DataId 约定：`orcp-ingest.yaml`、`orcp-admin.yaml`、`orcp-flink-job.properties`
- [ ] 敏感配置使用 **Jasypt 1.9.3**（JDK 8 兼容）加密存储于 Nacos

### 8.2 服务注册与发现

- [ ] Nacos `server-addr=node-1:8848`
- [ ] orcp-admin 通过 Nacos 发现 orcp-ingest；对 Flink 集群使用固定 REST 地址

### 8.3 健康检查

- [ ] 所有 Spring 服务暴露 `/actuator/health,info,metrics,prometheus`
- [ ] orcp-admin `/api/health` 聚合：
  - Kafka：`AdminClient.describeCluster()` 超时 2 s
  - Flink：`GET http://node-1:8081/overview`
  - MySQL：`SELECT 1`
  - OceanBase：`SELECT 1`（使用 `com.oceanbase.jdbc.Driver`）
- [ ] systemd 单元：`Restart=on-failure` / `RestartSec=10`

### 8.4 指标与监控

- [ ] Flink Prometheus reporter 见 §5.5
- [ ] Spring 服务：`micrometer-registry-prometheus` 暴露 `/actuator/prometheus`
- [ ] Prometheus scrape：
  - `flink` → `node-{1,2,3}:9250-9260`
  - `orcp-spring` → `orcp-ingest:8080/actuator/prometheus`、`orcp-admin:8080/actuator/prometheus`
- [ ] Grafana 面板：Flink 官方（ID 14911）+ Spring Boot（ID 10280）+ 自研端到端延迟面板

### 8.5 日志

- [ ] Logback JSON：`ts,level,logger,msg,traceId,spanId`
- [ ] 写到 `/var/log/orcp/{app}.log`，按天滚动保留 14 天
- [ ] Flink 日志：`log4j.properties` 按作业 ID 分文件

### 8.6 告警

- [ ] Prometheus Alertmanager → 钉钉/飞书 Webhook
- [ ] 规则：
  - `flink_jobmanager_job_uptime == 0` 持续 2 min
  - `flink_jobmanager_job_numRestarts_total` 5 min 增量 > 3
  - Kafka consumer lag > 10000 持续 5 min
  - `up{job="orcp-spring"} == 0` 持续 1 min
  - OceanBase 写入失败率（自定义 micrometer 计数器）> 1%

### 8.7 作业生命周期脚本

- [ ] `scripts/submit_job.sh`：上传 jar → 调用 `POST /jars/:id/run --allowNonRestoredState false`
- [ ] `scripts/savepoint.sh`：触发 savepoint 并打印路径
- [ ] `scripts/cancel_job.sh`：带 savepoint 优雅取消
- [ ] `scripts/restore_from_savepoint.sh`：携带 `savepointPath` 重新运行
- [ ] 所有脚本读取 `scripts/env.sh`

### 8.8 交付物验收清单

- [ ] 端到端自测：向外部 Kafka 发 1000 条消息 → 本地 MySQL 明细可见 → OceanBase 聚合表在 1 分钟内出结果
- [ ] 杀掉任一 TaskManager，作业 2 分钟内自愈，无丢失 / 无重复
- [ ] 重启 orcp-ingest 进程，无重复消费
- [ ] Grafana 可见作业 QPS、延迟、lag、失败率
- [ ] 告警：手动停掉 JobManager，1 条告警通过 Webhook 到达群

---

## 9. 开发与部署流程（Makefile）

- [ ] `make build`：`mvn -T 1C clean package -DskipTests`
- [ ] `make deploy-ingest` / `make deploy-admin`：scp jar + systemd restart
- [ ] `make submit-flink` / `make cancel JOB=xxx` / `make savepoint JOB=xxx`
- [ ] `make status`：打印各节点 systemd 状态

---

## 10. Claude Code 执行顺序（推荐）

1. **阶段 A：仓库骨架**
   - [ ] 按 §4.1 建目录与空 pom/占位 README
   - [ ] 根 pom 锁定 §2 与 §4.2 所列版本，子模块继承
   - [ ] PR：`feat: init repo skeleton (jdk8 baseline)`

2. **阶段 B：环境脚本**
   - [ ] 实现 `deploy/centos/*.sh`（含 ZooKeeper、Kafka ZK 模式）与 `deploy/systemd/*.service`
   - [ ] 实现 `deploy/flink-conf/*`
   - [ ] 在 1 台 CentOS 跑通全脚本
   - [ ] PR：`feat: centos bootstrap scripts (zk + kafka 3.5.2 + flink 1.17.2)`

3. **阶段 C：DDL 与测试数据**
   - [ ] 写 `docs/SQL/*.sql` 并执行
   - [ ] 发压脚本向外部 Kafka 发测试数据
   - [ ] PR：`feat: schemas and test data generator`

4. **阶段 D：orcp-ingest**
   - [ ] 按 §6.2 实现（Spring Boot 2.7 + Nacos + MyBatis-Plus）
   - [ ] PR：`feat: orcp-ingest kafka to mysql pipeline`

5. **阶段 E：orcp-flink-job**
   - [ ] 按 §6.3 实现 SQL + Java 入口 + shade 打包
   - [ ] 在 Flink UI 看到 RUNNING，OceanBase 出结果
   - [ ] PR：`feat: flink sql job with oceanbase 3.2.3 sink`

6. **阶段 F：orcp-admin + 监控告警**
   - [ ] 按 §6.4、§8.4、§8.6 实现
   - [ ] PR：`feat: admin service and observability stack`

7. **阶段 G：验收**
   - [ ] 跑完 §8.8 清单
   - [ ] 补充 `docs/OPS_RUNBOOK.md`
   - [ ] PR：`docs: ops runbook and acceptance report`

---

## 11. 风险与约束

- **JDK 8 生命周期**：Oracle 已停止公共更新，推荐使用 Eclipse Temurin 8（Adoptium）持续获取安全补丁；Flink 1.17/Spring Boot 2.7 均为"JDK 8 最后一个官方支持 LTS"，建议中期规划升级到 JDK 17 + Flink 1.20 + Spring Boot 3.x。
- **OceanBase 3.2.3 兼容性**：
  - 使用 **OceanBase Connector/J 2.4.7**（`com.oceanbase.jdbc.Driver`），兼容性优于 MySQL 驱动；
  - DDL 语法锁定在 **MySQL 5.7 兼容子集**；
  - 写入全部使用 Flink JDBC 的 upsert 语义（由 connector 基于主键拆分 INSERT/UPDATE），**不依赖** `INSERT ON DUPLICATE KEY UPDATE`；
  - 连接串 user 字段格式：直连用 `user@tenant#cluster`，走 OBProxy 用 `user@tenant`。
- **Kafka ZooKeeper 模式**：官方虽已推 KRaft，但本项目按保守生产原则采用 ZK 模式，与 Kafka 3.5.2 搭配；未来迁移 KRaft 需先升级到 Kafka 3.6/3.7 的 dual-write 模式。
- **Exactly-Once**：Kafka Source + Flink Checkpoint + JDBC Sink（主键 upsert）在当前 DDL 下保证**幂等写入**，等价端到端一次语义。严格 XA 不在本期。
- **Kafka 事务**：orcp-ingest 到 `orcp.mid.events` 的转发用**幂等生产者 + 去重表**而非 Kafka 事务，降低复杂度。
- **网络**：CentOS 默认防火墙必须放通 §5.8 端口，或直接关闭 firewalld（内网建议）。
- **容量**：单 4C8G 节点约可支撑 5k~10k msg/s 的端到端链路；水平扩 TaskManager + Kafka 分区。

---

## 12. 附录 A：最小端到端冒烟命令

```bash
# 1. 发送测试消息
kafka-console-producer.sh --bootstrap-server node-1:9092 --topic orcp.src.demo <<'EOF'
{"eventId":"e-1","bizType":"ORDER","bizKey":"o-1","customerId":100,"amount":99.50,"eventTime":"2026-05-09 10:00:00"}
EOF

# 2. 查看 orcp-ingest 日志
sudo journalctl -u orcp-ingest -f

# 3. 查看 Flink 作业状态
curl -s http://node-1:8081/jobs/overview | jq

# 4. 查看 OceanBase 聚合结果（使用 obclient）
obclient -h${OB_HOST} -P${OB_PORT} -u"orcp_rw@tenant#cluster" -p${OB_PWD} orcp_dw \
  -e "SELECT * FROM agg_order_1min ORDER BY window_start DESC LIMIT 10"
```

---

## 13. 附录 B：JDK 8 语法与 API 禁区

为避免后续升级时引入的静默兼容问题，**所有 Java 代码不得使用**：

- `var` 局部变量推断（JDK 10+）
- `record` 类（JDK 14+）
- `sealed` / `non-sealed`（JDK 17）
- 文本块 `"""..."""`（JDK 13+）
- Switch 表达式 `case ... ->`（JDK 14+）
- `Optional.stream()`（JDK 9+）
- `Files.readString` / `Files.writeString`（JDK 11+）
- `List.of` / `Map.of` 不可变集合工厂（JDK 9+，可用 `Collections.unmodifiableList(Arrays.asList(...))` 替代）
- `String.isBlank` / `String.strip` / `String.repeat`（JDK 11+）
- HttpClient（JDK 11）—— 使用 OkHttp 4.12.x（JDK 8 兼容）或 Spring `RestTemplate`

**允许使用**：Lambda、Stream、`Optional`、`CompletableFuture`、`java.time.*`、`java.util.function.*`。

---

**文档版本**：v1.1（JDK 8 / OceanBase 3.2.3）
**维护者**：ORCP Team
**最后更新**：2026-05-09
