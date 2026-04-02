# realtime-compute-platform

基于 **Spring Boot + Spring Cloud + Kafka + Flink + OceanBase(MySQL 模式)** 的实时计算系统 v1。

## 模块职责
- `rt-common`：通用常量（日志字段等）。
- `rt-api`：控制面 API DTO。
- `rt-gateway`：统一网关，仅路由控制面 API。
- `rt-compute-admin`：作业提交/启停/savepoint/状态/回放接口。
- `rt-flink/rt-flink-common`：Flink 通用模型、JSON 解析、OceanBase upsert SQL。
- `rt-flink/rt-flink-ods-sync-job`：Kafka 消费、清洗、校验、去重、ODS upsert、死信分流。
- `rt-flink/rt-flink-dws-sql-job`：执行 Flink SQL（temporal join + tumble window 聚合）并写入 DWS。
- `rt-sql/oceanbase`：OceanBase 管理库/ODS/DWS DDL。
- `rt-sql/flink`：Flink SQL 脚本。
- `deploy/centos`：CentOS 安装与 systemd 示例。
- `docs`：架构说明与开发说明。

## 本地启动
1. 启动依赖：
   ```bash
   docker compose up -d
   ```
2. 初始化 OceanBase（按需连接后执行）：
   ```bash
   mysql -h127.0.0.1 -P2881 -uroot@test < rt-sql/oceanbase/001_schema.sql
   ```
3. 构建：
   ```bash
   mvn clean package
   ```
4. 启动控制面：
   ```bash
   mvn -pl rt-compute-admin spring-boot:run
   mvn -pl rt-gateway spring-boot:run
   ```

## CentOS 部署
1. 安装基础组件：
   ```bash
   deploy/centos/install-jdk.sh
   deploy/centos/install-zookeeper.sh
   deploy/centos/install-kafka.sh
   deploy/centos/install-flink.sh
   ```
2. 按 `deploy/centos/flink-conf.yaml.tpl` 渲染 Flink 配置。
3. 使用 `flink-jobmanager.service`、`flink-taskmanager.service` 注册并启动 Flink。

## 作业提交流程
1. 生成 Job Jar：`rt-flink-ods-sync-job`、`rt-flink-dws-sql-job`。
2. 调用管理接口：
   - `POST /jobs/submit`
   - `POST /jobs/{jobId}/stop`
   - `POST /jobs/{jobId}/savepoint`
   - `GET /jobs/{jobId}`
   - `POST /replay`

示例：
```bash
curl -X POST localhost:8081/jobs/submit -H 'Content-Type: application/json' -d '{"jobJar":"/opt/jobs/rt-flink-ods-sync-job.jar","jobName":"rt-flink-ods-sync-job","args":["--topic","rt.biz.events"]}'
```

## 结果验证
1. 写入 Kafka 测试消息（包含 event_id/event_time/biz_key/schema_version/trace_id）。
2. 观察 ODS 表 `rt_ods.ods_event_detail` upsert 结果。
3. 执行 DWS SQL 作业后，检查 `rt_dws.dws_biz_tumble_1m`。
4. 发送坏消息（缺少必填字段）后，验证进入 `rt.biz.events.dlq`。

## 失败重试、死信、回放
- 重试：ODS JDBC sink 配置 `maxRetries=3`。
- 死信：校验失败事件输出至 dead-letter topic。
- 回放：调用 `POST /replay` 指定 topic 与 offset 区间（fromOffset/toOffset）。

## 关键约束落实
- Flink Job 独立 Jar，可独立提交。
- sink 使用幂等 upsert（`INSERT ... ON DUPLICATE KEY UPDATE`）。
- SQL sink 均有 `PRIMARY KEY NOT ENFORCED`。
- 配置通过环境变量外部化。
- 启用 checkpoint，支持 savepoint（控制面接口 + Flink 配置）。
