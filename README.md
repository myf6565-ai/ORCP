# 流计算系统 MVP（ORCP）

基于 **Spring Boot + Kafka + Flink + Docker Compose + Kubernetes** 的最小可用流计算平台。

> 目标：交付可编译、可启动、可验证、可部署的 MVP 工程。

## 1. 项目简介

本项目实现最小闭环：

```text
外部请求
  -> ingest-service
  -> Kafka raw-events
  -> event-cleaning-job
  -> Kafka standard-events
  -> rule-aggregation-job
  -> Kafka result-events
  -> query-service 查询结果
```

## 2. 模块说明

```text
/
  pom.xml
  /docs
  /services
    /gateway-service
    /ingest-service
    /config-service
    /query-service
    /ops-service
  /jobs
    /event-cleaning-job
    /rule-aggregation-job
  /shared
    /common-model
    /common-kafka
    /common-web
  /infra
    /docker-compose
    /k8s
    /scripts
  /examples
    /requests
    /events
```

- `gateway-service`：统一入口占位、健康检查。
- `ingest-service`：`POST /api/events` 写入 `raw-events`。
- `config-service`：规则与 Topic 映射配置 API（内存实现）。
- `query-service`：`GET /api/results/{key}` 查询结果。
- `ops-service`：作业状态、审计占位接口。
- `event-cleaning-job`：`raw-events -> standard-events`。
- `rule-aggregation-job`：`standard-events -> result-events`。

## 3. 技术栈

- Java 17
- Maven
- Spring Boot 3
- Kafka
- Flink 1.19
- Docker Compose
- Kubernetes

## 4. 本地启动步骤

### 4.1 构建

```bash
mvn clean install
```

### 4.2 启动 Kafka 依赖

```bash
docker compose -f infra/docker-compose/docker-compose.yml up -d
```

### 4.3 初始化 Topic

```bash
sh infra/scripts/init-topics.sh
```

### 4.4 启动服务（示例）

```bash
mvn -pl services/ingest-service spring-boot:run
mvn -pl services/query-service spring-boot:run
mvn -pl services/config-service spring-boot:run
```

可选：

```bash
mvn -pl services/gateway-service spring-boot:run
mvn -pl services/ops-service spring-boot:run
```

### 4.5 打包 Flink 作业

```bash
mvn -pl jobs/event-cleaning-job clean package
mvn -pl jobs/rule-aggregation-job clean package
```

## 5. 测试步骤

```bash
mvn test
```

## 6. Docker Compose 说明

- 启动：`docker compose -f infra/docker-compose/docker-compose.yml up -d`
- 停止：`docker compose -f infra/docker-compose/docker-compose.yml down`

## 7. Kubernetes 部署说明

```bash
kubectl apply -f infra/k8s/
```

包括：
- Spring Boot 服务 Deployment/Service
- ConfigMap/Secret 示例
- Flink 作业 Job 示例

## 8. 演示请求示例

### 8.1 接入事件

```bash
sh examples/requests/ingest-event.sh
```

### 8.2 查询结果

```bash
sh examples/requests/query-result.sh
```

## 9. 文档索引

- `docs/architecture.md`
- `docs/mvp-scope.md`
- `docs/topic-design.md`
- `docs/assumptions.md`

## 10. 当前版本边界

### 当前已实现

- Maven 多模块工程骨架与公共模块。
- 5 个 Spring Boot 服务最小 API。
- 2 个 Flink 作业（清洗+聚合）代码与测试。
- 本地 Docker Compose、K8s 清单、脚本与示例请求。

### 当前未实现

- 规则中心持久化。
- 结果存储外置（如 Redis / DB）。
- 完整的重放与死信处理链路。

### 已知风险

- Flink 聚合作业当前使用内存 Map 进行最小示例聚合。
- 跨服务与 Flink 的端到端集成测试需在完整 Kafka/Flink 环境执行。

### 下一阶段建议

- 引入规则持久化与动态热更新。
- 引入状态后端、checkpoint/savepoint 持久化。
- 完善可观测性（Prometheus / Trace）。
