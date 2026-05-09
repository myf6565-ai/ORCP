# ORCP

**在线实时计算平台** —— 基于 **Spring Cloud + Apache Kafka + Apache Flink + OceanBase**（MySQL 模式）构建的最小可生产、可落地实时计算系统。

## 当前状态

阶段 A–G 全部完成。最小可行生产切片已就绪：
基础设施引导脚本、数据库建表、orcp-ingest、orcp-flink-job、orcp-admin、
可观测性栈，以及 §8.8 验收测试套件。

## 技术栈（JDK 8 生产约束锁定）

- JDK：Temurin **8u402**
- Spring Boot **2.7.18** + Spring Cloud **2021.0.9** + Spring Cloud Alibaba **2021.0.5.0**
- Apache Kafka **3.5.2**（ZooKeeper 模式）+ Apache ZooKeeper **3.7.2**
- Apache Flink **1.17.2**（standalone，systemd 托管）
- OceanBase **3.2.3**（MySQL 模式），通过 `oceanbase-client` **2.4.14** 接入
- MySQL 8.0（本地明细 / 维表来源），MyBatis-Plus **3.5.5**
- Nacos **2.2.3** 作为注册中心与配置中心
- Prometheus + Grafana 负责可观测性

## 仓库目录

```
orcp-common/       共享 DTO、常量、工具类（纯库 jar）
orcp-ingest/       Spring Boot 服务：Kafka -> MySQL 明细 + 转发 topic
orcp-flink-job/    Flink 1.17.2 作业 jar（Kafka + MySQL 维表 + OceanBase sink）
orcp-admin/        最小管控服务（提交/取消 Flink 作业 + 聚合健康检查）
deploy/            CentOS 引导脚本、systemd 单元文件、flink-conf
docs/              DEV_SPEC.md、OPS_RUNBOOK.md、SQL 建表脚本
scripts/           作业生命周期辅助脚本
Makefile           统一构建与部署入口
```

## 快速开始（开发者）

```bash
# 前置条件：JDK 8、Maven 3.8.x
java -version         # 必须输出 1.8.x
mvn -version

# 构建所有模块
make build

# 本地启动摄入服务
java -jar orcp-ingest/target/orcp-ingest.jar

# 生产测试事件（需先安装 kafka-python）
pip install -r scripts/requirements.txt
make gen-events ARGS='--count 100 --rate 50'
```

## 部署

完整安装说明见 [`docs/OPS_RUNBOOK.md`](./docs/OPS_RUNBOOK.md)。简要流程：

```bash
# 每个节点：基础引导 + JDK 8 + ZooKeeper + Kafka + Flink
sudo bash deploy/centos/00_bootstrap.sh
sudo bash deploy/centos/10_install_jdk8.sh
sudo bash deploy/centos/15_install_zookeeper.sh
sudo bash deploy/centos/20_install_kafka_zk.sh
sudo bash deploy/centos/30_install_flink.sh

# 仅 node-1：MySQL + Nacos
sudo bash deploy/centos/40_install_mysql.sh
sudo bash deploy/centos/50_install_nacos.sh

# 仅 node-2：Prometheus + Grafana
sudo bash deploy/centos/60_install_prom_grafana.sh

# 在堡垒机：构建、部署 Spring 服务、提交 Flink 作业
make build
make deploy-systemd deploy-ingest deploy-admin
make submit-flink
```

## 验收测试

`scripts/acceptance_test.sh` 对运行中的集群执行 DEV_SPEC §8.8 验收清单，
并按门输出 PASS/FAIL。结果填入 [`docs/ACCEPTANCE_REPORT.md`](./docs/ACCEPTANCE_REPORT.md)。

```bash
# 安全（非破坏性）门：
bash scripts/acceptance_test.sh

# 完整套件（含 TaskManager 杀死和 JobManager 停机）：
TM_HOST=node-3 JM_HOST=node-1 INGEST_HOST=node-3 \
    bash scripts/acceptance_test.sh --all
```

## 文档索引

| 文件 | 用途 |
|------|------|
| `docs/DEV_SPEC.md` | 架构设计、版本矩阵、分阶段交付计划 |
| `docs/OPS_RUNBOOK.md` | 首次安装 + 日常运维 + 故障排查 |
| `docs/ACCEPTANCE_REPORT.md` | §8.8 证据记录模板（每次运行复制一份） |
| `docs/SQL/README.md` | 数据库建表脚本加载说明 |
| `deploy/grafana/README.md` | Grafana 看板导入指南 |

## 贡献指南

1. 从 `main` 基于 `feat/*`、`fix/*`、`docs/*`、`chore/*` 创建分支。
2. 每个 PR 应对应 `docs/DEV_SPEC.md` 的某个阶段章节。
3. 所有 Java 代码必须使用 JDK 8 source/target；禁止使用的 API/语法见 DEV_SPEC 附录 B。
