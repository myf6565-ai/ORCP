# ORCP 架构图（逻辑 / 物理 / 数据流向）

> 本文基于仓库当前实现（`orcp-ingest`、`orcp-flink-job`、`orcp-admin` 以及 `deploy` 配置）整理，便于设计评审与运维交接。

## 1) 逻辑架构图（Logical Architecture）

```mermaid
flowchart LR
    subgraph Ext[外部输入域]
      SRC[External Kafka\norcp.src.*]
    end

    subgraph Ingest[摄入层：orcp-ingest]
      KLISTENER[Kafka Listener\nSourceEventListener]
      DEDUP[DedupService\n去重]
      DETAIL[DetailWriteService\n写 MySQL 明细]
      FWD[ForwardService\n转发中间事件]
      KLISTENER --> DEDUP --> DETAIL --> FWD
    end

    subgraph Compute[实时计算层：orcp-flink-job]
      FSRC[Flink SQL Source\nKafka: orcp.mid.events]
      FDIM[维表 Lookup\nMySQL orcp_detail.*]
      FSQL[JOIN + 窗口聚合\n10_pipeline.sql]
      FSINK[Flink JDBC Sink\nOceanBase orcp_dw.*]
      FSRC --> FSQL
      FDIM --> FSQL
      FSQL --> FSINK
    end

    subgraph Ops[管控与可观测层]
      ADMIN[orcp-admin\nJobController / HealthController]
      NACOS[Nacos\n注册&配置中心]
      PROM[Prometheus]
      GRAFANA[Grafana]
      ADMIN --- NACOS
      PROM --> GRAFANA
    end

    subgraph Storage[数据存储域]
      MYSQL[(MySQL\norcp_detail)]
      OB[(OceanBase\norcp_dw)]
    end

    SRC --> KLISTENER
    DETAIL --> MYSQL
    FWD --> FSRC
    FDIM -. JDBC Lookup .-> MYSQL
    FSINK --> OB

    ADMIN -. Flink REST .-> Compute
    PROM -. 指标采集 .-> Ingest
    PROM -. 指标采集 .-> Compute
    PROM -. 指标采集 .-> ADMIN
```

---

## 2) 物理架构图（Physical Deployment）

```mermaid
flowchart TB
    subgraph NODE1[node-1]
      ZK1[ZooKeeper]
      K1[Kafka Broker]
      JM[Flink JobManager]
      NACOS[Nacos]
      MYSQL[(MySQL)]
    end

    subgraph NODE2[node-2]
      ZK2[ZooKeeper]
      K2[Kafka Broker]
      TM2[Flink TaskManager]
      PROM[Prometheus]
      GRAFANA[Grafana]
    end

    subgraph NODE3[node-3]
      ZK3[ZooKeeper]
      K3[Kafka Broker]
      TM3[Flink TaskManager]
      INGEST[orcp-ingest]
      ADMIN[orcp-admin]
    end

    subgraph OBCLUSTER[OceanBase 3.2.3 集群/托管]
      OB[(orcp_dw)]
    end

    ZK1 --- ZK2
    ZK2 --- ZK3

    K1 --- K2
    K2 --- K3

    JM --- TM2
    JM --- TM3

    INGEST --> K3
    TM2 --> OB
    TM3 --> OB

    ADMIN -. REST .-> JM
    PROM --> GRAFANA
    PROM -. scrape .-> INGEST
    PROM -. scrape .-> ADMIN
    PROM -. scrape .-> JM
    PROM -. scrape .-> TM2
    PROM -. scrape .-> TM3
```

---

## 3) 数据流向图（Data Flow）

```mermaid
sequenceDiagram
    participant Producer as 外部生产者
    participant KSrc as Kafka(orcp.src.*)
    participant Ingest as orcp-ingest
    participant MySQL as MySQL(orcp_detail)
    participant KMid as Kafka(orcp.mid.events)
    participant Flink as orcp-flink-job
    participant OB as OceanBase(orcp_dw)
    participant Admin as orcp-admin

    Producer->>KSrc: 写入原始事件
    KSrc->>Ingest: 消费 SourceEvent
    Ingest->>Ingest: 校验 / 去重
    Ingest->>MySQL: 写 detail 事实明细
    Ingest->>KMid: 转发标准化中间事件

    KMid->>Flink: Source 读取流事件
    Flink->>MySQL: JDBC Lookup 维表查询
    Flink->>Flink: JOIN + 窗口聚合
    Flink->>OB: JDBC Sink Upsert 聚合结果

    Admin->>Flink: 提交/取消/Savepoint(经 Flink REST)
```

## 4) 使用建议

- 如需在 Git 平台渲染，优先使用支持 Mermaid 的 Markdown 查看器。
- 如果需要导出 PNG/SVG，可用 `mmdc`（Mermaid CLI）进行离线渲染。
- 图中节点名称已与仓库模块及脚本命名保持一致，便于追溯到实现。
