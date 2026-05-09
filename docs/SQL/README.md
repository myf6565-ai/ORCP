# SQL 建表脚本

本目录包含流水线所涉及两个数据库的参考 DDL。两个文件均具备**幂等性**——对已有数据的实例重复执行是安全的。

| 文件 | 目标库 | 执行位置 | 说明 |
|------|--------|----------|------|
| `mysql_detail_schema.sql` | node-1 上的 MySQL 8.0 | node-1 shell | 创建 `orcp_detail` 库及 `orcp_rw` / `orcp_ro` 账号；预置 50 条客户种子数据。 |
| `oceanbase_dw_schema.sql` | OceanBase 3.2.3（MySQL 模式） | 任意装有 `obclient` 的主机 | 创建 `orcp_dw` 库及两张聚合表，**不**创建用户。 |

## MySQL（node-1）

```bash
# 以 MySQL root 账号执行，通常在 deploy/centos/40_install_mysql.sh 完成后首次运行。
mysql -uroot -p < docs/SQL/mysql_detail_schema.sql

# 快速验证
mysql -uorcp_ro -p'ChangeMe_ro_1!' -e "SELECT COUNT(*) FROM orcp_detail.t_customer"   # 期望 50
```

**在将实例暴露到 localhost 之外前，请立即修改默认密码**（`ChangeMe_rw_1!` / `ChangeMe_ro_1!`），真实密码应存放在 Nacos 中。

## OceanBase（独立集群）

租户用户必须已存在，并对 `orcp_dw` 库拥有 `CREATE`/`SELECT`/`INSERT`/`UPDATE` 权限。如不确定，请咨询 OB DBA。

```bash
# 直连（端口 2881）
obclient -h${OB_HOST} -P2881 -u"orcp_rw@tenant#cluster" -p${OB_PWD} < docs/SQL/oceanbase_dw_schema.sql

# 经 OBProxy 连接（端口 2883）——用户名格式为 user@tenant，不带 #cluster
obclient -h${OBPROXY_HOST} -P2883 -u"orcp_rw@tenant" -p${OB_PWD} < docs/SQL/oceanbase_dw_schema.sql

# 快速验证
obclient -h${OB_HOST} -P2881 -u"orcp_rw@tenant#cluster" -p${OB_PWD} orcp_dw -e "SHOW TABLES"
```

## 与流水线其余部分的关系

```
orcp-ingest  ---INSERT--->  orcp_detail.t_order / t_order_item / t_dedup
                                              ^
                                              |
                                              |  （JDBC lookup 维表关联）
                                              |
orcp.mid.events  ---Flink SQL---> agg_order_1min  （JDBC sink UPSERT）
                                              ^
                                              |  位于 OceanBase orcp_dw
```

- 阶段 D（`orcp-ingest`）将明细行和去重记录写入 MySQL。
- 阶段 E（`orcp-flink-job`）从 Kafka 读取 `orcp.mid.events`，通过 JDBC lookup 关联 `t_customer`，将结果 upsert 到 `agg_order_1min`。
