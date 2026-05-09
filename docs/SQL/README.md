# SQL schemas

Reference DDL for the two databases the pipeline touches. Both files are
idempotent; re-running them against a populated instance is safe.

| File                          | Target                          | Run from                  | Notes                                                   |
|-------------------------------|---------------------------------|---------------------------|---------------------------------------------------------|
| `mysql_detail_schema.sql`     | MySQL 8.0 on node-1             | node-1 shell              | Creates `orcp_detail` DB + `orcp_rw` / `orcp_ro` users; seeds 50 customers. |
| `oceanbase_dw_schema.sql`     | OceanBase 3.2.3 (MySQL mode)    | any host with `obclient`  | Creates `orcp_dw` DB + two aggregate tables. Does NOT create users. |

## MySQL (node-1)

```bash
# Run as the MySQL root user, typically the first time after
# deploy/centos/40_install_mysql.sh has set up the server.
mysql -uroot -p < docs/SQL/mysql_detail_schema.sql

# Smoke check
mysql -uorcp_ro -p'ChangeMe_ro_1!' -e "SELECT COUNT(*) FROM orcp_detail.t_customer"   # expect 50
```

**Rotate the default passwords** (`ChangeMe_rw_1!` / `ChangeMe_ro_1!`) before
exposing the instance to anything outside localhost. The real values should
live in Nacos.

## OceanBase (independent cluster)

Tenant users must already exist and have `CREATE`/`SELECT`/`INSERT`/`UPDATE`
privileges on the `orcp_dw` database. Check with your OB DBA if unsure.

```bash
# Direct connection (port 2881)
obclient -h${OB_HOST} -P2881 -u"orcp_rw@tenant#cluster" -p${OB_PWD} < docs/SQL/oceanbase_dw_schema.sql

# OBProxy connection (port 2883) -- note the user format: user@tenant (no #cluster)
obclient -h${OBPROXY_HOST} -P2883 -u"orcp_rw@tenant" -p${OB_PWD} < docs/SQL/oceanbase_dw_schema.sql

# Smoke check
obclient -h${OB_HOST} -P2881 -u"orcp_rw@tenant#cluster" -p${OB_PWD} orcp_dw -e "SHOW TABLES"
```

## Relationship to the rest of the pipeline

```
orcp-ingest  ---INSERT--->  orcp_detail.t_order / t_order_item / t_dedup
                                              ^
                                              |
                                              |  (JDBC lookup)
                                              |
orcp.mid.events  ---Flink SQL---> agg_order_1min  (UPSERT via JDBC sink)
                                              ^
                                              |  lives in OceanBase orcp_dw
```

- Stage D (`orcp-ingest`) writes detail rows and the dedup ledger into MySQL.
- Stage E (`orcp-flink-job`) reads `orcp.mid.events` from Kafka, joins
  `t_customer` via the JDBC lookup, and upserts into `agg_order_1min`.
