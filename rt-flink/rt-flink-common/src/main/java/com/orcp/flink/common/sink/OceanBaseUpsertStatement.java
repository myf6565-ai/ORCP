package com.orcp.flink.common.sink;

public final class OceanBaseUpsertStatement {
    private OceanBaseUpsertStatement() {
    }

    public static String odsUpsertSql(String table) {
        return "INSERT INTO " + table + " (event_id,event_time,biz_key,schema_version,trace_id,amount,version) VALUES (?,?,?,?,?,?,?) " +
                "ON DUPLICATE KEY UPDATE event_time=VALUES(event_time),schema_version=VALUES(schema_version),trace_id=VALUES(trace_id),amount=VALUES(amount),version=VALUES(version)";
    }
}
