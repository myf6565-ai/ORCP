package com.orcp.common;

/**
 * 全局常量：topic 名称、表名和配置键。
 *
 * <p>所有模块 <strong>必须</strong> 从本类引用常量，而不是硬编码字符串，
 * 以确保命名约定的单一维护入口。
 */
public final class OrcpConstants {

    private OrcpConstants() {
    }

    // ---------------------------------------------------------------
    // Kafka topic
    // ---------------------------------------------------------------

    /** 外部源 topic 前缀，具体 topic 名称按部署配置。 */
    public static final String SRC_TOPIC_PREFIX = "orcp.src.";

    /** Flink 作业消费的内部 topic。 */
    public static final String MID_TOPIC_EVENTS = "orcp.mid.events";

    /** 默认消费者组 ID。 */
    public static final String GROUP_INGEST = "orcp-ingest";
    public static final String GROUP_FLINK = "orcp-flink-job";

    // ---------------------------------------------------------------
    // MySQL（orcp_detail）表名
    // ---------------------------------------------------------------
    public static final String TABLE_CUSTOMER   = "t_customer";
    public static final String TABLE_ORDER      = "t_order";
    public static final String TABLE_ORDER_ITEM = "t_order_item";
    public static final String TABLE_DEDUP      = "t_dedup";

    // ---------------------------------------------------------------
    // OceanBase（orcp_dw）表名
    // ---------------------------------------------------------------
    public static final String TABLE_AGG_ORDER_1MIN  = "agg_order_1min";
    public static final String TABLE_AGG_ORDER_DAILY = "agg_order_daily";

    // ---------------------------------------------------------------
    // 业务类型枚举值（字符串形式，跨服务共享）
    // ---------------------------------------------------------------
    public static final String BIZ_TYPE_ORDER  = "ORDER";
    public static final String BIZ_TYPE_REFUND = "REFUND";
}
