package com.orcp.common;

/**
 * Global constants for topic names, table names and configuration keys.
 *
 * <p>All modules MUST reference constants from this class instead of
 * hard-coding strings, so that naming contracts stay in a single place.
 */
public final class OrcpConstants {

    private OrcpConstants() {
    }

    // ---------------------------------------------------------------
    // Kafka topics
    // ---------------------------------------------------------------

    /** External source topic prefix. Concrete topic is configured per deployment. */
    public static final String SRC_TOPIC_PREFIX = "orcp.src.";

    /** Internal topic consumed by the Flink job. */
    public static final String MID_TOPIC_EVENTS = "orcp.mid.events";

    /** Default consumer group ids. */
    public static final String GROUP_INGEST = "orcp-ingest";
    public static final String GROUP_FLINK = "orcp-flink-job";

    // ---------------------------------------------------------------
    // MySQL (orcp_detail) table names
    // ---------------------------------------------------------------
    public static final String TABLE_CUSTOMER = "t_customer";
    public static final String TABLE_ORDER = "t_order";
    public static final String TABLE_ORDER_ITEM = "t_order_item";
    public static final String TABLE_DEDUP = "t_dedup";

    // ---------------------------------------------------------------
    // OceanBase (orcp_dw) table names
    // ---------------------------------------------------------------
    public static final String TABLE_AGG_ORDER_1MIN = "agg_order_1min";
    public static final String TABLE_AGG_ORDER_DAILY = "agg_order_daily";

    // ---------------------------------------------------------------
    // Biz type enum values (string form, used across services)
    // ---------------------------------------------------------------
    public static final String BIZ_TYPE_ORDER = "ORDER";
    public static final String BIZ_TYPE_REFUND = "REFUND";
}
