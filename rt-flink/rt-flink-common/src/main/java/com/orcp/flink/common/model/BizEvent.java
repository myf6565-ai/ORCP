package com.orcp.flink.common.model;

import java.io.Serializable;

public class BizEvent implements Serializable {
    public String eventId;
    public String eventTime;
    public String bizKey;
    public Integer schemaVersion;
    public String traceId;
    public String version;
    public Double amount;

    public boolean isValid() {
        return eventId != null && !eventId.isBlank()
                && bizKey != null && !bizKey.isBlank()
                && eventTime != null && !eventTime.isBlank();
    }

    public String dedupKey() {
        return eventId != null ? eventId : (bizKey + "_" + version);
    }
}
