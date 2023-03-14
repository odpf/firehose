package com.gotocompany.firehose.sink.bigquery;

import java.util.Map;
import java.util.function.Function;

public class BigquerySinkUtils {
    public static Function<Map<String, Object>, String> getRowIDCreator() {
        return (m -> String.format("%s_%d_%d", m.get("message_topic"), m.get("message_partition"), m.get("message_offset")));
    }

    public static void addMetadataColumns(Map<String, String> config) {
        config.put("SINK_BIGQUERY_METADATA_COLUMNS_TYPES",
                "message_offset=integer,message_topic=string,load_time=timestamp,message_timestamp=timestamp,message_partition=integer");
    }
}
