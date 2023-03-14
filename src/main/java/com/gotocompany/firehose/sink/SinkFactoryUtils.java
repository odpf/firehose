package com.gotocompany.firehose.sink;

import com.gotocompany.depot.message.SinkConnectorSchemaMessageMode;

import java.util.HashMap;
import java.util.Map;

public class SinkFactoryUtils {
    protected static Map<String, String> addAdditionalConfigsForSinkConnectors(Map<String, String> env) {
        Map<String, String> finalConfig = new HashMap<>(env);
        finalConfig.put("SINK_CONNECTOR_SCHEMA_PROTO_MESSAGE_CLASS", env.getOrDefault("INPUT_SCHEMA_PROTO_CLASS", ""));
        finalConfig.put("SINK_CONNECTOR_SCHEMA_PROTO_KEY_CLASS", env.getOrDefault("INPUT_SCHEMA_PROTO_CLASS", ""));
        finalConfig.put("SINK_CONNECTOR_SCHEMA_DATA_TYPE", env.getOrDefault("INPUT_SCHEMA_DATA_TYPE", "protobuf"));
        finalConfig.put("SINK_METRICS_APPLICATION_PREFIX", "firehose_");
        finalConfig.put("SINK_CONNECTOR_SCHEMA_PROTO_ALLOW_UNKNOWN_FIELDS_ENABLE", env.getOrDefault("INPUT_SCHEMA_PROTO_ALLOW_UNKNOWN_FIELDS_ENABLE", "false"));
        finalConfig.put("SINK_CONNECTOR_SCHEMA_MESSAGE_MODE",
                env.getOrDefault("KAFKA_RECORD_PARSER_MODE", "").equals("key") ? SinkConnectorSchemaMessageMode.LOG_KEY.name() : SinkConnectorSchemaMessageMode.LOG_MESSAGE.name());
        return finalConfig;
    }
}
