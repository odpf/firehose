package com.gotocompany.firehose.config;

import com.gotocompany.firehose.config.converter.ProtoIndexToFieldMapConverter;
import com.gotocompany.firehose.config.converter.RangeToHashMapConverter;

import java.util.Map;
import java.util.Properties;

public interface PromSinkConfig extends AppConfig {

    @Key("SINK_PROM_RETRY_STATUS_CODE_RANGES")
    @DefaultValue("400-600")
    @ConverterClass(RangeToHashMapConverter.class)
    Map<Integer, Boolean> getSinkPromRetryStatusCodeRanges();

    @Key("SINK_PROM_REQUEST_LOG_STATUS_CODE_RANGES")
    @DefaultValue("400-499")
    @ConverterClass(RangeToHashMapConverter.class)
    Map<Integer, Boolean> getSinkPromRequestLogStatusCodeRanges();

    @Key("SINK_PROM_REQUEST_TIMEOUT_MS")
    @DefaultValue("10000")
    Integer getSinkPromRequestTimeoutMs();

    @Key("SINK_PROM_MAX_CONNECTIONS")
    Integer getSinkPromMaxConnections();

    @Key("SINK_PROM_SERVICE_URL")
    String getSinkPromServiceUrl();

    @Key("SINK_PROM_HEADERS")
    @DefaultValue("")
    String getSinkPromHeaders();

    @Key("SINK_PROM_METRIC_NAME_PROTO_INDEX_MAPPING")
    @ConverterClass(ProtoIndexToFieldMapConverter.class)
    Properties getSinkPromMetricNameProtoIndexMapping();

    @Key("SINK_PROM_LABEL_NAME_PROTO_INDEX_MAPPING")
    @ConverterClass(ProtoIndexToFieldMapConverter.class)
    Properties getSinkPromLabelNameProtoIndexMapping();

    @Key("SINK_PROM_PROTO_EVENT_TIMESTAMP_INDEX")
    @DefaultValue("1")
    Integer getSinkPromProtoEventTimestampIndex();

    @Key("SINK_PROM_WITH_EVENT_TIMESTAMP")
    @DefaultValue("false")
    boolean isEventTimestampEnabled();
}
