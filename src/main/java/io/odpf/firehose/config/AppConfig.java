package io.odpf.firehose.config;

import io.odpf.firehose.config.converter.FilterTypeConverter;
import io.odpf.firehose.config.converter.ProtoIndexToFieldMapConverter;
import io.odpf.firehose.config.converter.SinkTypeConverter;
import io.odpf.firehose.config.enums.FilterType;
import io.odpf.firehose.config.enums.SinkType;
import org.aeonbits.owner.Config;

import java.util.Properties;

public interface AppConfig extends Config {

    @Key("METRIC_STATSD_HOST")
    @DefaultValue("localhost")
    String getMetricStatsDHost();

    @Key("METRIC_STATSD_PORT")
    @DefaultValue("8125")
    Integer getMetricStatsDPort();

    @Key("METRIC_STATSD_TAGS")
    @DefaultValue("")
    String getMetricStatsDTags();

    @Key("SINK_TYPE")
    @ConverterClass(SinkTypeConverter.class)
    SinkType getSinkType();

    @Key("APPLICATION_THREAD_COUNT")
    @DefaultValue("1")
    Integer getApplicationThreadCount();

    @Key("APPLICATION_THREAD_CLEANUP_DELAY")
    @DefaultValue("2000")
    Integer getApplicationThreadCleanupDelay();

    @Key("SCHEMA_REGISTRY_STENCIL_ENABLE")
    @DefaultValue("false")
    Boolean isSchemaRegistryStencilEnable();

    @Key("SCHEMA_REGISTRY_STENCIL_FETCH_TIMEOUT_MS")
    @DefaultValue("10000")
    Integer getSchemaRegistryStencilFetchTimeoutMs();

    @Key("SCHEMA_REGISTRY_STENCIL_FETCH_RETRIES")
    @DefaultValue("4")
    Integer getSchemaRegistryStencilFetchRetries();

    @Key("SCHEMA_REGISTRY_STENCIL_FETCH_BACKOFF_MIN_MS")
    @DefaultValue("60000")
    Long getSchemaRegistryStencilFetchBackoffMinMs();

    @Key("SCHEMA_REGISTRY_STENCIL_FETCH_AUTH_BEARER_TOKEN")
    String getSchemaRegistryStencilFetchAuthBearerToken();

    @Key("SCHEMA_REGISTRY_STENCIL_CACHE_AUTO_REFRESH")
    @DefaultValue("false")
    Boolean getSchemaRegistryStencilCacheAutoRefresh();

    @Key("SCHEMA_REGISTRY_STENCIL_CACHE_TTL_MS")
    @DefaultValue("900000")
    Long getSchemaRegistryStencilCacheTtlMs();

    @Key("SCHEMA_REGISTRY_STENCIL_URLS")
    String getSchemaRegistryStencilUrls();

    @Key("INPUT_SCHEMA_PROTO_CLASS")
    String getInputSchemaProtoClass();

    @Key("INPUT_SCHEMA_PROTO_TO_COLUMN_MAPPING")
    @ConverterClass(ProtoIndexToFieldMapConverter.class)
    Properties getInputSchemaProtoToColumnMapping();

    @Key("KAFKA_RECORD_PARSER_MODE")
    @DefaultValue("message")
    String getKafkaRecordParserMode();

    @Key("FILTER_JEXL_DATA_SOURCE")
    @ConverterClass(FilterTypeConverter.class)
    @DefaultValue("NONE")
    FilterType getFilterJexlDataSource();

    @Key("FILTER_JEXL_EXPRESSION")
    String getFilterJexlExpression();

    @Key("FILTER_JEXL_SCHEMA_PROTO_CLASS")
    String getFilterJexlSchemaProtoClass();

    @Key("TRACE_JAEGAR_ENABLE")
    @DefaultValue("false")
    Boolean isTraceJaegarEnable();

    @Key("RETRY_EXPONENTIAL_BACKOFF_INITIAL_MS")
    @DefaultValue("10")
    Integer getRetryExponentialBackoffInitialMs();

    @Key("RETRY_EXPONENTIAL_BACKOFF_RATE")
    @DefaultValue("2")
    Integer getRetryExponentialBackoffRate();

    @Key("RETRY_EXPONENTIAL_BACKOFF_MAX_MS")
    @DefaultValue("60000")
    Integer getRetryExponentialBackoffMaxMs();

    @Key("DLQ_ENABLE")
    @DefaultValue("false")
    Boolean isDlqEnable();
}
