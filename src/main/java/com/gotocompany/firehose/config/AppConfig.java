package com.gotocompany.firehose.config;

import com.gotocompany.firehose.config.enums.InputSchemaType;
import com.gotocompany.firehose.config.enums.SinkType;
import com.gotocompany.firehose.config.converter.InputSchemaTypeConverter;
import com.gotocompany.firehose.config.converter.ProtoIndexToFieldMapConverter;
import com.gotocompany.firehose.config.converter.SchemaRegistryHeadersConverter;
import com.gotocompany.firehose.config.converter.SchemaRegistryRefreshConverter;
import com.gotocompany.firehose.config.converter.SinkTypeConverter;
import com.gotocompany.stencil.cache.SchemaRefreshStrategy;

import org.aeonbits.owner.Config;
import org.apache.http.Header;

import java.util.List;
import java.util.Properties;

public interface AppConfig extends Config {

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

    @Key("SCHEMA_REGISTRY_STENCIL_REFRESH_STRATEGY")
    @ConverterClass(SchemaRegistryRefreshConverter.class)
    @DefaultValue("VERSION_BASED_REFRESH")
    SchemaRefreshStrategy getSchemaRegistryStencilRefreshStrategy();

    @Key("SCHEMA_REGISTRY_STENCIL_FETCH_HEADERS")
    @TokenizerClass(SchemaRegistryHeadersConverter.class)
    @ConverterClass(SchemaRegistryHeadersConverter.class)
    @DefaultValue("")
    List<Header> getSchemaRegistryFetchHeaders();

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

    @Key("INPUT_SCHEMA_DATA_TYPE")
    @DefaultValue("PROTOBUF")
    @ConverterClass(InputSchemaTypeConverter.class)
    InputSchemaType getInputSchemaType();

    @Key("INPUT_SCHEMA_PROTO_TO_COLUMN_MAPPING")
    @ConverterClass(ProtoIndexToFieldMapConverter.class)
    Properties getInputSchemaProtoToColumnMapping();

    @Key("KAFKA_RECORD_PARSER_MODE")
    @DefaultValue("message")
    String getKafkaRecordParserMode();

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

    @Key("RETRY_FAIL_AFTER_MAX_ATTEMPTS_ENABLE")
    @DefaultValue("false")
    boolean getRetryFailAfterMaxAttemptsEnable();

    @Key("RETRY_MAX_ATTEMPTS")
    @DefaultValue("2147483647")
    Integer getRetryMaxAttempts();

    @Key("INPUT_SCHEMA_PROTO_ALLOW_UNKNOWN_FIELDS_ENABLE")
    @DefaultValue("true")
    boolean getInputSchemaProtoAllowUnknownFieldsEnable();
}
