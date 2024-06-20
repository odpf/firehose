package com.gotocompany.firehose.config;

import com.gotocompany.firehose.config.converter.HttpSinkSerializerJsonTypecastConfigConverter;
import com.gotocompany.firehose.config.enums.HttpSinkDataFormatType;
import com.gotocompany.firehose.config.enums.HttpSinkParameterPlacementType;
import com.gotocompany.firehose.config.enums.HttpSinkParameterSourceType;
import com.gotocompany.firehose.config.enums.HttpSinkRequestMethodType;
import com.gotocompany.firehose.config.converter.HttpSinkRequestMethodConverter;
import com.gotocompany.firehose.config.converter.HttpSinkParameterDataFormatConverter;
import com.gotocompany.firehose.config.converter.HttpSinkParameterPlacementTypeConverter;
import com.gotocompany.firehose.config.converter.HttpSinkParameterSourceTypeConverter;
import com.gotocompany.firehose.config.converter.RangeToHashMapConverter;

import java.util.Map;
import java.util.function.Function;

public interface HttpSinkConfig extends AppConfig {

    @Key("SINK_HTTP_RETRY_STATUS_CODE_RANGES")
    @DefaultValue("400-600")
    @ConverterClass(RangeToHashMapConverter.class)
    Map<Integer, Boolean> getSinkHttpRetryStatusCodeRanges();

    @Key("SINK_HTTP_REQUEST_LOG_STATUS_CODE_RANGES")
    @DefaultValue("400-499")
    @ConverterClass(RangeToHashMapConverter.class)
    Map<Integer, Boolean> getSinkHttpRequestLogStatusCodeRanges();

    @Key("SINK_HTTP_REQUEST_TIMEOUT_MS")
    @DefaultValue("10000")
    Integer getSinkHttpRequestTimeoutMs();

    @Key("SINK_HTTP_REQUEST_METHOD")
    @DefaultValue("put")
    @ConverterClass(HttpSinkRequestMethodConverter.class)
    HttpSinkRequestMethodType getSinkHttpRequestMethod();

    @Key("SINK_HTTP_MAX_CONNECTIONS")
    @DefaultValue("10")
    Integer getSinkHttpMaxConnections();

    @Key("SINK_HTTP_SERVICE_URL")
    String getSinkHttpServiceUrl();

    @Key("SINK_HTTP_HEADERS")
    @DefaultValue("")
    String getSinkHttpHeaders();

    @Key("SINK_HTTP_PARAMETER_SOURCE")
    @DefaultValue("disabled")
    @ConverterClass(HttpSinkParameterSourceTypeConverter.class)
    HttpSinkParameterSourceType getSinkHttpParameterSource();

    @Key("SINK_HTTP_DATA_FORMAT")
    @DefaultValue("proto")
    @ConverterClass(HttpSinkParameterDataFormatConverter.class)
    HttpSinkDataFormatType getSinkHttpDataFormat();

    @Key("SINK_HTTP_OAUTH2_ENABLE")
    @DefaultValue("false")
    Boolean isSinkHttpOAuth2Enable();

    @Key("SINK_HTTP_OAUTH2_ACCESS_TOKEN_URL")
    @DefaultValue("https://localhost:8888")
    String getSinkHttpOAuth2AccessTokenUrl();

    @Key("SINK_HTTP_OAUTH2_CLIENT_NAME")
    @DefaultValue("client_name")
    String getSinkHttpOAuth2ClientName();

    @Key("SINK_HTTP_OAUTH2_CLIENT_SECRET")
    @DefaultValue("client_secret")
    String getSinkHttpOAuth2ClientSecret();

    @Key("SINK_HTTP_OAUTH2_SCOPE")
    @DefaultValue("scope")
    String getSinkHttpOAuth2Scope();

    @Key("SINK_HTTP_JSON_BODY_TEMPLATE")
    @DefaultValue("")
    String getSinkHttpJsonBodyTemplate();

    @Key("SINK_HTTP_PARAMETER_PLACEMENT")
    @DefaultValue("header")
    @ConverterClass(HttpSinkParameterPlacementTypeConverter.class)
    HttpSinkParameterPlacementType getSinkHttpParameterPlacement();

    @Key("SINK_HTTP_PARAMETER_SCHEMA_PROTO_CLASS")
    String getSinkHttpParameterSchemaProtoClass();

    @Key("SINK_HTTP_DELETE_BODY_ENABLE")
    @DefaultValue("true")
    Boolean getSinkHttpDeleteBodyEnable();

    @Key("SINK_HTTP_SIMPLE_DATE_FORMAT_ENABLE")
    @DefaultValue("true")
    Boolean getSinkHttpSimpleDateFormatEnable();

    @Key("SINK_HTTP_SERIALIZER_JSON_TYPECAST")
    @ConverterClass(HttpSinkSerializerJsonTypecastConfigConverter.class)
    @DefaultValue("[]")
    Map<String, Function<String, Object>> getSinkHttpSerializerJsonTypecast();

}
