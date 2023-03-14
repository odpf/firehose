package com.gotocompany.firehose.config;

import com.gotocompany.firehose.config.converter.EsSinkMessageTypeConverter;
import com.gotocompany.firehose.config.enums.EsSinkMessageType;


public interface EsSinkConfig extends AppConfig {

    @Key("SINK_ES_SHARDS_ACTIVE_WAIT_COUNT")
    @DefaultValue("1")
    Integer getSinkEsShardsActiveWaitCount();

    @Key("SINK_ES_REQUEST_TIMEOUT_MS")
    @DefaultValue("60000")
    Long getSinkEsRequestTimeoutMs();

    @Key("SINK_ES_RETRY_STATUS_CODE_BLACKLIST")
    @DefaultValue("404")
    String getSinkEsRetryStatusCodeBlacklist();

    @Key("SINK_ES_CONNECTION_URLS")
    String getSinkEsConnectionUrls();

    @Key("SINK_ES_INDEX_NAME")
    String getSinkEsIndexName();

    @Key("SINK_ES_TYPE_NAME")
    String getSinkEsTypeName();

    @Key("SINK_ES_ID_FIELD")
    String getSinkEsIdField();

    @Key("SINK_ES_MODE_UPDATE_ONLY_ENABLE")
    @DefaultValue("false")
    Boolean isSinkEsModeUpdateOnlyEnable();

    @Key("SINK_ES_INPUT_MESSAGE_TYPE")
    @ConverterClass(EsSinkMessageTypeConverter.class)
    @DefaultValue("JSON")
    EsSinkMessageType getSinkEsInputMessageType();

    @Key("SINK_ES_PRESERVE_PROTO_FIELD_NAMES_ENABLE")
    @DefaultValue("true")
    Boolean isSinkEsPreserveProtoFieldNamesEnable();

    @Key("SINK_ES_ROUTING_KEY_NAME")
    String getSinkEsRoutingKeyName();
}
