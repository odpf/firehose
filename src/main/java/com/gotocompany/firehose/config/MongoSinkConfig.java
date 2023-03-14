package com.gotocompany.firehose.config;


import com.gotocompany.firehose.config.converter.MongoSinkMessageTypeConverter;
import com.gotocompany.firehose.config.enums.MongoSinkMessageType;

public interface MongoSinkConfig extends AppConfig {

    @Key("SINK_MONGO_CONNECT_TIMEOUT_MS")
    @DefaultValue("30000")
    int getSinkMongoConnectTimeoutMs();

    @Key("SINK_MONGO_CONNECTION_URLS")
    String getSinkMongoConnectionUrls();

    @Key("SINK_MONGO_DB_NAME")
    String getSinkMongoDBName();

    @Key("SINK_MONGO_RETRY_STATUS_CODE_BLACKLIST")
    @DefaultValue("11000")
    String getSinkMongoRetryStatusCodeBlacklist();

    @Key("SINK_MONGO_PRESERVE_PROTO_FIELD_NAMES_ENABLE")
    @DefaultValue("true")
    Boolean isSinkMongoPreserveProtoFieldNamesEnable();

    @Key("SINK_MONGO_AUTH_ENABLE")
    @DefaultValue("false")
    Boolean isSinkMongoAuthEnable();

    @Key("SINK_MONGO_AUTH_USERNAME")
    String getSinkMongoAuthUsername();

    @Key("SINK_MONGO_AUTH_PASSWORD")
    String getSinkMongoAuthPassword();

    @Key("SINK_MONGO_AUTH_DB")
    String getSinkMongoAuthDB();

    @Key("SINK_MONGO_INPUT_MESSAGE_TYPE")
    @ConverterClass(MongoSinkMessageTypeConverter.class)
    @DefaultValue("JSON")
    MongoSinkMessageType getSinkMongoInputMessageType();

    @Key("SINK_MONGO_COLLECTION_NAME")
    String getSinkMongoCollectionName();

    @Key("SINK_MONGO_PRIMARY_KEY")
    String getSinkMongoPrimaryKey();

    @Key("SINK_MONGO_MODE_UPDATE_ONLY_ENABLE")
    @DefaultValue("false")
    Boolean isSinkMongoModeUpdateOnlyEnable();

    @Key("SINK_MONGO_SERVER_SELECT_TIMEOUT_MS")
    @DefaultValue("30000")
    int getSinkMongoServerSelectTimeoutMs();
}
