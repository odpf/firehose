package com.gotocompany.firehose.config;

public interface JdbcSinkConfig extends AppConfig {

    @Key("SINK_JDBC_URL")
    String getSinkJdbcUrl();

    @Key("SINK_JDBC_USERNAME")
    String getSinkJdbcUsername();

    @Key("SINK_JDBC_PASSWORD")
    String getSinkJdbcPassword();

    @Key("SINK_JDBC_TABLE_NAME")
    String getSinkJdbcTableName();

    @Key("SINK_JDBC_UNIQUE_KEYS")
    @DefaultValue("")
    String getSinkJdbcUniqueKeys();

    @Key("SINK_JDBC_CONNECTION_POOL_MAX_SIZE")
    Integer getSinkJdbcConnectionPoolMaxSize();

    @Key("SINK_JDBC_CONNECTION_POOL_TIMEOUT_MS")
    Integer getSinkJdbcConnectionPoolTimeoutMs();

    @Key("SINK_JDBC_CONNECTION_POOL_IDLE_TIMEOUT_MS")
    Integer getSinkJdbcConnectionPoolIdleTimeoutMs();

    @Key("SINK_JDBC_CONNECTION_POOL_MIN_IDLE")
    Integer getSinkJdbcConnectionPoolMinIdle();
}
