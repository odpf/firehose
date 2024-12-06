package com.gotocompany.firehose.config;

import org.aeonbits.owner.Config;

public interface ObjectStorageServiceConfig extends Config {

    @Key("${OSS_TYPE}_OSS_ENDPOINT")
    String getOssEndpoint();

    @Key("${OSS_TYPE}_OSS_REGION")
    String getOssRegion();

    @Key("${OSS_TYPE}_OSS_ACCESS_ID")
    String getOssAccessId();

    @Key("${OSS_TYPE}_OSS_ACCESS_KEY")
    String getOssAccessKey();

    @Key("${OSS_TYPE}_OSS_BUCKET_NAME")
    String getOssBucketName();

    @Key("${OSS_TYPE}_OSS_DIRECTORY_PREFIX")
    String getOssDirectoryPrefix();

    @Key("${OSS_TYPE}_OSS_SOCKET_TIMEOUT_MS")
    @DefaultValue("50000")
    Integer getOssSocketTimeoutMs();

    @Key("${OSS_TYPE}_OSS_CONNECTION_TIMEOUT_MS")
    @DefaultValue("50000")
    Integer getOssConnectionTimeoutMs();

    @Key("${OSS_TYPE}_OSS_CONNECTION_REQUEST_TIMEOUT_MS")
    @DefaultValue("-1")
    Integer getOssConnectionRequestTimeoutMs();

    @Key("${OSS_TYPE}_OSS_REQUEST_TIMEOUT_MS")
    @DefaultValue("300000")
    Integer getOssRequestTimeoutMs();

    @Key("${OSS_TYPE}_OSS_RETRY_ENABLED")
    @DefaultValue("true")
    boolean isRetryEnabled();

    @Key("${OSS_TYPE}_OSS_MAX_RETRY_ATTEMPTS")
    @DefaultValue("3")
    int getOssMaxRetryAttempts();

}
