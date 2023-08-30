package com.gotocompany.firehose.config;

import org.aeonbits.owner.Config;

/**
 * GCS_TYPE needs to be set as SINK_BLOB or DLQ_BLOB_STORAGE.
 */
public interface GCSConfig extends Config {

    @Key("${GCS_TYPE}_GCS_GOOGLE_CLOUD_PROJECT_ID")
    String getGCloudProjectID();

    @Key("${GCS_TYPE}_GCS_BUCKET_NAME")
    String getGCSBucketName();

    @Key("${GCS_TYPE}_GCS_DIRECTORY_PREFIX")
    String getGCSDirectoryPrefix();

    @Key("${GCS_TYPE}_GCS_CREDENTIAL_PATH")
    String getGCSCredentialPath();

    /**
     * @return Total retry attempts for GCS object storage.
     */
    @Key("${GCS_TYPE}_GCS_RETRY_MAX_ATTEMPTS")
    @DefaultValue("10")
    Integer getGCSRetryMaxAttempts();

    /**
     * @return Total Timeout after which retries will fail.
     * By default, we can put this large, so that at-least all the retries can happen.
     */
    @Key("${GCS_TYPE}_GCS_RETRY_TOTAL_TIMEOUT_MS")
    @DefaultValue("120000")
    Long getGCSRetryTotalTimeoutMS();

    /**
     * @return Initial delay before retrying.
     */
    @Key("${GCS_TYPE}_GCS_RETRY_INITIAL_DELAY_MS")
    @DefaultValue("1000")
    Long getGCSRetryInitialDelayMS();

    /**
     * @return Max delay before each retry
     */
    @Key("${GCS_TYPE}_GCS_RETRY_MAX_DELAY_MS")
    @DefaultValue("30000")
    Long getGCSRetryMaxDelayMS();

    /**
     * @return The multiplier for the initial delay.
     * For the default value of 2, the delay will be doubled for each retry.
     */
    @Key("${GCS_TYPE}_GCS_RETRY_DELAY_MULTIPLIER")
    @DefaultValue("2")
    Long getGCSRetryDelayMultiplier();

    @Key("${GCS_TYPE}_GCS_RETRY_INITIAL_RPC_TIMEOUT_MS")
    @DefaultValue("5000")
    Long getGCSRetryInitialRPCTimeoutMS();

    /**
     * @return Multiplier of 1 means that the timeout will be constant.
     */
    @Key("${GCS_TYPE}_GCS_RETRY_RPC_TIMEOUT_MULTIPLIER")
    @DefaultValue("1")
    Long getGCSRetryRPCTimeoutMultiplier();

    @Key("${GCS_TYPE}_GCS_RETRY_RPC_MAX_TIMEOUT_MS")
    @DefaultValue("5000")
    Long getGCSRetryRPCMaxTimeoutMS();
}


