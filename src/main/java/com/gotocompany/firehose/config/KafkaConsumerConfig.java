package com.gotocompany.firehose.config;

import com.gotocompany.firehose.config.converter.ConsumerModeConverter;
import com.gotocompany.firehose.config.enums.KafkaConsumerMode;

/**
 * The interface for configurations required to instantiate a consumer.
 */
public interface KafkaConsumerConfig extends AppConfig {
    @Key("SOURCE_KAFKA_ASYNC_COMMIT_ENABLE")
    @DefaultValue("true")
    boolean isSourceKafkaAsyncCommitEnable();

    @Key("SOURCE_KAFKA_COMMIT_ONLY_CURRENT_PARTITIONS_ENABLE")
    @DefaultValue("true")
    boolean isSourceKafkaCommitOnlyCurrentPartitionsEnable();

    @Key("SOURCE_KAFKA_TOPIC")
    String getSourceKafkaTopic();

    @Key("SOURCE_KAFKA_BROKERS")
    String getSourceKafkaBrokers();

    @Key("SOURCE_KAFKA_CONSUMER_GROUP_ID")
    String getSourceKafkaConsumerGroupId();

    @Key("SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_COMMIT_ENABLE")
    @DefaultValue("false")
    boolean isSourceKafkaConsumerConfigAutoCommitEnable();

    @Key("SOURCE_KAFKA_CONSUMER_CONFIG_METADATA_MAX_AGE_MS")
    @DefaultValue("500")
    int getSourceKafkaConsumerConfigMetadataMaxAgeMs();

    @Key("SOURCE_KAFKA_CONSUMER_CONFIG_MAX_POLL_RECORDS")
    @DefaultValue("500")
    int getSourceKafkaConsumerConfigMaxPollRecords();

    @Key("SOURCE_KAFKA_CONSUMER_CONFIG_SESSION_TIMEOUT_MS")
    @DefaultValue("10000")
    int getSourceKafkaConsumerConfigSessionTimeoutMs();

    @Key("SOURCE_KAFKA_POLL_TIMEOUT_MS")
    @DefaultValue("9223372036854775807")
    Long getSourceKafkaPollTimeoutMs();

    @Key("SOURCE_KAFKA_CONSUMER_MODE")
    @ConverterClass(ConsumerModeConverter.class)
    @DefaultValue("SYNC")
    KafkaConsumerMode getSourceKafkaConsumerMode();

    @Key("SOURCE_KAFKA_CONSUMER_CONFIG_MANUAL_COMMIT_MIN_INTERVAL_MS")
    @DefaultValue("-1")
    long getSourceKafkaConsumerManualCommitMinIntervalMs();

    @Key("SOURCE_KAFKA_CONSUMER_CONFIG_PARTITION_ASSIGNMENT_STRATEGY")
    @DefaultValue("org.apache.kafka.clients.consumer.CooperativeStickyAssignor")
    String getSourceKafkaConsumerConfigPartitionAssignmentStrategy();

}
