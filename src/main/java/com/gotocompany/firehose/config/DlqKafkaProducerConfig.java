package com.gotocompany.firehose.config;

public interface DlqKafkaProducerConfig extends DlqConfig {

    @Key("DLQ_KAFKA_ACKS")
    @DefaultValue("all")
    String getDlqKafkaAcks();

    @Key("DLQ_KAFKA_RETRIES")
    @DefaultValue("2147483647")
    String getDlqKafkaRetries();

    @Key("DLQ_KAFKA_BATCH_SIZE")
    @DefaultValue("16384")
    String getDlqKafkaBatchSize();

    @Key("DLQ_KAFKA_LINGER_MS")
    @DefaultValue("0")
    String getDlqKafkaLingerMs();

    @Key("DLQ_KAFKA_BUFFER_MEMORY")
    @DefaultValue("33554432")
    String getDlqKafkaBufferMemory();

    @Key("DLQ_KAFKA_KEY_SERIALIZER")
    @DefaultValue("org.apache.kafka.common.serialization.ByteArraySerializer")
    String getDlqKafkaKeySerializer();

    @Key("DLQ_KAFKA_VALUE_SERIALIZER")
    @DefaultValue("org.apache.kafka.common.serialization.ByteArraySerializer")
    String getDlqKafkaValueSerializer();

    @Key("DLQ_KAFKA_BROKERS")
    String getDlqKafkaBrokers();

    @Key("DLQ_KAFKA_TOPIC")
    @DefaultValue("firehose-retry-topic")
    String getDlqKafkaTopic();
}
