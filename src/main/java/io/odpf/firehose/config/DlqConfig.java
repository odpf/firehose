package io.odpf.firehose.config;

import io.odpf.firehose.config.converter.DlqWriterTypeConverter;
import io.odpf.firehose.config.converter.ObjectStorageTypeConverter;
import io.odpf.firehose.objectstorage.ObjectStorageType;
import io.odpf.firehose.sinkdecorator.dlq.DLQWriterType;

public interface DlqConfig extends AppConfig {

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

    @Key("DLQ_WRITER_TYPE")
    @ConverterClass(DlqWriterTypeConverter.class)
    @DefaultValue("LOG")
    DLQWriterType getDlqWriterType();

    @Key("DLQ_OBJECT_STORAGE_TYPE")
    @DefaultValue("GCS")
    @ConverterClass(ObjectStorageTypeConverter.class)
    ObjectStorageType getObjectStorageType();

    @Key("DLQ_RETRY_MAX_ATTEMPTS")
    @DefaultValue("2147483647")
    Integer getDlqRetryMaxAttempts();

    @Key("DLQ_RETRY_FAIL_AFTER_MAX_ATTEMPT_ENABLE")
    @DefaultValue("true")
    boolean getDlqRetryFailAfterMaxAttemptEnable();


}
