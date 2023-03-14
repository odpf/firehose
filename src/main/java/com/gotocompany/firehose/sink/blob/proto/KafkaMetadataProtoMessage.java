package com.gotocompany.firehose.sink.blob.proto;

import com.github.os72.protobuf.dynamic.MessageDefinition;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Timestamp;

import java.time.Instant;

/**
 * KafkaMetadataProtoMessage is a class that contains schema of proto message that contains kafka metadata.
 * This class provides {@link com.github.os72.protobuf.dynamic.MessageDefinition} to generate protobuf descriptor and builder of kafka metadata {@link com.google.protobuf.DynamicMessage}.
 *
 * message KafkaOffsetMetadata{
 *     int64 message_offset = 536870907;
 *     int32 message_partition = 536870908;
 *     string message_topic = 536870909;
 *     Timestamp message_timestamp = 536870910;
 *     Timestamp load_time = 536870911;
 * }
 *
 */
public class KafkaMetadataProtoMessage {
    private static final String TYPE_NAME = "KafkaOffsetMetadata";

    public static final String MESSAGE_OFFSET_FIELD_NAME = "message_offset";
    public static final String MESSAGE_PARTITION_FIELD_NAME = "message_partition";
    public static final String MESSAGE_TOPIC_FIELD_NAME = "message_topic";
    public static final String MESSAGE_TIMESTAMP_FIELD_NAME = "message_timestamp";
    public static final String LOAD_TIME_FIELD_NAME = "load_time";
    public static final int MESSAGE_OFFSET_FIELD_NUMBER = 536870907;
    public static final int MESSAGE_PARTITION_FIELD_NUMBER = 536870908;
    public static final int MESSAGE_TOPIC_FIELD_NUMBER = 536870909;
    public static final int MESSAGE_TIMESTAMP_FIELD_NUMBER = 536870910;
    public static final int LOAD_TIME_FIELD_NUMBER = 536870911;

    public static MessageDefinition createMessageDefinition() {
        return MessageDefinition.newBuilder(TYPE_NAME)
                .addField("optional", "int64", MESSAGE_OFFSET_FIELD_NAME, MESSAGE_OFFSET_FIELD_NUMBER)
                .addField("optional", "int32", MESSAGE_PARTITION_FIELD_NAME, MESSAGE_PARTITION_FIELD_NUMBER)
                .addField("optional", "string", MESSAGE_TOPIC_FIELD_NAME, MESSAGE_TOPIC_FIELD_NUMBER)
                .addField("optional", "Timestamp", MESSAGE_TIMESTAMP_FIELD_NAME, MESSAGE_TIMESTAMP_FIELD_NUMBER)
                .addField("optional", "Timestamp", LOAD_TIME_FIELD_NAME, LOAD_TIME_FIELD_NUMBER)
                .build();
    }

    public static String getTypeName() {
        return TYPE_NAME;
    }

    /**
     * Builder of KafkaOffsetMetadata dynamic message.
     */
    public static class MessageBuilder {

        private String topic;
        private int partition;
        private long offset;
        private Instant loadTime;
        private Instant messageTimestamp;

        private Descriptors.Descriptor descriptor;

        public MessageBuilder(Descriptors.Descriptor descriptor) {
            this.descriptor = descriptor;
        }

        public MessageBuilder setTopic(String topic) {
            this.topic = topic;
            return this;
        }

        public MessageBuilder setPartition(int partition) {
            this.partition = partition;
            return this;
        }

        public MessageBuilder setOffset(long offset) {
            this.offset = offset;
            return this;
        }

        public MessageBuilder setLoadTime(Instant loadTime) {
            this.loadTime = loadTime;
            return this;
        }

        public MessageBuilder setMessageTimestamp(Instant messageTimestamp) {
            this.messageTimestamp = messageTimestamp;
            return this;
        }

        public DynamicMessage build() {
            Timestamp timestamp = TimestampMetadataProtoMessage.newBuilder()
                    .setSeconds(loadTime.getEpochSecond())
                    .setNanos(loadTime.getNano())
                    .build();
            return DynamicMessage.newBuilder(descriptor)
                    .setField(descriptor.findFieldByName(LOAD_TIME_FIELD_NAME), timestamp)
                    .setField(descriptor.findFieldByName(MESSAGE_TIMESTAMP_FIELD_NAME), TimestampMetadataProtoMessage.newBuilder()
                            .setSeconds(messageTimestamp.getEpochSecond())
                            .setNanos(messageTimestamp.getNano())
                            .build())
                    .setField(descriptor.findFieldByName(MESSAGE_OFFSET_FIELD_NAME), offset)
                    .setField(descriptor.findFieldByName(MESSAGE_PARTITION_FIELD_NAME), partition)
                    .setField(descriptor.findFieldByName(MESSAGE_TOPIC_FIELD_NAME), topic)
                    .build();
        }
    }

    public static MessageBuilder newBuilder(Descriptors.Descriptor descriptor) {
        return new MessageBuilder(descriptor);
    }
}
