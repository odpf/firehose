package com.gotocompany.firehose.sink.blob.message;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Timestamp;
import com.gotocompany.firehose.message.Message;
import com.gotocompany.firehose.sink.blob.proto.KafkaMetadataProtoMessage;
import com.gotocompany.firehose.sink.blob.proto.KafkaMetadataProtoMessageUtils;
import org.junit.Test;

import java.time.Instant;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;


public class KafkaMetadataUtilsTest {

    private final String topic = "default";
    private final long offset = 1L;
    private final int partition = 1;
    private final byte[] logKey = "key".getBytes();
    private final byte[] logMessage = "value".getBytes();
    private final Instant timestamp = Instant.parse("2021-01-01T00:00:00.000Z");
    private final Instant consumeTimestamp = Instant.parse("2021-01-01T10:00:00.000Z");

    @Test
    public void shouldCreateKafkaMetadataDynamicMessage() {
        String kafkaMetadataColumnName = "";

        Message message = new Message(logKey, logMessage, topic, partition, offset, null, timestamp.toEpochMilli(), consumeTimestamp.toEpochMilli());
        Descriptors.FileDescriptor fileDescriptor = KafkaMetadataProtoMessageUtils.createFileDescriptor(kafkaMetadataColumnName);
        DynamicMessage kafkaMetadata = KafkaMetadataUtils.createKafkaMetadata(fileDescriptor, message, kafkaMetadataColumnName);

        Descriptors.Descriptor descriptor = kafkaMetadata.getDescriptorForType();

        assertEquals(topic, kafkaMetadata.getField(descriptor.findFieldByName(KafkaMetadataProtoMessage.MESSAGE_TOPIC_FIELD_NAME)));
        assertEquals(partition, kafkaMetadata.getField(descriptor.findFieldByName(KafkaMetadataProtoMessage.MESSAGE_PARTITION_FIELD_NAME)));
        assertEquals(offset, kafkaMetadata.getField(descriptor.findFieldByName(KafkaMetadataProtoMessage.MESSAGE_OFFSET_FIELD_NAME)));
        assertEquals(Timestamp.newBuilder()
                        .setSeconds(timestamp.getEpochSecond())
                        .setNanos(timestamp.getNano()).build(),
                kafkaMetadata.getField(descriptor.findFieldByName(KafkaMetadataProtoMessage.MESSAGE_TIMESTAMP_FIELD_NAME)));
        assertThat(kafkaMetadata.getField(descriptor.findFieldByName(KafkaMetadataProtoMessage.LOAD_TIME_FIELD_NAME)), is(instanceOf(Timestamp.class)));
    }

    @Test
    public void shouldCreateKafkaDynamicMessageWhenMetadataIsNested() {
        String kafkaMetadataColumnName = "metadata_field_name";

        Message message = new Message(logKey, logMessage, topic, partition, offset, null, timestamp.toEpochMilli(), consumeTimestamp.toEpochMilli());

        Descriptors.FileDescriptor fileDescriptor = KafkaMetadataProtoMessageUtils.createFileDescriptor(kafkaMetadataColumnName);
        DynamicMessage nestedKafkaMetadata = KafkaMetadataUtils.createKafkaMetadata(fileDescriptor, message, kafkaMetadataColumnName);
        Descriptors.Descriptor nestedMetadataDescriptor = nestedKafkaMetadata.getDescriptorForType();

        DynamicMessage kafkaMetadata = (DynamicMessage) nestedKafkaMetadata.getField(nestedMetadataDescriptor.findFieldByName(kafkaMetadataColumnName));
        Descriptors.Descriptor descriptor = kafkaMetadata.getDescriptorForType();

        assertEquals(topic, kafkaMetadata.getField(descriptor.findFieldByName(KafkaMetadataProtoMessage.MESSAGE_TOPIC_FIELD_NAME)));
        assertEquals(partition, kafkaMetadata.getField(descriptor.findFieldByName(KafkaMetadataProtoMessage.MESSAGE_PARTITION_FIELD_NAME)));
        assertEquals(offset, kafkaMetadata.getField(descriptor.findFieldByName(KafkaMetadataProtoMessage.MESSAGE_OFFSET_FIELD_NAME)));
        assertEquals(Timestamp.newBuilder()
                        .setSeconds(timestamp.getEpochSecond())
                        .setNanos(timestamp.getNano()).build(),
                kafkaMetadata.getField(descriptor.findFieldByName(KafkaMetadataProtoMessage.MESSAGE_TIMESTAMP_FIELD_NAME)));
        assertThat(kafkaMetadata.getField(descriptor.findFieldByName(KafkaMetadataProtoMessage.LOAD_TIME_FIELD_NAME)), is(instanceOf(Timestamp.class)));
    }

}
