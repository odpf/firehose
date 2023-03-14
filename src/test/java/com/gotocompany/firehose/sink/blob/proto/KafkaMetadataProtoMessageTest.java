package com.gotocompany.firehose.sink.blob.proto;

import com.github.os72.protobuf.dynamic.DynamicSchema;
import com.github.os72.protobuf.dynamic.MessageDefinition;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Timestamp;
import org.junit.Test;

import java.time.Instant;

import static com.gotocompany.firehose.sink.blob.proto.KafkaMetadataProtoMessageUtils.FILE_NAME;
import static com.gotocompany.firehose.sink.blob.proto.KafkaMetadataProtoMessageUtils.PACKAGE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class KafkaMetadataProtoMessageTest {

    private DynamicSchema.Builder schemaBuilder = DynamicSchema.newBuilder().setName(FILE_NAME).setPackage(PACKAGE);

    @Test
    public void shouldCreateKafkaMetadataMessageDefinition() throws Descriptors.DescriptorValidationException {
        MessageDefinition messageDefinition = KafkaMetadataProtoMessage.createMessageDefinition();
        schemaBuilder.addMessageDefinition(messageDefinition);
        schemaBuilder.addMessageDefinition(TimestampMetadataProtoMessage.createMessageDefinition());

        DynamicSchema dynamicSchema = schemaBuilder.build();
        Descriptors.Descriptor descriptor = dynamicSchema.getMessageDescriptor(KafkaMetadataProtoMessage.getTypeName());

        assertNotNull(descriptor.findFieldByName(KafkaMetadataProtoMessage.MESSAGE_OFFSET_FIELD_NAME));
        assertNotNull(descriptor.findFieldByName(KafkaMetadataProtoMessage.MESSAGE_PARTITION_FIELD_NAME));
        assertNotNull(descriptor.findFieldByName(KafkaMetadataProtoMessage.MESSAGE_TOPIC_FIELD_NAME));
        assertNotNull(descriptor.findFieldByName(KafkaMetadataProtoMessage.MESSAGE_TIMESTAMP_FIELD_NAME));
        assertNotNull(descriptor.findFieldByName(KafkaMetadataProtoMessage.LOAD_TIME_FIELD_NAME));
    }

    @Test
    public void shouldCreateKafkaMetadataDynamicMessage() throws Descriptors.DescriptorValidationException {
        MessageDefinition messageDefinition = KafkaMetadataProtoMessage.createMessageDefinition();
        schemaBuilder.addMessageDefinition(messageDefinition);
        schemaBuilder.addMessageDefinition(TimestampMetadataProtoMessage.createMessageDefinition());

        DynamicSchema dynamicSchema = schemaBuilder.build();
        Descriptors.Descriptor descriptor = dynamicSchema.getMessageDescriptor(KafkaMetadataProtoMessage.getTypeName());

        DynamicMessage dynamicMessage = KafkaMetadataProtoMessage.newBuilder(descriptor).setOffset(1)
                .setPartition(1)
                .setTopic("default")
                .setMessageTimestamp(Instant.EPOCH)
                .setLoadTime(Instant.EPOCH)
                .build();

        assertEquals(1L, dynamicMessage.getField(descriptor.findFieldByName(KafkaMetadataProtoMessage.MESSAGE_OFFSET_FIELD_NAME)));
        assertEquals(1, dynamicMessage.getField(descriptor.findFieldByName(KafkaMetadataProtoMessage.MESSAGE_PARTITION_FIELD_NAME)));
        assertEquals("default", dynamicMessage.getField(descriptor.findFieldByName(KafkaMetadataProtoMessage.MESSAGE_TOPIC_FIELD_NAME)));
        Timestamp timestamp = Timestamp.newBuilder().setSeconds(0).setNanos(0).build();
        assertEquals(timestamp, dynamicMessage.getField(descriptor.findFieldByName(KafkaMetadataProtoMessage.MESSAGE_TIMESTAMP_FIELD_NAME)));
        assertEquals(timestamp, dynamicMessage.getField(descriptor.findFieldByName(KafkaMetadataProtoMessage.LOAD_TIME_FIELD_NAME)));
    }
}
