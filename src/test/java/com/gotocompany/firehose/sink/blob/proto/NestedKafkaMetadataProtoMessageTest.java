package com.gotocompany.firehose.sink.blob.proto;

import com.github.os72.protobuf.dynamic.DynamicSchema;
import com.github.os72.protobuf.dynamic.MessageDefinition;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import org.junit.Test;

import java.time.Instant;

import static com.gotocompany.firehose.sink.blob.proto.KafkaMetadataProtoMessageUtils.FILE_NAME;
import static com.gotocompany.firehose.sink.blob.proto.KafkaMetadataProtoMessageUtils.PACKAGE;
import static org.junit.Assert.*;

public class NestedKafkaMetadataProtoMessageTest {

    private DynamicSchema.Builder schemaBuilder = DynamicSchema.newBuilder().setName(FILE_NAME).setPackage(PACKAGE);
    private final String metadataColumnName = "metadata";

    @Test
    public void shouldGenerateMessageDefinition() throws Descriptors.DescriptorValidationException {
        MessageDefinition kafkaMetadataMessageDefinition = KafkaMetadataProtoMessage.createMessageDefinition();
        schemaBuilder.addMessageDefinition(kafkaMetadataMessageDefinition);
        schemaBuilder.addMessageDefinition(TimestampMetadataProtoMessage.createMessageDefinition());
        MessageDefinition nestedKafkaMetadataMessageDefinition = NestedKafkaMetadataProtoMessage.createMessageDefinition(metadataColumnName,
                KafkaMetadataProtoMessage.getTypeName(),
                kafkaMetadataMessageDefinition);
        schemaBuilder.addMessageDefinition(nestedKafkaMetadataMessageDefinition);

        DynamicSchema dynamicSchema = schemaBuilder.build();

        Descriptors.Descriptor nestedDescriptor = dynamicSchema.getMessageDescriptor(NestedKafkaMetadataProtoMessage.getTypeName());
        assertNotNull(nestedDescriptor.findFieldByName(metadataColumnName));

        Descriptors.Descriptor metadataDescriptor = nestedDescriptor.findFieldByName(metadataColumnName).getMessageType();
        assertNotNull(metadataDescriptor.findFieldByName(KafkaMetadataProtoMessage.MESSAGE_OFFSET_FIELD_NAME));
        assertNotNull(metadataDescriptor.findFieldByName(KafkaMetadataProtoMessage.MESSAGE_PARTITION_FIELD_NAME));
        assertNotNull(metadataDescriptor.findFieldByName(KafkaMetadataProtoMessage.MESSAGE_TOPIC_FIELD_NAME));
        assertNotNull(metadataDescriptor.findFieldByName(KafkaMetadataProtoMessage.MESSAGE_TIMESTAMP_FIELD_NAME));
        assertNotNull(metadataDescriptor.findFieldByName(KafkaMetadataProtoMessage.LOAD_TIME_FIELD_NAME));
    }

    @Test
    public void shouldCreateDynamicMessage() throws Descriptors.DescriptorValidationException {
        MessageDefinition kafkaMetadataMessageDefinition = KafkaMetadataProtoMessage.createMessageDefinition();
        schemaBuilder.addMessageDefinition(kafkaMetadataMessageDefinition);
        schemaBuilder.addMessageDefinition(TimestampMetadataProtoMessage.createMessageDefinition());
        MessageDefinition nestedKafkaMetadataMessageDefinition = NestedKafkaMetadataProtoMessage.createMessageDefinition(metadataColumnName,
                KafkaMetadataProtoMessage.getTypeName(),
                kafkaMetadataMessageDefinition);
        schemaBuilder.addMessageDefinition(nestedKafkaMetadataMessageDefinition);
        DynamicSchema dynamicSchema = schemaBuilder.build();

        Descriptors.Descriptor metadataDescriptor = dynamicSchema.getMessageDescriptor(KafkaMetadataProtoMessage.getTypeName());
        DynamicMessage metadataMessage = KafkaMetadataProtoMessage.newBuilder(metadataDescriptor)
                .setOffset(1)
                .setPartition(1)
                .setTopic("default")
                .setMessageTimestamp(Instant.EPOCH)
                .setLoadTime(Instant.EPOCH)
                .build();

        Descriptors.Descriptor nestedMetadataDescriptor = dynamicSchema.getMessageDescriptor(NestedKafkaMetadataProtoMessage.getTypeName());
        DynamicMessage nestedMetadataMessage = NestedKafkaMetadataProtoMessage.newMessageBuilder(nestedMetadataDescriptor)
                .setMetadata(metadataMessage)
                .setMetadataColumnName(metadataColumnName)
                .build();

        assertEquals(metadataMessage, nestedMetadataMessage.getField(nestedMetadataDescriptor.findFieldByName(metadataColumnName)));
    }
}
