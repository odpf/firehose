package com.gotocompany.firehose.sink.blob.proto;

import com.github.os72.protobuf.dynamic.DynamicSchema;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Timestamp;
import org.junit.Test;

import static com.gotocompany.firehose.sink.blob.proto.KafkaMetadataProtoMessageUtils.FILE_NAME;
import static com.gotocompany.firehose.sink.blob.proto.KafkaMetadataProtoMessageUtils.PACKAGE;
import static org.junit.Assert.*;

public class TimestampMetadataProtoMessageTest {

    private DynamicSchema.Builder schemaBuilder = DynamicSchema.newBuilder().setName(FILE_NAME).setPackage(PACKAGE);

    @Test
    public void shouldGenerateMessageDefinition() throws Descriptors.DescriptorValidationException {
        schemaBuilder.addMessageDefinition(TimestampMetadataProtoMessage.createMessageDefinition());
        DynamicSchema dynamicSchema = schemaBuilder.build();

        Descriptors.Descriptor descriptor = dynamicSchema.getMessageDescriptor(TimestampMetadataProtoMessage.getTypeName());
        assertNotNull(descriptor.findFieldByName(TimestampMetadataProtoMessage.NANOS_FIELD_NAME));
        assertNotNull(descriptor.findFieldByName(TimestampMetadataProtoMessage.SECONDS_FIELD_NAME));
    }

    @Test
    public void shouldCreateDynamicMessage() {
        Timestamp timestamp = TimestampMetadataProtoMessage.newBuilder().setNanos(1).setSeconds(1)
                .build();
        assertEquals(1, timestamp.getNanos());
        assertEquals(1, timestamp.getSeconds());
    }
}
