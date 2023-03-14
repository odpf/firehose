package com.gotocompany.firehose.sink.blob.proto;

import com.github.os72.protobuf.dynamic.DynamicSchema;
import com.google.protobuf.Descriptors;
import com.gotocompany.firehose.sink.blob.TestProtoMessage;
import org.junit.Test;

import static org.junit.Assert.*;

public class KafkaMetadataProtoFirehoseMessageUtilsTest {

    @Test
    public void shouldCreateDescriptors() {
        String kafkaMetadataColumnName = "";
        Descriptors.FileDescriptor fileDescriptor = KafkaMetadataProtoMessageUtils.createFileDescriptor(kafkaMetadataColumnName);
        Descriptors.Descriptor kafkaMetadataDescriptor = fileDescriptor.findMessageTypeByName(KafkaMetadataProtoMessage.getTypeName());
        Descriptors.Descriptor timestampDescriptor = fileDescriptor.findMessageTypeByName(TimestampMetadataProtoMessage.getTypeName());

        assertNotNull(kafkaMetadataDescriptor);
        assertNotNull(timestampDescriptor);
    }

    @Test
    public void shouldCreateDescriptorsForNestedMetadata() {
        String kafkaMetadataColumnName = "metadata_column_name";
        Descriptors.FileDescriptor fileDescriptor = KafkaMetadataProtoMessageUtils.createFileDescriptor(kafkaMetadataColumnName);
        Descriptors.Descriptor kafkaMetadataDescriptor = fileDescriptor.findMessageTypeByName(KafkaMetadataProtoMessage.getTypeName());
        Descriptors.Descriptor timestampDescriptor = fileDescriptor.findMessageTypeByName(TimestampMetadataProtoMessage.getTypeName());
        Descriptors.Descriptor nestedKafkaMetadataDescriptor = fileDescriptor.findMessageTypeByName(NestedKafkaMetadataProtoMessage.getTypeName());

        assertNotNull(kafkaMetadataDescriptor);
        assertNotNull(timestampDescriptor);
        assertNotNull(nestedKafkaMetadataDescriptor);
    }


    @Test
    public void shouldCreateFileDescriptor() {
        DynamicSchema dynamicSchema = TestProtoMessage.createSchema();
        Descriptors.FileDescriptor fileDescriptor = KafkaMetadataProtoMessageUtils.createFileDescriptor(dynamicSchema);
        Descriptors.Descriptor descriptor = fileDescriptor.findMessageTypeByName(TestProtoMessage.getTypeName());

        assertNotNull(descriptor);
        assertNotNull(descriptor.findFieldByName(TestProtoMessage.ORDER_NUMBER_FIELD_NAME));
        assertNotNull(descriptor.findFieldByName(TestProtoMessage.CREATED_TIME_FIELD_NAME));
    }
}
