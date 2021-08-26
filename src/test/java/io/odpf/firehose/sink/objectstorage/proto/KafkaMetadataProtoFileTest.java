package io.odpf.firehose.sink.objectstorage.proto;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Timestamp;
import org.junit.Test;

import static org.junit.Assert.*;

public class KafkaMetadataProtoFileTest {

    @Test
    public void shouldCreateDescriptors() {
        String kafkaMetadataColumnName = "";
        Descriptors.FileDescriptor fileDescriptor = KafkaMetadataProtoFile.createFileDescriptor(kafkaMetadataColumnName);
        Descriptors.Descriptor metadataDescriptor = fileDescriptor.findMessageTypeByName(KafkaMetadataProto.getTypeName());
        Descriptors.Descriptor timestampDescriptor = fileDescriptor.findMessageTypeByName(TimestampProto.getTypeName());

        assertEquals(KafkaMetadataProto.getTypeName(), metadataDescriptor.getName());
        assertEquals(Timestamp.getDescriptor().getName(), timestampDescriptor.getName());
    }

    @Test
    public void shouldCreateDescriptorsForNestedMetadata() {
        String kafkaMetadataColumnName = "metadata_column_name";
        Descriptors.FileDescriptor fileDescriptor = KafkaMetadataProtoFile.createFileDescriptor(kafkaMetadataColumnName);
        Descriptors.Descriptor metadataDescriptor = fileDescriptor.findMessageTypeByName(KafkaMetadataProto.getTypeName());
        Descriptors.Descriptor timestampDescriptor = fileDescriptor.findMessageTypeByName(TimestampProto.getTypeName());
        Descriptors.Descriptor nestedMetadataDescriptor = fileDescriptor.findMessageTypeByName(NestedKafkaMetadataProto.getTypeName());

        assertEquals(KafkaMetadataProto.getTypeName(), metadataDescriptor.getName());
        assertEquals(Timestamp.getDescriptor().getName(), timestampDescriptor.getName());
        assertEquals(NestedKafkaMetadataProto.getTypeName(), nestedMetadataDescriptor.getName());
    }
}
