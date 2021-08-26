package io.odpf.firehose.sink.common;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.UnknownFieldSet;
import io.odpf.firehose.sink.objectstorage.proto.KafkaMetadataProto;
import io.odpf.firehose.sink.objectstorage.proto.KafkaMetadataProtoFile;
import io.odpf.firehose.sink.objectstorage.proto.NestedKafkaMetadataProto;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ProtoUtilTest {
    @Test
    public void shouldReturnTrueWhenUnknownFieldsExistOnRootLevelFields() {
        Descriptors.FileDescriptor fileDescriptor = KafkaMetadataProtoFile.createFileDescriptor("meta_field");
        Descriptors.Descriptor nestedDescriptor = fileDescriptor.findMessageTypeByName(NestedKafkaMetadataProto.getTypeName());
        Descriptors.Descriptor metaDescriptor = fileDescriptor.findMessageTypeByName(KafkaMetadataProto.getTypeName());

        Descriptors.FieldDescriptor fieldDescriptor = nestedDescriptor.findFieldByName("meta_field");
        DynamicMessage dynamicMessage = DynamicMessage.newBuilder(nestedDescriptor)
                .setField(fieldDescriptor, DynamicMessage.newBuilder(metaDescriptor)
                        .build())
                .setUnknownFields(UnknownFieldSet.newBuilder()
                        .addField(1, UnknownFieldSet.Field.getDefaultInstance())
                        .addField(2, UnknownFieldSet.Field.getDefaultInstance())
                        .build())
                .build();

        boolean unknownFieldExist = ProtoUtils.hasUnknownField(dynamicMessage);
        assertTrue(unknownFieldExist);
    }

    @Test
    public void shouldReturnTrueWhenUnknownFieldsExistOnNestedChildFields() {
        Descriptors.FileDescriptor fileDescriptor = KafkaMetadataProtoFile.createFileDescriptor("meta_field");
        Descriptors.Descriptor nestedDescriptor = fileDescriptor.findMessageTypeByName(NestedKafkaMetadataProto.getTypeName());
        Descriptors.Descriptor metaDescriptor = fileDescriptor.findMessageTypeByName(KafkaMetadataProto.getTypeName());
        Descriptors.FieldDescriptor fieldDescriptor = nestedDescriptor.findFieldByName("meta_field");

        DynamicMessage dynamicMessage = DynamicMessage.newBuilder(nestedDescriptor)
                .setField(fieldDescriptor, DynamicMessage.newBuilder(metaDescriptor)
                        .setUnknownFields(UnknownFieldSet.newBuilder()
                                .addField(1, UnknownFieldSet.Field.getDefaultInstance())
                                .addField(2, UnknownFieldSet.Field.getDefaultInstance())
                                .build())
                        .build())
                .build();

        boolean unknownFieldExist = ProtoUtils.hasUnknownField(dynamicMessage);
        assertTrue(unknownFieldExist);
    }

    @Test
    public void shouldReturnFalseWhenRootIsNull() {
        boolean unknownFieldExist = ProtoUtils.hasUnknownField(null);
        assertFalse(unknownFieldExist);
    }
}

