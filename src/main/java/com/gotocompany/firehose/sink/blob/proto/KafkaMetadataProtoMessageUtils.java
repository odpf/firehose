package com.gotocompany.firehose.sink.blob.proto;

import com.github.os72.protobuf.dynamic.DynamicSchema;
import com.github.os72.protobuf.dynamic.MessageDefinition;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import lombok.Getter;


/**
 * KafkaMetadataProtoUtils provide function to create {@link com.google.protobuf.Descriptors.FileDescriptor FileDescriptor} of kafka metadata proto message.
 */
@Getter
public class KafkaMetadataProtoMessageUtils {
    public static final String FILE_NAME = "Metadata.proto";
    public static final String PACKAGE = "google.protobuf";

    public static Descriptors.FileDescriptor createFileDescriptor(String kafkaMetadataColumnName) {
        DynamicSchema schema = createSchema(kafkaMetadataColumnName);
        return createFileDescriptor(schema);
    }

    private static DynamicSchema createSchema(String kafkaMetadataColumnName) {
        DynamicSchema.Builder schemaBuilder = DynamicSchema.newBuilder().setName(FILE_NAME).setPackage(PACKAGE);

        MessageDefinition timestampMessageDefinition = TimestampMetadataProtoMessage.createMessageDefinition();
        schemaBuilder.addMessageDefinition(timestampMessageDefinition);

        MessageDefinition kafkaMetadataMessageDefinition = KafkaMetadataProtoMessage.createMessageDefinition();
        schemaBuilder.addMessageDefinition(kafkaMetadataMessageDefinition);

        if (!kafkaMetadataColumnName.isEmpty()) {
            MessageDefinition kafkaNestedMetadataProtoMessageDefinition = NestedKafkaMetadataProtoMessage
                    .createMessageDefinition(
                            kafkaMetadataColumnName,
                            KafkaMetadataProtoMessage.getTypeName(),
                            kafkaMetadataMessageDefinition);
            schemaBuilder.addMessageDefinition(kafkaNestedMetadataProtoMessageDefinition);
        }

        DynamicSchema schema;
        try {
            schema = schemaBuilder.build();
        } catch (Descriptors.DescriptorValidationException e) {
            throw new IllegalArgumentException("Invalid proto schema", e);
        }
        return schema;
    }

    public static Descriptors.FileDescriptor createFileDescriptor(DynamicSchema schema) {
        DescriptorProtos.FileDescriptorSet fileDescriptorSet = schema.getFileDescriptorSet();
        DescriptorProtos.FileDescriptorProto fileDescriptorProto = fileDescriptorSet.getFile(0);
        Descriptors.FileDescriptor[] dependencies = {};

        try {
            return Descriptors.FileDescriptor.buildFrom(fileDescriptorProto, dependencies);
        } catch (Descriptors.DescriptorValidationException e) {
            throw new IllegalArgumentException("Invalid proto schema", e);
        }
    }
}
