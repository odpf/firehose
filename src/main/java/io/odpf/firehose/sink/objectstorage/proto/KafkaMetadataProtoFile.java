package io.odpf.firehose.sink.objectstorage.proto;

import com.github.os72.protobuf.dynamic.DynamicSchema;
import com.github.os72.protobuf.dynamic.MessageDefinition;
import com.google.protobuf.Descriptors;
import lombok.Getter;

@Getter
public class KafkaMetadataProtoFile {
    public static final String FILE_NAME = "Metadata.proto";
    public static final String PACKAGE = "google.protobuf";

    public static Descriptors.FileDescriptor createFileDescriptor(String kafkaMetadataColumnName) {
        DynamicSchema schema = createSchema(kafkaMetadataColumnName);
        return ProtoUtils.createFileDescriptor(schema);
    }

    private static DynamicSchema createSchema(String kafkaMetadataColumnName) {
        DynamicSchema.Builder schemaBuilder = DynamicSchema.newBuilder().setName(FILE_NAME).setPackage(PACKAGE);

        MessageDefinition timestampMessageDefinition = TimestampProto.createMessageDefinition();
        schemaBuilder.addMessageDefinition(timestampMessageDefinition);

        MessageDefinition messageDefinition = KafkaMetadataProto.createMessageDefinition();
        schemaBuilder.addMessageDefinition(messageDefinition);

        if (!kafkaMetadataColumnName.isEmpty()) {
            MessageDefinition kafkaNestedMetadataProtoMessageDefinition = NestedKafkaMetadataProto
                    .createMessageDefinition(
                            kafkaMetadataColumnName,
                            KafkaMetadataProto.getTypeName(),
                            messageDefinition);
            schemaBuilder.addMessageDefinition(kafkaNestedMetadataProtoMessageDefinition);
        }

        DynamicSchema schema;
        try {
            schema = schemaBuilder.build();
        } catch (Descriptors.DescriptorValidationException e) {
            throw new RuntimeException(e);
        }
        return schema;
    }
}
