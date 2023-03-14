package com.gotocompany.firehose.sink.blob.proto;

import com.github.os72.protobuf.dynamic.MessageDefinition;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import lombok.AllArgsConstructor;


/**
 * NestedKafkaMetadataProtoMessage contains schema of kafka metadata proto message nested under a top level field.
 * This class provides {@link com.github.os72.protobuf.dynamic.MessageDefinition} to generate protobuf descriptor and builder of kafka metadata {@link com.google.protobuf.DynamicMessage}.
 * message KafkaNestedOffsetMetadata{
 *     KafkaOffsetMetadata ${kafka_metadata_column_name} = 536870911;
 * }
 *
 */
@AllArgsConstructor
public class NestedKafkaMetadataProtoMessage {
    private static final String NESTED_OFFSET_METADATA_PROTO_NAME = "KafkaNestedOffsetMetadata";
    public static final int METADATA_FIELD_NUMBER = 536870911;

    public static String getTypeName() {
        return NESTED_OFFSET_METADATA_PROTO_NAME;
    }

    public static MessageDefinition createMessageDefinition(String nestedKafkaMetadataColumnName, String kafkaMetadataProtoTypeName, MessageDefinition metadataMessageDefinition) {

        return MessageDefinition.newBuilder(NestedKafkaMetadataProtoMessage.getTypeName())
                .addMessageDefinition(metadataMessageDefinition)
                .addField("optional", kafkaMetadataProtoTypeName, nestedKafkaMetadataColumnName, METADATA_FIELD_NUMBER)
                .build();
    }

    public static MessageBuilder newMessageBuilder(Descriptors.Descriptor descriptor) {
        return new MessageBuilder(descriptor);
    }

    /**
     * Builder of KafkaNestedOffsetMetadata dynamic message.
     */
    public static class MessageBuilder {

        private String metadataColumnName;
        private DynamicMessage metadata;

        private Descriptors.Descriptor descriptor;

        public MessageBuilder(Descriptors.Descriptor descriptor) {
            this.descriptor = descriptor;
        }

        public MessageBuilder setMetadataColumnName(String metadataColumnName) {
            this.metadataColumnName = metadataColumnName;
            return this;
        }

        public MessageBuilder setMetadata(DynamicMessage metadata) {
            this.metadata = metadata;
            return this;
        }

        public DynamicMessage build() {
            return DynamicMessage.newBuilder(descriptor)
                    .setField(descriptor.findFieldByName(metadataColumnName), metadata)
                    .build();
        }
    }
}
