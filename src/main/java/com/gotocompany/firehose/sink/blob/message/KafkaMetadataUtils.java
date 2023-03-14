package com.gotocompany.firehose.sink.blob.message;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.gotocompany.firehose.message.Message;
import com.gotocompany.firehose.sink.blob.proto.KafkaMetadataProtoMessage;
import com.gotocompany.firehose.sink.blob.proto.NestedKafkaMetadataProtoMessage;

import java.time.Instant;

/**
 *  KafkaMetadataUtils utility class for creating kafka metadata {@link com.google.protobuf.DynamicMessage DynamicMessage} from {@link Message}.
 */
public class KafkaMetadataUtils {

    public static DynamicMessage createKafkaMetadata(Descriptors.FileDescriptor kafkaMetadataFileDescriptor, Message message, String kafkaMetadataColumnName) {
        Descriptors.Descriptor metadataDescriptor = kafkaMetadataFileDescriptor.findMessageTypeByName(KafkaMetadataProtoMessage.getTypeName());

        Instant loadTime = Instant.now();
        Instant messageTimestamp = Instant.ofEpochMilli(message.getTimestamp());

        KafkaMetadataProtoMessage.MessageBuilder messageBuilder = KafkaMetadataProtoMessage.newBuilder(metadataDescriptor)
                .setLoadTime(loadTime)
                .setMessageTimestamp(messageTimestamp)
                .setOffset(message.getOffset())
                .setPartition(message.getPartition())
                .setTopic(message.getTopic());

        DynamicMessage metadata = messageBuilder.build();

        if (kafkaMetadataColumnName.isEmpty()) {
            return metadata;
        }

        Descriptors.Descriptor nestedMetadataDescriptor = kafkaMetadataFileDescriptor.findMessageTypeByName(NestedKafkaMetadataProtoMessage.getTypeName());

        return NestedKafkaMetadataProtoMessage.newMessageBuilder(nestedMetadataDescriptor)
                .setMetadata(metadata)
                .setMetadataColumnName(kafkaMetadataColumnName).build();
    }
}
