package io.odpf.firehose.sink.objectstorage.message;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import io.odpf.firehose.consumer.Message;
import io.odpf.firehose.sink.objectstorage.proto.KafkaMetadataProto;
import io.odpf.firehose.sink.objectstorage.proto.KafkaMetadataProtoFile;
import io.odpf.firehose.sink.objectstorage.proto.NestedKafkaMetadataProto;

import java.time.Instant;
import java.util.List;

public class KafkaMetadataUtils {

    private final Descriptors.FileDescriptor kafkaMetadataFileDescriptor;
    private final String kafkaMetadataColumnName;

    public KafkaMetadataUtils(String kafkaMetadataColumnName) {
        this.kafkaMetadataColumnName = kafkaMetadataColumnName;
        kafkaMetadataFileDescriptor = KafkaMetadataProtoFile.createFileDescriptor(kafkaMetadataColumnName);
    }

    public DynamicMessage createKafkaMetadata(Message message) {
        Descriptors.Descriptor metadataDescriptor = kafkaMetadataFileDescriptor.findMessageTypeByName(KafkaMetadataProto.getTypeName());

        Instant loadTime = Instant.now();
        Instant messageTimestamp = Instant.ofEpochMilli(message.getTimestamp());

        KafkaMetadataProto.MessageBuilder messageBuilder = KafkaMetadataProto.newBuilder(metadataDescriptor)
                .setLoadTime(loadTime)
                .setMessageTimestamp(messageTimestamp)
                .setOffset(message.getOffset())
                .setPartition(message.getPartition())
                .setTopic(message.getTopic());

        DynamicMessage metadata = messageBuilder.build();

        if (kafkaMetadataColumnName.isEmpty()) {
            return metadata;
        }

        Descriptors.Descriptor nestedMetadataDescriptor = kafkaMetadataFileDescriptor.findMessageTypeByName(NestedKafkaMetadataProto.getTypeName());

        return NestedKafkaMetadataProto.newMessageBuilder(nestedMetadataDescriptor)
                .setMetadata(metadata)
                .setMetadataColumnName(kafkaMetadataColumnName).build();
    }

    public List<Descriptors.FieldDescriptor> getFieldDescriptor() {
        return getMetadataDescriptor().getFields();
    }

    public Descriptors.Descriptor getMetadataDescriptor() {
        if (kafkaMetadataColumnName.isEmpty()) {
            return kafkaMetadataFileDescriptor.findMessageTypeByName(KafkaMetadataProto.getTypeName());
        }
        return kafkaMetadataFileDescriptor.findMessageTypeByName(NestedKafkaMetadataProto.getTypeName());
    }
}
