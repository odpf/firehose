package com.gotocompany.firehose.sink.blob.message;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.gotocompany.firehose.sink.blob.proto.KafkaMetadataProtoMessage;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.time.Instant;

@AllArgsConstructor
@Data
public class Record {
    private DynamicMessage message;
    private DynamicMessage metadata;

    public String getTopic(String fieldName) {
        Descriptors.Descriptor metadataDescriptor = metadata.getDescriptorForType();

        if (!fieldName.isEmpty()) {
            DynamicMessage nestedMetadataMessage = (DynamicMessage) metadata.getField(metadataDescriptor.findFieldByName(fieldName));
            Descriptors.Descriptor nestedMetadataMessageDescriptor = nestedMetadataMessage.getDescriptorForType();
            return (String) nestedMetadataMessage.getField(nestedMetadataMessageDescriptor.findFieldByName(KafkaMetadataProtoMessage.MESSAGE_TOPIC_FIELD_NAME));
        }

        return (String) metadata.getField(metadataDescriptor.findFieldByName(KafkaMetadataProtoMessage.MESSAGE_TOPIC_FIELD_NAME));
    }

    public Instant getTimestamp(String fieldName) {
        Descriptors.Descriptor descriptor = message.getDescriptorForType();
        Descriptors.FieldDescriptor timestampField = descriptor.findFieldByName(fieldName);
        DynamicMessage timestamp = (DynamicMessage) message.getField(timestampField);
        long seconds = (long) timestamp.getField(timestamp.getDescriptorForType().findFieldByName("seconds"));
        int nanos = (int) timestamp.getField(timestamp.getDescriptorForType().findFieldByName("nanos"));
        return Instant.ofEpochSecond(seconds, nanos);
    }
}
