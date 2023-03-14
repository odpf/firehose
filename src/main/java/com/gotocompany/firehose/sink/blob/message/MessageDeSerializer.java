package com.gotocompany.firehose.sink.blob.message;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.gotocompany.firehose.config.BlobSinkConfig;
import com.gotocompany.firehose.exception.DeserializerException;
import com.gotocompany.firehose.exception.EmptyMessageException;
import com.gotocompany.firehose.exception.UnknownFieldsException;
import com.gotocompany.firehose.message.Message;
import com.gotocompany.firehose.proto.ProtoUtils;
import com.gotocompany.firehose.sink.blob.proto.KafkaMetadataProtoMessageUtils;
import com.gotocompany.stencil.client.StencilClient;
import com.gotocompany.stencil.Parser;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class MessageDeSerializer {

    private final Descriptors.FileDescriptor kafkaMetadataFileDescriptor;
    private final Parser protoParser;
    private final BlobSinkConfig sinkConfig;

    public MessageDeSerializer(BlobSinkConfig sinkConfig, StencilClient stencilClient) {
        this.sinkConfig = sinkConfig;
        this.protoParser = stencilClient.getParser(sinkConfig.getInputSchemaProtoClass());
        this.kafkaMetadataFileDescriptor = KafkaMetadataProtoMessageUtils.createFileDescriptor(sinkConfig.getOutputKafkaMetadataColumnName());
    }

    public Record deSerialize(Message message) throws DeserializerException {
        try {
            if (message.getLogMessage() == null || message.getLogMessage().length == 0) {
                throw new EmptyMessageException();
            }
            DynamicMessage dynamicMessage = protoParser.parse(message.getLogMessage());

            if (!sinkConfig.getInputSchemaProtoAllowUnknownFieldsEnable() && ProtoUtils.hasUnknownField(dynamicMessage)) {
                throw new UnknownFieldsException(dynamicMessage);
            }

            DynamicMessage kafkaMetadata = KafkaMetadataUtils.createKafkaMetadata(kafkaMetadataFileDescriptor, message, sinkConfig.getOutputKafkaMetadataColumnName());
            return new Record(dynamicMessage, kafkaMetadata);
        } catch (InvalidProtocolBufferException e) {
            throw new DeserializerException("failed to parse message", e);
        }
    }
}
