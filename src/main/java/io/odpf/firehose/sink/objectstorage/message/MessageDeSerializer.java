package io.odpf.firehose.sink.objectstorage.message;

import com.gojek.de.stencil.client.StencilClient;
import com.gojek.de.stencil.parser.Parser;
import com.gojek.de.stencil.parser.ProtoParser;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import io.odpf.firehose.config.ObjectStorageSinkConfig;
import io.odpf.firehose.consumer.Message;
import io.odpf.firehose.exception.DeserializerException;
import io.odpf.firehose.sink.common.ProtoUtils;
import io.odpf.firehose.sink.exception.EmptyMessageException;
import io.odpf.firehose.sink.exception.UnknownFieldsException;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class MessageDeSerializer {

    private final KafkaMetadataUtils metadataUtils;
    private final Parser protoParser;
    private final ObjectStorageSinkConfig sinkConfig;

    public MessageDeSerializer(ObjectStorageSinkConfig sinkConfig, StencilClient stencilClient) {
        this.sinkConfig = sinkConfig;
        this.protoParser = new ProtoParser(stencilClient, sinkConfig.getInputSchemaProtoClass());
        this.metadataUtils = new KafkaMetadataUtils(sinkConfig.getKafkaMetadataColumnName());

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

            DynamicMessage kafkaMetadata = null;
            if (sinkConfig.getWriteKafkaMetadata()) {
                kafkaMetadata = metadataUtils.createKafkaMetadata(message);
            }
            return new Record(dynamicMessage, kafkaMetadata);
        } catch (InvalidProtocolBufferException e) {
            throw new DeserializerException("failed to parse message", e);
        }
    }
}
