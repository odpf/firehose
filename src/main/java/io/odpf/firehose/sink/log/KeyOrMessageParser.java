package io.odpf.firehose.sink.log;


import io.odpf.firehose.config.AppConfig;
import io.odpf.firehose.consumer.Message;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import io.odpf.stencil.parser.ProtoParser;
import lombok.AllArgsConstructor;

import java.io.IOException;

/**
 * Parser for Key or message.
 */
@AllArgsConstructor
public class KeyOrMessageParser {

    private ProtoParser protoParser;
    private AppConfig appConfig;

    /**
     * Parse dynamic message.
     *
     * @param message the message
     * @return the dynamic message
     * @throws IOException when invalid message is encountered
     */
    public DynamicMessage parse(Message message) throws IOException {
        if (appConfig.getKafkaRecordParserMode().equals("key")) {
            return protoParse(message.getLogKey());
        }
        return protoParse(message.getLogMessage());
    }

    private DynamicMessage protoParse(byte[] data) throws IOException {
        try {
            return protoParser.parse(data);
        } catch (InvalidProtocolBufferException e) {
            throw new IOException(e);
        }
    }
}
