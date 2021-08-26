package io.odpf.firehose.sink.log;


import io.odpf.firehose.config.AppConfig;
import io.odpf.firehose.consumer.Message;
import io.odpf.firehose.consumer.TestMessage;
import com.google.protobuf.DynamicMessage;
import io.odpf.stencil.parser.ProtoParser;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;

@RunWith(MockitoJUnitRunner.class)
public class KeyOrMessageParserTest {

    @Mock
    private AppConfig appConfig;

    @Mock
    private ProtoParser protoParser;

    private DynamicMessage dynamicMessage;

    private Message message;

    private KeyOrMessageParser parser;

    @Before
    public void setup() throws IOException {
        dynamicMessage = DynamicMessage.newBuilder(TestMessage.getDescriptor()).build();

        Mockito.when(appConfig.getKafkaRecordParserMode()).thenReturn("message");
        Mockito.when(protoParser.parse(Mockito.any(byte[].class))).thenReturn(dynamicMessage);

        message = new Message("logKey".getBytes(), "logMessage".getBytes(), "topic", 0, 10);
        parser = new KeyOrMessageParser(protoParser, appConfig);
    }

    @Test
    public void shouldParseMessageByDefault() throws IOException {
        parser.parse(message);

        Mockito.verify(protoParser, Mockito.times(1)).parse("logMessage".getBytes());
    }

    @Test
    public void shouldParseKeyWhenKafkaMessageParserModeSetToKey() throws IOException {
        Mockito.when(appConfig.getKafkaRecordParserMode()).thenReturn("key");
        parser.parse(message);

        Mockito.verify(protoParser, Mockito.times(1)).parse("logKey".getBytes());
    }

}
