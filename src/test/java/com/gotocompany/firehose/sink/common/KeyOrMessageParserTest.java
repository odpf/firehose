package com.gotocompany.firehose.sink.common;


import com.gotocompany.firehose.config.AppConfig;
import com.gotocompany.firehose.message.Message;
import com.gotocompany.firehose.consumer.TestMessage;
import com.google.protobuf.DynamicMessage;
import com.gotocompany.stencil.Parser;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;

@RunWith(MockitoJUnitRunner.class)
public class KeyOrMessageParserTest {

    @Mock
    private AppConfig appConfig;

    @Mock
    private Parser protoParser;

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
