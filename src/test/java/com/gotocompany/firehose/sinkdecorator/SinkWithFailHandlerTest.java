package com.gotocompany.firehose.sinkdecorator;

import com.gotocompany.firehose.config.ErrorConfig;
import com.gotocompany.firehose.error.ErrorHandler;
import com.gotocompany.firehose.exception.SinkException;
import com.gotocompany.firehose.message.Message;
import com.gotocompany.firehose.sink.Sink;
import com.gotocompany.depot.error.ErrorInfo;
import com.gotocompany.depot.error.ErrorType;
import org.aeonbits.owner.ConfigFactory;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class SinkWithFailHandlerTest {

    @Mock
    private Sink sink;


    @Test(expected = SinkException.class)
    public void shouldThrowIOExceptionWhenMessageContainsConfiguredError() throws IOException {
        ErrorHandler errorHandler = new ErrorHandler(ConfigFactory.create(ErrorConfig.class, new HashMap<String, String>() {{
            put("ERROR_TYPES_FOR_FAILING", ErrorType.DESERIALIZATION_ERROR.name());
        }}));
        List<Message> messages = new LinkedList<>();
        messages.add(new Message("".getBytes(), "".getBytes(), "basic", 1, 1,
                null, 0, 0,
                new ErrorInfo(new RuntimeException(), ErrorType.DESERIALIZATION_ERROR)));

        when(sink.pushMessage(anyList())).thenReturn(messages);

        SinkWithFailHandler sinkWithFailHandler = new SinkWithFailHandler(sink, errorHandler);
        sinkWithFailHandler.pushMessage(messages);
    }

    @Test
    public void shouldNotThrowIOExceptionWhenConfigIsNotSet() throws IOException {
        ErrorConfig config = ConfigFactory.create(ErrorConfig.class, new HashMap<String, String>());
        config.setProperty("ERROR_TYPES_FOR_FAILING", "");
        ErrorHandler errorHandler = new ErrorHandler(config);

        List<Message> messages = new LinkedList<>();
        messages.add(new Message("".getBytes(), "".getBytes(), "basic", 1, 1,
                null, 0, 0,
                new ErrorInfo(new RuntimeException(), ErrorType.DESERIALIZATION_ERROR)));

        when(sink.pushMessage(anyList())).thenReturn(messages);

        SinkWithFailHandler sinkWithFailHandler = new SinkWithFailHandler(sink, errorHandler);
        Assert.assertEquals(messages, sinkWithFailHandler.pushMessage(messages));
    }
}
