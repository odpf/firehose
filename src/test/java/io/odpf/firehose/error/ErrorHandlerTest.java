package io.odpf.firehose.error;

import io.odpf.firehose.config.ErrorConfig;
import io.odpf.firehose.consumer.Message;
import org.aeonbits.owner.ConfigFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ErrorHandlerTest {

    @Mock
    private Message message;


    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);

    }

    @Test
    public void shouldFilterMessageIfErrorInfoIsNull() {
        ErrorHandler handler = new ErrorHandler(ConfigFactory.create(ErrorConfig.class, new HashMap<String, String>() {{
            put("ERROR_TYPES_FOR_DLQ", ErrorType.DESERIALIZATION_ERROR.name() + "," + ErrorType.SINK_UNKNOWN_ERROR.name());
            put("ERROR_TYPES_FOR_RETRY", ErrorType.DESERIALIZATION_ERROR.name());
            put("ERROR_TYPES_FOR_FAILING", ErrorType.DESERIALIZATION_ERROR.name());
        }}));
        Mockito.when(message.getErrorInfo()).thenReturn(null);

        Assert.assertTrue(handler.filter(message, ErrorScope.RETRY));
        Assert.assertFalse(handler.filter(message, ErrorScope.DLQ));
        Assert.assertFalse(handler.filter(message, ErrorScope.FAIL));
    }

    @Test
    public void shouldFilterMessage() {
        ErrorHandler handler = new ErrorHandler(ConfigFactory.create(ErrorConfig.class, new HashMap<String, String>() {{
            put("ERROR_TYPES_FOR_DLQ", ErrorType.DESERIALIZATION_ERROR.name() + "," + ErrorType.SINK_UNKNOWN_ERROR.name());
            put("ERROR_TYPES_FOR_RETRY", ErrorType.DESERIALIZATION_ERROR.name());
            put("ERROR_TYPES_FOR_FAILING", ErrorType.DESERIALIZATION_ERROR.name());
        }}));
        Mockito.when(message.getErrorInfo()).thenReturn(new ErrorInfo(new RuntimeException(), ErrorType.DESERIALIZATION_ERROR));
        Assert.assertTrue(handler.filter(message, ErrorScope.RETRY));
        Assert.assertTrue(handler.filter(message, ErrorScope.DLQ));
        Assert.assertTrue(handler.filter(message, ErrorScope.FAIL));

        Mockito.when(message.getErrorInfo()).thenReturn(new ErrorInfo(new RuntimeException(), ErrorType.SINK_UNKNOWN_ERROR));
        Assert.assertFalse(handler.filter(message, ErrorScope.RETRY));
        Assert.assertTrue(handler.filter(message, ErrorScope.DLQ));
        Assert.assertFalse(handler.filter(message, ErrorScope.FAIL));
    }

    @Test
    public void shouldSplitBasedOnType() {
        ErrorHandler handler = new ErrorHandler(ConfigFactory.create(ErrorConfig.class, new HashMap<String, String>() {{
            put("ERROR_TYPES_FOR_DLQ", ErrorType.DESERIALIZATION_ERROR.name());
        }}));

        Message m1 = Mockito.mock(Message.class);
        Message m2 = Mockito.mock(Message.class);
        Message m3 = Mockito.mock(Message.class);
        Message m4 = Mockito.mock(Message.class);
        Message m5 = Mockito.mock(Message.class);

        Mockito.when(m1.getErrorInfo()).thenReturn(new ErrorInfo(new RuntimeException(), ErrorType.DESERIALIZATION_ERROR));
        Mockito.when(m3.getErrorInfo()).thenReturn(new ErrorInfo(new RuntimeException(), ErrorType.DESERIALIZATION_ERROR));
        Mockito.when(m2.getErrorInfo()).thenReturn(new ErrorInfo(new RuntimeException(), ErrorType.SINK_UNKNOWN_ERROR));
        Mockito.when(m4.getErrorInfo()).thenReturn(new ErrorInfo(new RuntimeException(), ErrorType.SINK_UNKNOWN_ERROR));
        Mockito.when(m5.getErrorInfo()).thenReturn(new ErrorInfo(new RuntimeException(), ErrorType.DESERIALIZATION_ERROR));

        Map<Boolean, List<Message>> split = handler.split(new ArrayList<Message>() {{
            add(m1);
            add(m2);
            add(m3);
            add(m4);
            add(m5);
        }}, ErrorScope.DLQ);

        Assert.assertEquals(3, split.get(Boolean.TRUE).size());
        Assert.assertEquals(2, split.get(Boolean.FALSE).size());
        Assert.assertTrue(split.get(Boolean.TRUE).containsAll(new ArrayList<Message>() {{
            add(m1);
            add(m3);
            add(m5);
        }}));

        Assert.assertTrue(split.get(Boolean.FALSE).containsAll(new ArrayList<Message>() {{
            add(m2);
            add(m4);
        }}));
    }

}
