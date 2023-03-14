package com.gotocompany.firehose.sink;

import com.gotocompany.depot.SinkResponse;
import com.gotocompany.depot.error.ErrorInfo;
import com.gotocompany.depot.error.ErrorType;
import com.gotocompany.firehose.message.Message;
import com.gotocompany.firehose.metrics.FirehoseInstrumentation;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class GenericSinkTest {

    private FirehoseInstrumentation instrumentation;
    private com.gotocompany.depot.Sink depotSink;

    @Before
    public void setup() {
        instrumentation = Mockito.mock(FirehoseInstrumentation.class);
        depotSink = Mockito.mock(com.gotocompany.depot.Sink.class);
    }

    @Test
    public void shouldReturnEmptyListOfMessagesWhenSuccess() throws Exception {
        GenericSink sink = new GenericSink(instrumentation, "test", this.depotSink);
        Mockito.when(this.depotSink.pushToSink(Mockito.anyList())).thenReturn(new SinkResponse());
        List<Message> messages = new ArrayList<Message>() {{
            Message m1 = new Message(new byte[1], new byte[1], "test", 1, 1);
            Message m2 = new Message(new byte[1], new byte[1], "test", 1, 2);
            Message m3 = new Message(new byte[1], new byte[1], "test", 1, 3);
            Message m4 = new Message(new byte[1], new byte[1], "test", 1, 4);
            Message m5 = new Message(new byte[1], new byte[1], "test", 1, 5);
            Message m6 = new Message(new byte[1], new byte[1], "test", 1, 6);
            add(m1);
            add(m2);
            add(m3);
            add(m4);
            add(m5);
            add(m6);
        }};
        sink.prepare(messages);
        List<Message> failedMessages = sink.execute();
        Assert.assertEquals(Collections.emptyList(), failedMessages);
    }

    @Test
    public void shouldReturnFailedMessages() throws Exception {
        GenericSink sink = new GenericSink(instrumentation, "test", this.depotSink);
        SinkResponse response = new SinkResponse();
        response.addErrors(5, new ErrorInfo(new Exception(), ErrorType.SINK_4XX_ERROR));
        response.addErrors(2, new ErrorInfo(new Exception(), ErrorType.DEFAULT_ERROR));
        response.addErrors(4, new ErrorInfo(new Exception(), ErrorType.DESERIALIZATION_ERROR));
        Mockito.when(this.depotSink.pushToSink(Mockito.anyList())).thenReturn(response);
        Message m1 = new Message(new byte[1], new byte[1], "test", 1, 1);
        Message m2 = new Message(new byte[1], new byte[1], "test", 1, 2);
        Message m3 = new Message(new byte[1], new byte[1], "test", 1, 3);
        Message m4 = new Message(new byte[1], new byte[1], "test", 1, 4);
        Message m5 = new Message(new byte[1], new byte[1], "test", 1, 5);
        Message m6 = new Message(new byte[1], new byte[1], "test", 1, 6);
        List<Message> messages = new ArrayList<Message>() {{
            add(m1);
            add(m2);
            add(m3);
            add(m4);
            add(m5);
            add(m6);
        }};
        sink.prepare(messages);
        List<Message> failedMessages = sink.execute();
        Assert.assertEquals(3, failedMessages.size());
        Assert.assertEquals(ErrorType.DEFAULT_ERROR, failedMessages.get(0).getErrorInfo().getErrorType());
        Assert.assertEquals(ErrorType.DESERIALIZATION_ERROR, failedMessages.get(1).getErrorInfo().getErrorType());
        Assert.assertEquals(ErrorType.SINK_4XX_ERROR, failedMessages.get(2).getErrorInfo().getErrorType());

        Assert.assertEquals(3, failedMessages.get(0).getOffset());
        Assert.assertEquals(5, failedMessages.get(1).getOffset());
        Assert.assertEquals(6, failedMessages.get(2).getOffset());
    }
}
