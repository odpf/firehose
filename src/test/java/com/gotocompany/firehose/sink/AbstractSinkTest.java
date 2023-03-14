package com.gotocompany.firehose.sink;

import com.gotocompany.depot.error.ErrorInfo;
import com.gotocompany.depot.error.ErrorType;
import com.gotocompany.firehose.message.Message;
import com.gotocompany.firehose.exception.DeserializerException;
import com.gotocompany.firehose.metrics.FirehoseInstrumentation;
import com.gotocompany.firehose.metrics.Metrics;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

import java.io.IOException;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.*;

public class AbstractSinkTest {
    private static class TestSink extends AbstractSink {
        TestSink(FirehoseInstrumentation firehoseInstrumentation, String sinkType) {
            super(firehoseInstrumentation, sinkType);
        }

        private final List<Message> failedMessages = new ArrayList<>();

        private boolean shouldThrowException = false;

        private Exception exception;

        protected List<Message> execute() throws Exception {
            if (exception != null) {
                throw exception;
            }
            return failedMessages;
        }

        @Override
        protected void prepare(List<Message> messages) throws DeserializerException, IOException, SQLException {
            if (shouldThrowException) {
                throw new DeserializerException("test");
            }
        }

        @Override
        public void close() throws IOException {

        }
    }

    @Mock
    private FirehoseInstrumentation firehoseInstrumentation = Mockito.mock(FirehoseInstrumentation.class);

    private Message createMessage(String topic, String key, String value) {
        return new Message(key.getBytes(), value.getBytes(), topic, 0, 0);
    }

    @Test
    public void shouldProcessMessages() {
        when(firehoseInstrumentation.startExecution()).thenReturn(Instant.now());
        TestSink sink = new TestSink(firehoseInstrumentation, "TestSink");
        Message m1 = createMessage("test", "test", "test1");
        Message m2 = createMessage("test", "test", "test2");
        Message m3 = createMessage("test", "test", "test3");
        Message m4 = createMessage("test", "test", "test4");
        List<Message> failedMessages = sink.pushMessage(new ArrayList<Message>() {{
            add(m1);
            add(m2);
            add(m3);
            add(m4);
        }});
        Assert.assertEquals(0, failedMessages.size());
        Mockito.verify(firehoseInstrumentation, Mockito.times(1)).captureMessageMetrics(Metrics.SINK_MESSAGES_TOTAL, Metrics.MessageType.TOTAL, 4);
        Mockito.verify(firehoseInstrumentation, Mockito.times(1)).captureMessageMetrics(Metrics.SINK_MESSAGES_TOTAL, Metrics.MessageType.SUCCESS, 4);
        Mockito.verify(firehoseInstrumentation, Mockito.times(1)).captureGlobalMessageMetrics(Metrics.MessageScope.SINK, 4);
        Mockito.verify(firehoseInstrumentation, Mockito.times(1)).captureMessageBatchSize(4);
        Mockito.verify(firehoseInstrumentation, Mockito.times(1)).logInfo("Preparing {} messages", 4);
        Mockito.verify(firehoseInstrumentation, Mockito.times(1)).capturePreExecutionLatencies(new ArrayList<Message>() {{
            add(m1);
            add(m2);
            add(m3);
            add(m4);
        }});
        Mockito.verify(firehoseInstrumentation, Mockito.times(1)).startExecution();
        Mockito.verify(firehoseInstrumentation, Mockito.times(1)).captureSinkExecutionTelemetry("TestSink", 4);
        Mockito.verify(firehoseInstrumentation, Mockito.times(1)).logInfo("Pushed {} messages", 4);
    }

    @Test
    public void shouldProcessFailedMessages() {
        when(firehoseInstrumentation.startExecution()).thenReturn(Instant.now());
        TestSink sink = new TestSink(firehoseInstrumentation, "TestSink");
        Message m1 = createMessage("test", "test", "test1");
        Message m2 = createMessage("test", "test", "test2");
        Message m3 = createMessage("test", "test", "test3");
        Message m4 = createMessage("test", "test", "test4");
        Message m5 = createMessage("test", "test", "test5");
        m5.setErrorInfo(new ErrorInfo(null, ErrorType.DESERIALIZATION_ERROR));
        sink.failedMessages.add(m2);
        sink.failedMessages.add(m4);
        sink.failedMessages.add(m5);
        List<Message> failedMessages = sink.pushMessage(new ArrayList<Message>() {{
            add(m1);
            add(m2);
            add(m3);
            add(m4);
            add(m5);
        }});
        Assert.assertEquals(3, failedMessages.size());
        Mockito.verify(firehoseInstrumentation, Mockito.times(1)).captureMessageMetrics(Metrics.SINK_MESSAGES_TOTAL, Metrics.MessageType.TOTAL, 5);
        Mockito.verify(firehoseInstrumentation, Mockito.times(1)).captureMessageMetrics(Metrics.SINK_MESSAGES_TOTAL, Metrics.MessageType.SUCCESS, 2);
        Mockito.verify(firehoseInstrumentation, Mockito.times(2)).captureMessageMetrics(Metrics.SINK_MESSAGES_TOTAL, Metrics.MessageType.FAILURE, ErrorType.DEFAULT_ERROR, 1);
        Mockito.verify(firehoseInstrumentation, Mockito.times(1)).captureMessageMetrics(Metrics.SINK_MESSAGES_TOTAL, Metrics.MessageType.FAILURE, ErrorType.DESERIALIZATION_ERROR, 1);
        Mockito.verify(firehoseInstrumentation, Mockito.times(1)).captureGlobalMessageMetrics(Metrics.MessageScope.SINK, 2);
        Mockito.verify(firehoseInstrumentation, Mockito.times(1)).captureMessageBatchSize(5);
        Mockito.verify(firehoseInstrumentation, Mockito.times(1)).logInfo("Preparing {} messages", 5);
        Mockito.verify(firehoseInstrumentation, Mockito.times(1)).capturePreExecutionLatencies(new ArrayList<Message>() {{
            add(m1);
            add(m2);
            add(m3);
            add(m4);
            add(m5);
        }});
        Mockito.verify(firehoseInstrumentation, Mockito.times(1)).startExecution();
        Mockito.verify(firehoseInstrumentation, Mockito.times(1)).captureSinkExecutionTelemetry("TestSink", 5);
        Mockito.verify(firehoseInstrumentation, Mockito.times(1)).logInfo("Pushed {} messages", 2);
        Mockito.verify(firehoseInstrumentation, Mockito.times(1)).logError("Failed to Push {} messages to sink ", 3);
        Mockito.verify(firehoseInstrumentation, Mockito.times(1)).captureErrorMetrics(ErrorType.DESERIALIZATION_ERROR);
        Mockito.verify(firehoseInstrumentation, Mockito.times(2)).captureErrorMetrics(ErrorType.DEFAULT_ERROR);
    }

    @Test
    public void shouldProcessException() {
        when(firehoseInstrumentation.startExecution()).thenReturn(Instant.now());
        TestSink sink = new TestSink(firehoseInstrumentation, "TestSink");
        Message m1 = createMessage("test", "test", "test1");
        Message m2 = createMessage("test", "test", "test2");
        Message m3 = createMessage("test", "test", "test3");
        Message m4 = createMessage("test", "test", "test4");
        sink.exception = new Exception();
        List<Message> failedMessages = sink.pushMessage(new ArrayList<Message>() {{
            add(m1);
            add(m2);
            add(m3);
            add(m4);
        }});
        Assert.assertEquals(4, failedMessages.size());

        Mockito.verify(firehoseInstrumentation, Mockito.times(1)).captureMessageMetrics(Metrics.SINK_MESSAGES_TOTAL, Metrics.MessageType.TOTAL, 4);
        Mockito.verify(firehoseInstrumentation, Mockito.times(4)).captureMessageMetrics(Metrics.SINK_MESSAGES_TOTAL, Metrics.MessageType.FAILURE, ErrorType.DEFAULT_ERROR, 1);
        Mockito.verify(firehoseInstrumentation, Mockito.times(1)).captureMessageBatchSize(4);
        Mockito.verify(firehoseInstrumentation, Mockito.times(1)).logInfo("Preparing {} messages", 4);
        Mockito.verify(firehoseInstrumentation, Mockito.times(1)).capturePreExecutionLatencies(new ArrayList<Message>() {{
            add(m1);
            add(m2);
            add(m3);
            add(m4);
        }});
        Mockito.verify(firehoseInstrumentation, Mockito.times(1)).startExecution();
        Mockito.verify(firehoseInstrumentation, Mockito.times(1)).captureSinkExecutionTelemetry("TestSink", 4);
        Mockito.verify(firehoseInstrumentation, Mockito.times(1)).logError("Failed to Push {} messages to sink ", 4);
        Mockito.verify(firehoseInstrumentation, Mockito.times(4)).captureErrorMetrics(ErrorType.DEFAULT_ERROR);
    }

    @Test(expected = DeserializerException.class)
    public void shouldProcessExceptionInPrepare() {
        TestSink sink = new TestSink(firehoseInstrumentation, "TestSink");
        Message m1 = createMessage("test", "test", "test1");
        Message m2 = createMessage("test", "test", "test2");
        Message m3 = createMessage("test", "test", "test3");
        Message m4 = createMessage("test", "test", "test4");
        sink.shouldThrowException = true;
        sink.pushMessage(new ArrayList<Message>() {{
            add(m1);
            add(m2);
            add(m3);
            add(m4);
        }});
    }

    @Test
    public void shouldNotCaptureSinkExecutionTelemetry() {
        TestSink sink = new TestSink(firehoseInstrumentation, "TestSink");
        Message m1 = createMessage("test", "test", "test1");
        Message m2 = createMessage("test", "test", "test2");
        Message m3 = createMessage("test", "test", "test3");
        Message m4 = createMessage("test", "test", "test4");
        sink.shouldThrowException = true;
        try {
            sink.pushMessage(new ArrayList<Message>() {{
            add(m1);
            add(m2);
            add(m3);
            add(m4);
        }});
        } catch (Exception e) {
            Mockito.verify(firehoseInstrumentation, Mockito.times(0)).captureSinkExecutionTelemetry(any(), any());
        }
    }

}
