package io.odpf.firehose.sinkdecorator;

import io.odpf.firehose.config.DlqConfig;
import io.odpf.firehose.config.ErrorConfig;
import io.odpf.firehose.error.ErrorInfo;
import io.odpf.firehose.error.ErrorType;
import io.odpf.firehose.consumer.Message;
import io.odpf.firehose.error.ErrorHandler;
import io.odpf.firehose.metrics.Instrumentation;
import io.odpf.firehose.metrics.Metrics;
import io.odpf.firehose.sinkdecorator.dlq.DlqWriter;
import org.aeonbits.owner.ConfigFactory;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import static io.odpf.firehose.metrics.Metrics.DLQ_MESSAGES_TOTAL;
import static io.odpf.firehose.metrics.Metrics.DLQ_RETRY_TOTAL;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;

public class SinkWithDlqTest {

    @Mock
    private BackOffProvider backOffProvider;

    @Mock
    private SinkWithRetry sinkWithRetry;

    @Mock
    private Message message;

    @Mock
    private Instrumentation instrumentation;

    @Mock
    private DlqWriter dlqWriter;

    @Mock
    private DlqConfig dlqConfig;

    private ErrorHandler errorHandler;

    @Before
    public void setup() {
        initMocks(this);
        when(dlqConfig.getDlqRetryMaxAttempts()).thenReturn(10);
        when(dlqConfig.getDlqRetryFailAfterMaxAttemptEnable()).thenReturn(true);
        errorHandler = new ErrorHandler(ConfigFactory.create(ErrorConfig.class, new HashMap<String, String>() {{
            put("ERROR_TYPES_FOR_DLQ", ErrorType.DESERIALIZATION_ERROR.name());
        }}));
    }

    @Test
    public void shouldWriteToDLQWriter() throws Exception {
        when(dlqWriter.write(anyList())).thenReturn(new LinkedList<>());
        ArrayList<Message> messages = new ArrayList<>();
        messages.add(message);
        messages.add(message);
        when(message.getErrorInfo()).thenReturn(new ErrorInfo(new RuntimeException(), ErrorType.DESERIALIZATION_ERROR));
        when(sinkWithRetry.pushMessage(anyList())).thenReturn(messages);

        SinkWithDlq sinkWithDlq = new SinkWithDlq(sinkWithRetry, dlqWriter, backOffProvider, dlqConfig, errorHandler, instrumentation);

        List<Message> pushResult = sinkWithDlq.pushMessage(messages);
        verify(dlqWriter, times(1)).write(messages);
        assertEquals(0, pushResult.size());
        verify(instrumentation, times(2)).captureMessageMetrics(DLQ_MESSAGES_TOTAL, Metrics.MessageType.TOTAL, ErrorType.DESERIALIZATION_ERROR, 1);
        verify(instrumentation, times(1)).captureMessageMetrics(DLQ_MESSAGES_TOTAL, Metrics.MessageType.SUCCESS, 2);
        verify(instrumentation, times(1)).incrementCounter(DLQ_RETRY_TOTAL);
        verify(instrumentation, times(1)).captureGlobalMessageMetrics(Metrics.MessageScope.DLQ, 2);
    }

    @Test
    public void shouldNotWriteToDLQWhenDlqMessagesIsEmpty() throws IOException {
        ArrayList<Message> messages = new ArrayList<>();
        when(sinkWithRetry.pushMessage(anyList())).thenReturn(messages);

        SinkWithDlq sinkWithDlq = new SinkWithDlq(sinkWithRetry, dlqWriter, backOffProvider, dlqConfig, errorHandler, instrumentation);

        sinkWithDlq.pushMessage(messages);
        verify(dlqWriter, never()).write(messages);
    }

    @Test(expected = IOException.class)
    public void shouldThrowIOExceptionWhenWriterThrowIOException() throws IOException {
        when(dlqWriter.write(anyList())).thenThrow(new IOException());
        ArrayList<Message> messages = new ArrayList<>();
        messages.add(message);
        messages.add(message);
        when(message.getErrorInfo()).thenReturn(new ErrorInfo(new RuntimeException(), ErrorType.DESERIALIZATION_ERROR));
        when(sinkWithRetry.pushMessage(anyList())).thenReturn(messages);

        SinkWithDlq sinkWithDlq = new SinkWithDlq(sinkWithRetry, dlqWriter, backOffProvider, dlqConfig, errorHandler, instrumentation);

        sinkWithDlq.pushMessage(messages);
    }

    @Test
    public void shouldRetryWriteMessagesToDlqUntilRetryMessagesEmpty() throws IOException {
        Message messageWithError = new Message(this.message, new ErrorInfo(new IOException(), ErrorType.DESERIALIZATION_ERROR));
        ArrayList<Message> messages = new ArrayList<>();
        messages.add(messageWithError);
        messages.add(messageWithError);

        List<Message> dlqRetryMessages = new LinkedList<>();
        dlqRetryMessages.add(messageWithError);

        when(sinkWithRetry.pushMessage(messages)).thenReturn(messages);
        when(dlqWriter.write(messages)).thenReturn(dlqRetryMessages);
        when(dlqWriter.write(dlqRetryMessages)).thenReturn(new ArrayList<>());

        SinkWithDlq sinkWithDlq = new SinkWithDlq(sinkWithRetry, dlqWriter, backOffProvider, dlqConfig, errorHandler, instrumentation);

        sinkWithDlq.pushMessage(messages);

        verify(dlqWriter, times(1)).write(messages);
        verify(dlqWriter, times(1)).write(dlqRetryMessages);
        verify(instrumentation, times(1)).captureDLQErrors(any(), any());

        verify(instrumentation, times(2)).captureMessageMetrics(DLQ_MESSAGES_TOTAL, Metrics.MessageType.TOTAL, ErrorType.DESERIALIZATION_ERROR, 1);
        verify(instrumentation, times(1)).captureMessageMetrics(DLQ_MESSAGES_TOTAL, Metrics.MessageType.SUCCESS, 2);
        verify(instrumentation, times(2)).incrementCounter(DLQ_RETRY_TOTAL);
        verify(instrumentation, times(1)).captureGlobalMessageMetrics(Metrics.MessageScope.DLQ, 2);
    }

    @Test(expected = IOException.class)
    public void shouldThrowIOExceptionWhenExceedMaxRetryAttemptsButButHasFailedToBeDlqProcessedMessages() throws IOException {
        int currentMaxRetryAttempts = 5;
        when(dlqConfig.getDlqRetryMaxAttempts()).thenReturn(currentMaxRetryAttempts);
        Message messageWithError = new Message(this.message, new ErrorInfo(new IOException(), ErrorType.DESERIALIZATION_ERROR));
        ArrayList<Message> messages = new ArrayList<>();
        messages.add(messageWithError);
        messages.add(messageWithError);

        List<Message> dlqRetryMessages = new LinkedList<>();
        dlqRetryMessages.add(messageWithError);

        when(sinkWithRetry.pushMessage(messages)).thenReturn(messages);
        when(dlqWriter.write(messages)).thenReturn(dlqRetryMessages);
        when(dlqWriter.write(dlqRetryMessages)).thenReturn(dlqRetryMessages);
        when(dlqWriter.write(dlqRetryMessages)).thenReturn(dlqRetryMessages);
        when(dlqWriter.write(dlqRetryMessages)).thenReturn(dlqRetryMessages);
        when(dlqWriter.write(dlqRetryMessages)).thenReturn(dlqRetryMessages);
        when(dlqWriter.write(dlqRetryMessages)).thenReturn(dlqRetryMessages);

        SinkWithDlq sinkWithDlq = new SinkWithDlq(sinkWithRetry, dlqWriter, backOffProvider, dlqConfig, errorHandler, instrumentation);

        sinkWithDlq.pushMessage(messages);
    }

    @Test
    public void shouldNotThrowIOExceptionWhenFailOnMaxRetryAttemptDisabled() throws IOException {
        when(dlqConfig.getDlqRetryMaxAttempts()).thenReturn(2);
        when(dlqConfig.getDlqRetryFailAfterMaxAttemptEnable()).thenReturn(false);
        Message messageWithError = new Message(message, new ErrorInfo(new IOException(), ErrorType.SINK_UNKNOWN_ERROR));
        ArrayList<Message> messages = new ArrayList<>();
        messages.add(messageWithError);
        messages.add(messageWithError);

        List<Message> dlqRetryMessages = new LinkedList<>();
        dlqRetryMessages.add(messageWithError);

        when(sinkWithRetry.pushMessage(messages)).thenReturn(messages);
        when(dlqWriter.write(messages)).thenReturn(dlqRetryMessages);
        when(dlqWriter.write(dlqRetryMessages)).thenReturn(dlqRetryMessages);
        when(dlqWriter.write(dlqRetryMessages)).thenReturn(dlqRetryMessages);

        SinkWithDlq sinkWithDlq = new SinkWithDlq(sinkWithRetry, dlqWriter, backOffProvider, dlqConfig, errorHandler, instrumentation);
        sinkWithDlq.pushMessage(messages);
    }

    @Test
    public void shouldCommitOffsetsOfDlqMessagesWhenSinkManageOffset() throws IOException {
        long timestamp = Instant.parse("2020-01-01T00:00:00Z").toEpochMilli();
        Message message1 = new Message("123".getBytes(), "abc".getBytes(), "booking", 1, 1, null, 0, timestamp, new ErrorInfo(new IOException(), ErrorType.DESERIALIZATION_ERROR));
        Message message2 = new Message("123".getBytes(), "abc".getBytes(), "booking", 1, 2, null, 0, timestamp, new ErrorInfo(new IOException(), ErrorType.DESERIALIZATION_ERROR));
        Message message3 = new Message("123".getBytes(), "abc".getBytes(), "booking", 1, 3, null, 0, timestamp, new ErrorInfo(new IOException(), ErrorType.DESERIALIZATION_ERROR));

        ArrayList<Message> messages = new ArrayList<>();
        messages.add(message1);
        messages.add(message2);
        messages.add(message3);

        ArrayList<Message> dlqProcessedMessages = new ArrayList<>();
        dlqProcessedMessages.add(message2);
        dlqProcessedMessages.add(message3);

        when(sinkWithRetry.canManageOffsets()).thenReturn(true);
        when(sinkWithRetry.pushMessage(messages)).thenReturn(dlqProcessedMessages);
        when(dlqWriter.write(anyList())).thenReturn(new LinkedList<>());

        SinkWithDlq sinkWithDlq = new SinkWithDlq(sinkWithRetry, dlqWriter, backOffProvider, dlqConfig, errorHandler, instrumentation);
        List<Message> pushResult = sinkWithDlq.pushMessage(messages);

        ArgumentCaptor<List<Message>> argumentCaptor = ArgumentCaptor.forClass(List.class);
        verify(sinkWithRetry, times(1)).addOffsets(eq(SinkWithDlq.DLQ_BATCH_KEY), argumentCaptor.capture());
        List<Message> value = argumentCaptor.getValue();
        assertThat(value, Matchers.containsInAnyOrder(dlqProcessedMessages.toArray()));

        verify(sinkWithRetry, times(1)).setCommittable(SinkWithDlq.DLQ_BATCH_KEY);
        assertEquals(0, pushResult.size());
    }

    @Test
    public void shouldNotRegisterAndCommitOffsetWhenNoMessagesIsProcessedByDLQ() throws IOException {
        when(dlqWriter.write(anyList())).thenReturn(new LinkedList<>());
        ArrayList<Message> messages = new ArrayList<>();
        when(sinkWithRetry.pushMessage(anyList())).thenReturn(messages);

        SinkWithDlq sinkWithDlq = new SinkWithDlq(sinkWithRetry, dlqWriter, backOffProvider, dlqConfig, errorHandler, instrumentation);

        sinkWithDlq.pushMessage(messages);
        verify(sinkWithRetry, never()).addOffsets(anyString(), anyList());
        verify(sinkWithRetry, never()).setCommittable(anyString());
    }

    @Test
    public void shouldWriteDlqMessagesWhenErrorTypesConfigured() throws IOException {
        Message messageWithError = new Message(this.message, new ErrorInfo(new IOException(), ErrorType.DESERIALIZATION_ERROR));
        when(dlqWriter.write(anyList())).thenReturn(new LinkedList<>());
        ArrayList<Message> messages = new ArrayList<>();
        messages.add(messageWithError);
        messages.add(new Message(message, new ErrorInfo(null, ErrorType.SINK_UNKNOWN_ERROR)));
        when(sinkWithRetry.pushMessage(anyList())).thenReturn(messages);
        SinkWithDlq sinkWithDlq = new SinkWithDlq(sinkWithRetry, dlqWriter, backOffProvider, dlqConfig, errorHandler, instrumentation);

        List<Message> pushResult = sinkWithDlq.pushMessage(messages);
        ArgumentCaptor<List<Message>> argumentCaptor = ArgumentCaptor.forClass(List.class);
        verify(dlqWriter, times(1)).write(argumentCaptor.capture());
        assertEquals(1, argumentCaptor.getValue().size());
        assertThat(argumentCaptor.getValue(), Matchers.contains(messageWithError));
        assertEquals(1, pushResult.size());
    }

    @Test
    public void shouldInstrumentFailure() throws Exception {
        ArrayList<Message> messages = new ArrayList<>();
        messages.add(message);
        messages.add(message);
        when(dlqConfig.getDlqRetryFailAfterMaxAttemptEnable()).thenReturn(false);
        when(message.getErrorInfo()).thenReturn(new ErrorInfo(new RuntimeException(), ErrorType.DESERIALIZATION_ERROR));
        when(sinkWithRetry.pushMessage(anyList())).thenReturn(messages);
        when(dlqWriter.write(anyList())).thenReturn(messages);

        SinkWithDlq sinkWithDlq = new SinkWithDlq(sinkWithRetry, dlqWriter, backOffProvider, dlqConfig, errorHandler, instrumentation);

        List<Message> pushResult = sinkWithDlq.pushMessage(messages);
        verify(dlqWriter, times(10)).write(messages);
        assertEquals(2, pushResult.size());
        verify(instrumentation, times(2)).captureMessageMetrics(DLQ_MESSAGES_TOTAL, Metrics.MessageType.TOTAL, ErrorType.DESERIALIZATION_ERROR, 1);
        verify(instrumentation, times(1)).captureMessageMetrics(DLQ_MESSAGES_TOTAL, Metrics.MessageType.SUCCESS, 0);
        verify(instrumentation, times(2)).captureMessageMetrics(DLQ_MESSAGES_TOTAL, Metrics.MessageType.FAILURE, ErrorType.DESERIALIZATION_ERROR, 1);
        verify(instrumentation, times(10)).incrementCounter(DLQ_RETRY_TOTAL);
        verify(instrumentation, times(1)).captureGlobalMessageMetrics(Metrics.MessageScope.DLQ, 0);
    }
}
