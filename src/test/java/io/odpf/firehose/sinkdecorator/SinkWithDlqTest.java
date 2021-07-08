package io.odpf.firehose.sinkdecorator;

import io.odpf.firehose.consumer.ErrorInfo;
import io.odpf.firehose.consumer.ErrorType;
import io.odpf.firehose.consumer.Message;
import io.odpf.firehose.metrics.Instrumentation;
import io.odpf.firehose.sinkdecorator.dlq.DlqWriter;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

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

    private final int maxRetryAttempts = 10;
    private final boolean isFailOnMaxRetryAttemptsExceeded = true;

    @Before
    public void setup() {
        initMocks(this);
    }

    @Test
    public void shouldWriteToDLQWriter() throws Exception {
        when(dlqWriter.write(anyList())).thenReturn(new LinkedList<>());
        ArrayList<Message> messages = new ArrayList<>();
        messages.add(message);
        messages.add(message);
        when(sinkWithRetry.pushMessage(anyList())).thenReturn(messages);

        SinkWithDlq sinkWithDlq = new SinkWithDlq(sinkWithRetry, dlqWriter, backOffProvider, maxRetryAttempts, isFailOnMaxRetryAttemptsExceeded, instrumentation);

        List<Message> pushResult = sinkWithDlq.pushMessage(messages);
        verify(dlqWriter, times(1)).write(messages);
        assertEquals(0, pushResult.size());
    }

    @Test
    public void shouldNotWriteToDLQWhenDlqMessagesIsEmpty() throws IOException {
        ArrayList<Message> messages = new ArrayList<>();
        when(sinkWithRetry.pushMessage(anyList())).thenReturn(messages);

        SinkWithDlq sinkWithDlq = new SinkWithDlq(sinkWithRetry, dlqWriter, backOffProvider, maxRetryAttempts, isFailOnMaxRetryAttemptsExceeded, instrumentation);

        sinkWithDlq.pushMessage(messages);
        verify(dlqWriter, never()).write(messages);
    }

    @Test(expected = IOException.class)
    public void shouldThrowIOExceptionWhenWriterThrowIOException() throws IOException {
        when(dlqWriter.write(anyList())).thenThrow(new IOException());
        ArrayList<Message> messages = new ArrayList<>();
        messages.add(message);
        messages.add(message);
        when(sinkWithRetry.pushMessage(anyList())).thenReturn(messages);

        SinkWithDlq sinkWithDlq = new SinkWithDlq(sinkWithRetry, dlqWriter, backOffProvider, maxRetryAttempts, isFailOnMaxRetryAttemptsExceeded, instrumentation);

        sinkWithDlq.pushMessage(messages);
    }

    @Test
    public void shouldRetryWriteMessagesToDlqUntilRetryMessagesEmpty() throws IOException {
        Message messageWithError = new Message(this.message, new ErrorInfo(new IOException(), ErrorType.UNKNOWN_ERROR));
        ArrayList<Message> messages = new ArrayList<>();
        messages.add(messageWithError);
        messages.add(messageWithError);

        List<Message> dlqRetryMessages = new LinkedList<>();
        dlqRetryMessages.add(messageWithError);

        when(sinkWithRetry.pushMessage(messages)).thenReturn(messages);
        when(dlqWriter.write(messages)).thenReturn(dlqRetryMessages);
        when(dlqWriter.write(dlqRetryMessages)).thenReturn(new ArrayList<>());

        SinkWithDlq sinkWithDlq = new SinkWithDlq(sinkWithRetry, dlqWriter, backOffProvider, maxRetryAttempts, isFailOnMaxRetryAttemptsExceeded, instrumentation);

        sinkWithDlq.pushMessage(messages);

        verify(dlqWriter, times(1)).write(messages);
        verify(dlqWriter, times(1)).write(dlqRetryMessages);
        verify(instrumentation, times(2)).captureRetryAttempts();
        verify(instrumentation, times(2)).incrementMessageSucceedCount();
        verify(instrumentation, times(1)).incrementMessageFailCount(any(), any());
    }

    @Test(expected = IOException.class)
    public void shouldThrowIOExceptionWhenExceedMaxRetryAttemptsButButHasFailedToBeDlqProcessedMessages() throws IOException {
        int currentMaxRetryAttempts = 5;
        Message messageWithError = new Message(this.message, new ErrorInfo(new IOException(), ErrorType.UNKNOWN_ERROR));
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

        SinkWithDlq sinkWithDlq = new SinkWithDlq(sinkWithRetry, dlqWriter, backOffProvider, currentMaxRetryAttempts, isFailOnMaxRetryAttemptsExceeded, instrumentation);

        sinkWithDlq.pushMessage(messages);
    }

    @Test
    public void shouldNotThrowIOExceptionWhenFailOnMaxRetryAttemptDisabled() throws IOException {
        int currentMaxRetryAttempts = 2;
        Message messageWithError = new Message(message, new ErrorInfo(new IOException(), ErrorType.UNKNOWN_ERROR));
        ArrayList<Message> messages = new ArrayList<>();
        messages.add(messageWithError);
        messages.add(messageWithError);

        List<Message> dlqRetryMessages = new LinkedList<>();
        dlqRetryMessages.add(messageWithError);

        when(sinkWithRetry.pushMessage(messages)).thenReturn(messages);
        when(dlqWriter.write(messages)).thenReturn(dlqRetryMessages);
        when(dlqWriter.write(dlqRetryMessages)).thenReturn(dlqRetryMessages);
        when(dlqWriter.write(dlqRetryMessages)).thenReturn(dlqRetryMessages);

        SinkWithDlq sinkWithDlq = new SinkWithDlq(sinkWithRetry, dlqWriter, backOffProvider, currentMaxRetryAttempts, false, instrumentation);
        sinkWithDlq.pushMessage(messages);
    }

    @Test
    public void shouldCommitOffsetsOfDlqMessagesWhenSinkManageOffset() throws IOException {
        long timestamp = Instant.parse("2020-01-01T00:00:00Z").toEpochMilli();
        Message message1 = new Message("123".getBytes(), "abc".getBytes(), "booking", 1, 1, null, 0, timestamp, new ErrorInfo(new IOException(), ErrorType.UNKNOWN_ERROR));
        Message message2 = new Message("123".getBytes(), "abc".getBytes(), "booking", 1, 2, null, 0, timestamp, new ErrorInfo(new IOException(), ErrorType.UNKNOWN_ERROR));
        Message message3 = new Message("123".getBytes(), "abc".getBytes(), "booking", 1, 3, null, 0, timestamp, new ErrorInfo(new IOException(), ErrorType.UNKNOWN_ERROR));

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

        SinkWithDlq sinkWithDlq = new SinkWithDlq(sinkWithRetry, dlqWriter, backOffProvider, maxRetryAttempts, isFailOnMaxRetryAttemptsExceeded, instrumentation);
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

        SinkWithDlq sinkWithDlq = new SinkWithDlq(sinkWithRetry, dlqWriter, backOffProvider, maxRetryAttempts, isFailOnMaxRetryAttemptsExceeded, instrumentation);

        sinkWithDlq.pushMessage(messages);
        verify(sinkWithRetry, never()).addOffsets(anyString(), anyList());
        verify(sinkWithRetry, never()).setCommittable(anyString());
    }
}
