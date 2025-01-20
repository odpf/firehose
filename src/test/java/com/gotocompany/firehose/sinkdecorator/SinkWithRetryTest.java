package com.gotocompany.firehose.sinkdecorator;

import com.gotocompany.firehose.config.AppConfig;
import com.gotocompany.firehose.config.ErrorConfig;
import com.gotocompany.firehose.config.enums.InputSchemaType;
import com.gotocompany.firehose.error.ErrorHandler;
import com.gotocompany.firehose.exception.DeserializerException;
import com.gotocompany.firehose.message.Message;
import com.gotocompany.firehose.metrics.FirehoseInstrumentation;
import com.gotocompany.firehose.metrics.Metrics;
import com.gotocompany.firehose.sink.common.KeyOrMessageParser;
import com.gotocompany.depot.error.ErrorInfo;
import com.gotocompany.depot.error.ErrorType;
import org.aeonbits.owner.ConfigFactory;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.*;

import static com.gotocompany.firehose.metrics.Metrics.RETRY_ATTEMPTS_TOTAL;
import static com.gotocompany.firehose.metrics.Metrics.RETRY_MESSAGES_TOTAL;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;

public class SinkWithRetryTest {

    @Mock
    private SinkDecorator sinkDecorator;

    @Mock
    private BackOffProvider backOffProvider;

    @Mock
    private Message message;

    @Mock
    private FirehoseInstrumentation firehoseInstrumentation;

    @Mock
    private KeyOrMessageParser parser;

    private ErrorHandler errorHandler;

    @Mock
    private AppConfig appConfig;

    @Before
    public void setUp() {
        initMocks(this);
        when(appConfig.getRetryMaxAttempts()).thenReturn(3);
        when(appConfig.getRetryFailAfterMaxAttemptsEnable()).thenReturn(false);
        errorHandler = new ErrorHandler(ConfigFactory.create(ErrorConfig.class, new HashMap<String, String>() {{
            put("ERROR_TYPES_FOR_RETRY", ErrorType.DESERIALIZATION_ERROR.name());
        }}));

    }

    @Test
    public void shouldReturnEmptyListIfSuperReturnsEmptyList() throws IOException, DeserializerException {
        when(sinkDecorator.pushMessage(anyList())).thenReturn(new ArrayList<>());
        SinkWithRetry sinkWithRetry = new SinkWithRetry(sinkDecorator, backOffProvider, firehoseInstrumentation, appConfig, parser, errorHandler);
        List<Message> messages = sinkWithRetry.pushMessage(
                Collections.singletonList(new Message("key".getBytes(), "value".getBytes(), "topic", 1, 1)));

        assertTrue(messages.isEmpty());
        verify(sinkDecorator, Mockito.times(1)).pushMessage(anyList());
    }

    @Test
    public void shouldRetryForNumberOfAttemptsIfSuperReturnsEsbMessages() throws IOException, DeserializerException {
        ArrayList<Message> messages = new ArrayList<>();
        messages.add(message);
        messages.add(message);
        when(message.getErrorInfo()).thenReturn(new ErrorInfo(null, ErrorType.DESERIALIZATION_ERROR));
        when(sinkDecorator.pushMessage(anyList())).thenReturn(messages).thenReturn(messages).thenReturn(messages)
                .thenReturn(messages);
        SinkWithRetry sinkWithRetry = new SinkWithRetry(sinkDecorator, backOffProvider, firehoseInstrumentation, appConfig, parser, errorHandler);

        List<Message> messageList = sinkWithRetry.pushMessage(Collections.singletonList(message));

        assertFalse(messageList.isEmpty());
        verify(sinkDecorator, Mockito.times(4)).pushMessage(anyList());
    }

    @Test
    public void shouldRetryForNumberOfAttemptsAndSendEmptyMessageOnSuccess() throws IOException, DeserializerException {
        ArrayList<Message> messages = new ArrayList<>();
        messages.add(message);
        messages.add(message);
        when(message.getErrorInfo()).thenReturn(new ErrorInfo(null, ErrorType.DESERIALIZATION_ERROR));
        when(sinkDecorator.pushMessage(anyList())).thenReturn(messages).thenReturn(messages)
                .thenReturn(new ArrayList<>());
        SinkWithRetry sinkWithRetry = new SinkWithRetry(sinkDecorator, backOffProvider, firehoseInstrumentation, appConfig, parser, errorHandler);

        List<Message> messageList = sinkWithRetry.pushMessage(Collections.singletonList(message));

        assertTrue(messageList.isEmpty());
        verify(sinkDecorator, Mockito.times(3)).pushMessage(anyList());
    }

    @Test
    public void shouldRetryUntilSuccess() throws IOException, DeserializerException {
        when(appConfig.getRetryMaxAttempts()).thenReturn(Integer.MAX_VALUE);

        ArrayList<Message> messages = new ArrayList<>();
        messages.add(message);
        messages.add(message);
        when(message.getErrorInfo()).thenReturn(new ErrorInfo(null, ErrorType.DESERIALIZATION_ERROR));
        when(sinkDecorator.pushMessage(anyList())).thenReturn(messages).thenReturn(messages).thenReturn(messages)
                .thenReturn(messages).thenReturn(messages).thenReturn(new ArrayList<>());
        SinkWithRetry sinkWithRetry = new SinkWithRetry(sinkDecorator, backOffProvider, firehoseInstrumentation, appConfig, parser, errorHandler);

        List<Message> messageList = sinkWithRetry.pushMessage(Collections.singletonList(message));

        assertTrue(messageList.isEmpty());
        verify(sinkDecorator, Mockito.times(6)).pushMessage(anyList());
    }

    @Test
    public void shouldLogRetriesMessages() throws IOException, DeserializerException {
        when(appConfig.getRetryMaxAttempts()).thenReturn(10);
        when(appConfig.getInputSchemaType()).thenReturn(InputSchemaType.PROTOBUF);
        ArrayList<Message> messages = new ArrayList<>();
        messages.add(message);
        messages.add(message);
        when(message.getErrorInfo()).thenReturn(new ErrorInfo(null, ErrorType.DESERIALIZATION_ERROR));
        when(firehoseInstrumentation.isDebugEnabled()).thenReturn(true);
        when(sinkDecorator.pushMessage(anyList())).thenReturn(messages).thenReturn(messages).thenReturn(messages)
                .thenReturn(messages).thenReturn(messages).thenReturn(new ArrayList<>());
        SinkWithRetry sinkWithRetry = new SinkWithRetry(sinkDecorator, backOffProvider, firehoseInstrumentation, appConfig, parser, errorHandler);

        List<Message> messageList = sinkWithRetry.pushMessage(Collections.singletonList(message));
        assertTrue(messageList.isEmpty());
        verify(firehoseInstrumentation, times(1)).logInfo("Maximum retry attempts: {}", 10);
        verify(firehoseInstrumentation, times(5)).incrementCounter("firehose_retry_attempts_total");
        verify(firehoseInstrumentation, times(1)).logInfo("Retrying messages attempt count: {}, Number of messages: {}", 1, 2);
        verify(firehoseInstrumentation, times(1)).logInfo("Retrying messages attempt count: {}, Number of messages: {}", 2, 2);
        verify(firehoseInstrumentation, times(1)).logInfo("Retrying messages attempt count: {}, Number of messages: {}", 3, 2);
        verify(firehoseInstrumentation, times(1)).logInfo("Retrying messages attempt count: {}, Number of messages: {}", 4, 2);
        verify(firehoseInstrumentation, times(1)).logInfo("Retrying messages attempt count: {}, Number of messages: {}", 5, 2);
        verify(firehoseInstrumentation, times(5)).logDebug("Retry failed messages: \n{}", "[null, null]");
    }

    @Test
    public void shouldLogRetriesMessagesForJsonInput() throws IOException, DeserializerException {
        when(appConfig.getRetryMaxAttempts()).thenReturn(10);
        when(appConfig.getInputSchemaType()).thenReturn(InputSchemaType.JSON);
        when(appConfig.getKafkaRecordParserMode()).thenReturn("message");
        when(message.getLogMessage()).thenReturn("testing message".getBytes());
        ArrayList<Message> messages = new ArrayList<>();
        messages.add(message);
        messages.add(message);
        when(message.getErrorInfo()).thenReturn(new ErrorInfo(null, ErrorType.DESERIALIZATION_ERROR));
        when(firehoseInstrumentation.isDebugEnabled()).thenReturn(true);
        when(sinkDecorator.pushMessage(anyList())).thenReturn(messages).thenReturn(messages).thenReturn(messages)
                .thenReturn(messages).thenReturn(messages).thenReturn(new ArrayList<>());
        SinkWithRetry sinkWithRetry = new SinkWithRetry(sinkDecorator, backOffProvider, firehoseInstrumentation, appConfig, parser, errorHandler);

        List<Message> messageList = sinkWithRetry.pushMessage(Collections.singletonList(message));
        assertTrue(messageList.isEmpty());
        verify(firehoseInstrumentation, times(1)).logInfo("Maximum retry attempts: {}", 10);
        verify(firehoseInstrumentation, times(5)).incrementCounter("firehose_retry_attempts_total");
        verify(firehoseInstrumentation, times(1)).logInfo("Retrying messages attempt count: {}, Number of messages: {}", 1, 2);
        verify(firehoseInstrumentation, times(1)).logInfo("Retrying messages attempt count: {}, Number of messages: {}", 2, 2);
        verify(firehoseInstrumentation, times(1)).logInfo("Retrying messages attempt count: {}, Number of messages: {}", 3, 2);
        verify(firehoseInstrumentation, times(1)).logInfo("Retrying messages attempt count: {}, Number of messages: {}", 4, 2);
        verify(firehoseInstrumentation, times(1)).logInfo("Retrying messages attempt count: {}, Number of messages: {}", 5, 2);
        verify(firehoseInstrumentation, times(5)).logDebug("Retry failed messages: \n{}", "[testing message, testing message]");
    }

    @Test
    public void shouldAddInstrumentationForRetry() throws Exception {
        when(appConfig.getRetryMaxAttempts()).thenReturn(3);
        ArrayList<Message> messages = new ArrayList<>();
        messages.add(message);
        messages.add(message);
        messages.add(message);
        when(message.getErrorInfo()).thenReturn(new ErrorInfo(null, ErrorType.DESERIALIZATION_ERROR));
        SinkWithRetry sinkWithRetry = new SinkWithRetry(sinkDecorator, backOffProvider, firehoseInstrumentation, appConfig, parser, errorHandler);
        when(sinkDecorator.pushMessage(anyList())).thenReturn(messages).thenReturn(messages).thenReturn(new ArrayList<>());
        List<Message> messageList = sinkWithRetry.pushMessage(Collections.singletonList(message));
        assertTrue(messageList.isEmpty());
        verify(firehoseInstrumentation, times(1)).logInfo("Maximum retry attempts: {}", 3);
        verify(firehoseInstrumentation, times(3)).captureMessageMetrics(RETRY_MESSAGES_TOTAL, Metrics.MessageType.TOTAL, ErrorType.DESERIALIZATION_ERROR, 1);
        verify(firehoseInstrumentation, times(2)).incrementCounter(RETRY_ATTEMPTS_TOTAL);
        verify(firehoseInstrumentation, times(1)).captureMessageMetrics(RETRY_MESSAGES_TOTAL, Metrics.MessageType.SUCCESS, 3);
    }

    @Test
    public void shouldAddInstrumentationForRetryFailures() throws Exception {
        when(appConfig.getRetryMaxAttempts()).thenReturn(1);
        ArrayList<Message> messages = new ArrayList<>();
        messages.add(message);
        messages.add(message);
        messages.add(message);
        when(message.getErrorInfo()).thenReturn(new ErrorInfo(null, ErrorType.DESERIALIZATION_ERROR));
        SinkWithRetry sinkWithRetry = new SinkWithRetry(sinkDecorator, backOffProvider, firehoseInstrumentation, appConfig, parser, errorHandler);
        when(sinkDecorator.pushMessage(anyList())).thenReturn(messages).thenReturn(messages).thenReturn(new ArrayList<>());
        List<Message> messageList = sinkWithRetry.pushMessage(Collections.singletonList(message));
        assertFalse(messageList.isEmpty());
        verify(firehoseInstrumentation, times(1)).logInfo("Maximum retry attempts: {}", 1);
        verify(firehoseInstrumentation, times(3)).captureMessageMetrics(RETRY_MESSAGES_TOTAL, Metrics.MessageType.TOTAL, ErrorType.DESERIALIZATION_ERROR, 1);
        verify(firehoseInstrumentation, times(1)).incrementCounter(RETRY_ATTEMPTS_TOTAL);
        verify(firehoseInstrumentation, times(1)).captureMessageMetrics(RETRY_MESSAGES_TOTAL, Metrics.MessageType.SUCCESS, 0);
        verify(firehoseInstrumentation, times(3)).captureMessageMetrics(RETRY_MESSAGES_TOTAL, Metrics.MessageType.FAILURE, ErrorType.DESERIALIZATION_ERROR, 1);
    }

    @Test(expected = IOException.class)
    public void shouldThrowIOExceptionWhenExceedMaximumRetryAttempts() throws IOException {
        when(appConfig.getRetryMaxAttempts()).thenReturn(4);
        when(appConfig.getRetryFailAfterMaxAttemptsEnable()).thenReturn(true);

        ArrayList<Message> messages = new ArrayList<>();
        messages.add(message);
        messages.add(message);
        when(message.getErrorInfo()).thenReturn(new ErrorInfo(null, ErrorType.DESERIALIZATION_ERROR));
        when(sinkDecorator.pushMessage(anyList())).thenReturn(messages).thenReturn(messages).thenReturn(messages)
                .thenReturn(messages).thenReturn(messages).thenReturn(new ArrayList<>());
        SinkWithRetry sinkWithRetry = new SinkWithRetry(sinkDecorator, backOffProvider, firehoseInstrumentation, appConfig, parser, errorHandler);

        sinkWithRetry.pushMessage(Collections.singletonList(message));
    }

    @Test
    public void shouldRetryMessagesWhenErrorTypesConfigured() throws IOException {
        Message messageWithError = new Message("key".getBytes(), "value".getBytes(), "topic", 1, 1, null, 0, 0, new ErrorInfo(null, ErrorType.DESERIALIZATION_ERROR));

        ArrayList<Message> messages = new ArrayList<>();
        messages.add(messageWithError);
        messages.add(new Message(message, new ErrorInfo(null, ErrorType.SINK_UNKNOWN_ERROR)));
        when(sinkDecorator.pushMessage(anyList())).thenReturn(messages).thenReturn(new LinkedList<>());

        HashSet<ErrorType> errorTypes = new HashSet<>();
        errorTypes.add(ErrorType.DESERIALIZATION_ERROR);
        SinkWithRetry sinkWithRetry = new SinkWithRetry(sinkDecorator, backOffProvider, firehoseInstrumentation, appConfig, parser, errorHandler);

        List<Message> messageList = sinkWithRetry.pushMessage(messages);

        assertEquals(1, messageList.size());
        ArgumentCaptor<List> argumentCaptor = ArgumentCaptor.forClass(List.class);
        verify(sinkDecorator, times(2)).pushMessage(argumentCaptor.capture());
        List<List> args = argumentCaptor.getAllValues();

        assertEquals(2, args.get(0).size());
        assertEquals(1, args.get(1).size());
        assertEquals(messageWithError, args.get(1).get(0));
    }

    @Test
    public void shouldFilterOutNonRetryableErrorsFromRetryAttempts() throws IOException, DeserializerException {
        Message retryableMessage = new Message("key1".getBytes(), "value1".getBytes(), "topic", 1, 1,
                null, 0, 0, new ErrorInfo(null, ErrorType.DESERIALIZATION_ERROR));
        Message nonRetryableMessage = new Message("key2".getBytes(), "value2".getBytes(), "topic", 1, 2,
                null, 0, 0, new ErrorInfo(null, ErrorType.SINK_UNKNOWN_ERROR));

        ArrayList<Message> initialFailedMessages = new ArrayList<>();
        initialFailedMessages.add(retryableMessage);
        initialFailedMessages.add(nonRetryableMessage);

        when(sinkDecorator.pushMessage(anyList()))
                .thenReturn(initialFailedMessages)
                .thenReturn(new ArrayList<>());

        SinkWithRetry sinkWithRetry = new SinkWithRetry(sinkDecorator, backOffProvider,
                firehoseInstrumentation, appConfig, parser, errorHandler);

        List<Message> messageList = sinkWithRetry.pushMessage(initialFailedMessages);

        assertEquals(1, messageList.size());
        assertEquals(ErrorType.SINK_UNKNOWN_ERROR, messageList.get(0).getErrorInfo().getErrorType());

        ArgumentCaptor<List> argumentCaptor = ArgumentCaptor.forClass(List.class);
        verify(sinkDecorator, times(2)).pushMessage(argumentCaptor.capture());
        List<List> capturedArgs = argumentCaptor.getAllValues();

        assertEquals(2, capturedArgs.get(0).size());
        assertEquals(1, capturedArgs.get(1).size());
        assertEquals(ErrorType.DESERIALIZATION_ERROR,
                ((Message) capturedArgs.get(1).get(0)).getErrorInfo().getErrorType());
    }

    @Test
    public void shouldStopRetryingWhenNonRetryableErrorOccurs() throws IOException, DeserializerException {
        errorHandler = new ErrorHandler(ConfigFactory.create(ErrorConfig.class, new HashMap<String, String>() {{
            put("ERROR_TYPES_FOR_RETRY", ErrorType.DESERIALIZATION_ERROR.name() + "," + ErrorType.SINK_RETRYABLE_ERROR.name());
        }}));

        Message message1 = new Message("key1".getBytes(), "value1".getBytes(), "topic", 1, 1);
        Message message2 = new Message("key2".getBytes(), "value2".getBytes(), "topic", 1, 2);
        ArrayList<Message> messages = new ArrayList<>();
        messages.add(message1);
        messages.add(message2);

        ArrayList<Message> firstAttemptMessages = new ArrayList<>();
        firstAttemptMessages.add(new Message(message1, new ErrorInfo(new IOException(), ErrorType.SINK_RETRYABLE_ERROR)));
        firstAttemptMessages.add(new Message(message2, new ErrorInfo(new IOException(), ErrorType.SINK_RETRYABLE_ERROR)));

        ArrayList<Message> secondAttemptMessages = new ArrayList<>();
        secondAttemptMessages.add(new Message(message1, new ErrorInfo(new IOException(), ErrorType.SINK_4XX_ERROR)));
        secondAttemptMessages.add(new Message(message2, new ErrorInfo(new IOException(), ErrorType.SINK_4XX_ERROR)));

        when(sinkDecorator.pushMessage(anyList()))
                .thenReturn(firstAttemptMessages)
                .thenReturn(secondAttemptMessages);

        SinkWithRetry sinkWithRetry = new SinkWithRetry(sinkDecorator, backOffProvider,
                firehoseInstrumentation, appConfig, parser, errorHandler);

        List<Message> finalFailedMessages = sinkWithRetry.pushMessage(messages);

        assertEquals(0, finalFailedMessages.size());
        verify(sinkDecorator, times(2)).pushMessage(anyList());
        verify(firehoseInstrumentation, times(1)).incrementCounter(RETRY_ATTEMPTS_TOTAL);
        verify(firehoseInstrumentation, times(1)).logInfo("Retrying messages attempt count: {}, Number of messages: {}", 1, 2);
    }

}
