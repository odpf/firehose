package com.gotocompany.firehose.sinkdecorator;

import com.gotocompany.firehose.config.DlqConfig;
import com.gotocompany.firehose.message.Message;
import com.gotocompany.firehose.error.ErrorHandler;
import com.gotocompany.firehose.error.ErrorScope;
import com.gotocompany.firehose.exception.DeserializerException;
import com.gotocompany.firehose.metrics.FirehoseInstrumentation;
import com.gotocompany.firehose.metrics.Metrics;
import com.gotocompany.firehose.sink.Sink;
import com.gotocompany.firehose.sink.dlq.DlqWriter;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.gotocompany.firehose.metrics.Metrics.DLQ_MESSAGES_TOTAL;
import static com.gotocompany.firehose.metrics.Metrics.DLQ_RETRY_ATTEMPTS_TOTAL;

/**
 * This Sink pushes failed messages to kafka after retries are exhausted.
 */
public class SinkWithDlq extends SinkDecorator {

    public static final String DLQ_BATCH_KEY = "dlq-batch-key";
    private final DlqWriter writer;
    private final BackOffProvider backOffProvider;
    private final DlqConfig dlqConfig;
    private final ErrorHandler errorHandler;

    private final FirehoseInstrumentation firehoseInstrumentation;

    public SinkWithDlq(Sink sink, DlqWriter writer, BackOffProvider backOffProvider, DlqConfig dlqConfig, ErrorHandler errorHandler, FirehoseInstrumentation firehoseInstrumentation) {
        super(sink);
        this.writer = writer;
        this.backOffProvider = backOffProvider;
        this.errorHandler = errorHandler;
        this.firehoseInstrumentation = firehoseInstrumentation;
        this.dlqConfig = dlqConfig;
    }

    /**
     * Pushes all the failed messages to kafka as DLQ.
     *
     * @param inputMessages list of messages to push
     * @return list of failed messaged
     * @throws IOException
     * @throws DeserializerException
     */
    @Override
    public List<Message> pushMessage(List<Message> inputMessages) throws IOException, DeserializerException {
        List<Message> messages = super.pushMessage(inputMessages);
        if (messages.isEmpty()) {
            return messages;
        }
        Map<Boolean, List<Message>> splitLists = errorHandler.split(messages, ErrorScope.DLQ);
        List<Message> returnedMessages = doDLQ(splitLists.get(Boolean.TRUE));
        if (!returnedMessages.isEmpty() && dlqConfig.getDlqRetryFailAfterMaxAttemptEnable()) {
            throw new IOException("exhausted maximum number of allowed retry attempts to write messages to DLQ");
        }
        if (super.canManageOffsets()) {
            super.addOffsetsAndSetCommittable(messages);
        }
        returnedMessages.addAll(splitLists.get(Boolean.FALSE));
        return returnedMessages;
    }

    private void backOff(List<Message> messageList, int attemptCount) {
        if (messageList.isEmpty()) {
            return;
        }
        backOffProvider.backOff(attemptCount);
    }

    private List<Message> doDLQ(List<Message> messages) throws IOException {
        List<Message> retryQueueMessages = new LinkedList<>(messages);
        retryQueueMessages.forEach(m -> {
            m.setDefaultErrorIfNotPresent();
            firehoseInstrumentation.captureMessageMetrics(DLQ_MESSAGES_TOTAL, Metrics.MessageType.TOTAL, m.getErrorInfo().getErrorType(), 1);
        });
        int attemptCount = 1;
        while (attemptCount <= this.dlqConfig.getDlqRetryMaxAttempts() && !retryQueueMessages.isEmpty()) {
            firehoseInstrumentation.incrementCounter(DLQ_RETRY_ATTEMPTS_TOTAL);
            retryQueueMessages = writer.write(retryQueueMessages);
            retryQueueMessages.forEach(message -> Optional.ofNullable(message.getErrorInfo())
                    .flatMap(errorInfo -> Optional.ofNullable(errorInfo.getException()))
                    .ifPresent(e -> firehoseInstrumentation.captureDLQErrors(message, e)));
            backOff(retryQueueMessages, attemptCount);
            attemptCount++;
        }
        if (!retryQueueMessages.isEmpty()) {
            firehoseInstrumentation.logInfo("failed to be processed by DLQ messages: {}", retryQueueMessages.size());
        }
        firehoseInstrumentation.captureMessageMetrics(DLQ_MESSAGES_TOTAL, Metrics.MessageType.SUCCESS, messages.size() - retryQueueMessages.size());
        retryQueueMessages.forEach(m -> firehoseInstrumentation.captureMessageMetrics(DLQ_MESSAGES_TOTAL, Metrics.MessageType.FAILURE, m.getErrorInfo().getErrorType(), 1));
        firehoseInstrumentation.captureGlobalMessageMetrics(Metrics.MessageScope.DLQ, messages.size() - retryQueueMessages.size());
        return retryQueueMessages;
    }

    @Override
    public void close() throws IOException {
        super.close();
    }
}
