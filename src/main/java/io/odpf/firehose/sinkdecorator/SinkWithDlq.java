package io.odpf.firehose.sinkdecorator;

import io.odpf.firehose.config.DlqConfig;
import io.odpf.firehose.consumer.Message;
import io.odpf.firehose.error.ErrorHandler;
import io.odpf.firehose.error.ErrorInfo;
import io.odpf.firehose.error.ErrorScope;
import io.odpf.firehose.error.ErrorType;
import io.odpf.firehose.exception.DeserializerException;
import io.odpf.firehose.metrics.Instrumentation;
import io.odpf.firehose.metrics.Metrics;
import io.odpf.firehose.metrics.StatsDReporter;
import io.odpf.firehose.sink.Sink;
import io.odpf.firehose.sinkdecorator.dlq.DlqWriter;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.odpf.firehose.metrics.Metrics.DLQ_MESSAGES_TOTAL;
import static io.odpf.firehose.metrics.Metrics.DLQ_RETRY_TOTAL;

/**
 * This Sink pushes failed messages to kafka after retries are exhausted.
 */
public class SinkWithDlq extends SinkDecorator {

    public static final String DLQ_BATCH_KEY = "dlq-batch-key";
    private final DlqWriter writer;
    private final BackOffProvider backOffProvider;
    private final DlqConfig dlqConfig;
    private final ErrorHandler errorHandler;

    private final Instrumentation instrumentation;

    public SinkWithDlq(Sink sink, DlqWriter writer, BackOffProvider backOffProvider, DlqConfig dlqConfig, ErrorHandler errorHandler, Instrumentation instrumentation) {
        super(sink);
        this.writer = writer;
        this.backOffProvider = backOffProvider;
        this.errorHandler = errorHandler;
        this.instrumentation = instrumentation;
        this.dlqConfig = dlqConfig;
    }

    public static SinkWithDlq withInstrumentationFactory(Sink sink, DlqWriter dlqWriter, BackOffProvider backOffProvider, DlqConfig dlqConfig, ErrorHandler errorHandler, StatsDReporter statsDReporter) {
        return new SinkWithDlq(sink, dlqWriter, backOffProvider, dlqConfig, errorHandler, new Instrumentation(statsDReporter, SinkWithDlq.class));
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
            super.addOffsets(DLQ_BATCH_KEY, messages);
            super.setCommittable(DLQ_BATCH_KEY);
        }
        returnedMessages.addAll(splitLists.get(Boolean.FALSE));
        return returnedMessages;
    }

    private List<Message> doDLQ(List<Message> messages) throws IOException {
        List<Message> retryQueueMessages = new LinkedList<>(messages);
        retryQueueMessages.forEach(m -> {
            if (m.getErrorInfo() == null) {
                m.setErrorInfo(new ErrorInfo(null, ErrorType.DEFAULT_ERROR));
            }
            instrumentation.captureMessageMetrics(DLQ_MESSAGES_TOTAL, Metrics.MessageType.TOTAL, m.getErrorInfo().getErrorType(), 1);
        });
        int attemptCount = 1;
        while (attemptCount <= this.dlqConfig.getDlqRetryMaxAttempts() && !retryQueueMessages.isEmpty()) {
            instrumentation.incrementCounter(DLQ_RETRY_TOTAL);
            retryQueueMessages = writer.write(retryQueueMessages);
            retryQueueMessages.forEach(message -> Optional.ofNullable(message.getErrorInfo())
                    .flatMap(errorInfo -> Optional.ofNullable(errorInfo.getException()))
                    .ifPresent(e -> instrumentation.captureDLQErrors(message, e)));
            backOffProvider.backOff(++attemptCount);
        }
        if (!retryQueueMessages.isEmpty()) {
            instrumentation.logInfo("failed to be processed by DLQ messages: {}", retryQueueMessages.size());
        }
        instrumentation.captureMessageMetrics(DLQ_MESSAGES_TOTAL, Metrics.MessageType.SUCCESS, messages.size() - retryQueueMessages.size());
        retryQueueMessages.forEach(m -> instrumentation.captureMessageMetrics(DLQ_MESSAGES_TOTAL, Metrics.MessageType.FAILURE, m.getErrorInfo().getErrorType(), 1));
        instrumentation.captureGlobalMessageMetrics(Metrics.MessageScope.DLQ, messages.size() - retryQueueMessages.size());
        return retryQueueMessages;
    }

    @Override
    public void close() throws IOException {
        super.close();
    }
}
