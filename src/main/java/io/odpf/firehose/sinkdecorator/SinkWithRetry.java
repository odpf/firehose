package io.odpf.firehose.sinkdecorator;

import com.google.protobuf.DynamicMessage;
import io.odpf.firehose.config.AppConfig;
import io.odpf.firehose.consumer.Message;
import io.odpf.firehose.error.ErrorHandler;
import io.odpf.firehose.error.ErrorInfo;
import io.odpf.firehose.error.ErrorScope;
import io.odpf.firehose.error.ErrorType;
import io.odpf.firehose.exception.DeserializerException;
import io.odpf.firehose.metrics.Instrumentation;
import io.odpf.firehose.metrics.Metrics;
import io.odpf.firehose.sink.Sink;
import io.odpf.firehose.sink.log.KeyOrMessageParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static io.odpf.firehose.metrics.Metrics.RETRY_MESSAGES_TOTAL;
import static io.odpf.firehose.metrics.Metrics.RETRY_TOTAL;

/**
 * Pushes messages with configured retry.
 */
public class SinkWithRetry extends SinkDecorator {

    private final BackOffProvider backOffProvider;
    private final Instrumentation instrumentation;
    private final AppConfig appConfig;
    private final KeyOrMessageParser parser;
    private final ErrorHandler errorHandler;

    public SinkWithRetry(Sink sink, BackOffProvider backOffProvider, Instrumentation instrumentation, AppConfig appConfig, KeyOrMessageParser parser, ErrorHandler errorHandler) {
        super(sink);
        this.backOffProvider = backOffProvider;
        this.instrumentation = instrumentation;
        this.appConfig = appConfig;
        this.parser = parser;
        this.errorHandler = errorHandler;
    }

    /**
     * Pushes messages with retry.
     *
     * @param inputMessages list of messages
     * @return the remaining failed messages
     * @throws IOException           the io exception
     * @throws DeserializerException the deserializer exception
     */
    @Override
    public List<Message> pushMessage(List<Message> inputMessages) throws IOException, DeserializerException {
        List<Message> messages = super.pushMessage(inputMessages);
        if (messages.isEmpty()) {
            return messages;
        }

        Map<Boolean, List<Message>> splitLists = errorHandler.split(messages, ErrorScope.RETRY);

        List<Message> returnedMessages = doRetry(splitLists.get(Boolean.TRUE));
        if (!returnedMessages.isEmpty() && appConfig.getRetryFailAfterMaxAttemptsEnable()) {
            throw new IOException("exceeded maximum Sink retry attempts");
        }

        returnedMessages.addAll(splitLists.get(Boolean.FALSE));
        return returnedMessages;
    }

    private List<Message> doRetry(List<Message> messages) throws IOException {
        List<Message> retryMessages = new LinkedList<>(messages);
        instrumentation.logInfo("Maximum retry attempts: {}", appConfig.getRetryMaxAttempts());
        retryMessages.forEach(m -> {
            if (m.getErrorInfo() == null) {
                m.setErrorInfo(new ErrorInfo(null, ErrorType.DEFAULT_ERROR));
            }
            instrumentation.captureMessageMetrics(RETRY_MESSAGES_TOTAL, Metrics.MessageType.TOTAL, m.getErrorInfo().getErrorType(), 1);
        });

        int attemptCount = 1;
        while ((attemptCount <= appConfig.getRetryMaxAttempts() && !retryMessages.isEmpty())
               || (appConfig.getRetryMaxAttempts() == Integer.MAX_VALUE && !retryMessages.isEmpty())) {
            instrumentation.incrementCounter(RETRY_TOTAL);
            instrumentation.logInfo("Retrying messages attempt count: {}, Number of messages: {}", attemptCount, messages.size());

            List<DynamicMessage> serializedBody = new ArrayList<>();
            for (Message message : retryMessages) {
                serializedBody.add(parser.parse(message));
            }
            instrumentation.logDebug("Retry failed messages: \n{}", serializedBody.toString());
            retryMessages = super.pushMessage(retryMessages);
            backOffProvider.backOff(++attemptCount);
        }
        instrumentation.captureMessageMetrics(RETRY_MESSAGES_TOTAL, Metrics.MessageType.SUCCESS, messages.size() - retryMessages.size());
        retryMessages.forEach(m -> instrumentation.captureMessageMetrics(RETRY_MESSAGES_TOTAL, Metrics.MessageType.FAILURE, m.getErrorInfo().getErrorType(), 1));
        return retryMessages;
    }

    @Override
    public void close() throws IOException {
        super.close();
    }
}
