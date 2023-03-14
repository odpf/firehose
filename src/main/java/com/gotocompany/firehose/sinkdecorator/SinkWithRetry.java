package com.gotocompany.firehose.sinkdecorator;

import com.google.protobuf.DynamicMessage;
import com.gotocompany.firehose.config.AppConfig;
import com.gotocompany.firehose.message.Message;
import com.gotocompany.firehose.error.ErrorHandler;
import com.gotocompany.firehose.error.ErrorScope;
import com.gotocompany.firehose.exception.DeserializerException;
import com.gotocompany.firehose.metrics.FirehoseInstrumentation;
import com.gotocompany.firehose.metrics.Metrics;
import com.gotocompany.firehose.sink.Sink;
import com.gotocompany.firehose.sink.common.KeyOrMessageParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.gotocompany.firehose.metrics.Metrics.RETRY_MESSAGES_TOTAL;
import static com.gotocompany.firehose.metrics.Metrics.RETRY_ATTEMPTS_TOTAL;

/**
 * Pushes messages with configured retry.
 */
public class SinkWithRetry extends SinkDecorator {

    private final BackOffProvider backOffProvider;
    private final FirehoseInstrumentation firehoseInstrumentation;
    private final AppConfig appConfig;
    private final KeyOrMessageParser parser;
    private final ErrorHandler errorHandler;

    public SinkWithRetry(Sink sink, BackOffProvider backOffProvider, FirehoseInstrumentation firehoseInstrumentation, AppConfig appConfig, KeyOrMessageParser parser, ErrorHandler errorHandler) {
        super(sink);
        this.backOffProvider = backOffProvider;
        this.firehoseInstrumentation = firehoseInstrumentation;
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
        List<Message> failedMessages = super.pushMessage(inputMessages);
        if (failedMessages.isEmpty()) {
            return failedMessages;
        }
        Map<Boolean, List<Message>> splitLists = errorHandler.split(failedMessages, ErrorScope.RETRY);
        List<Message> messagesAfterRetry = doRetry(splitLists.get(Boolean.TRUE));
        if (!messagesAfterRetry.isEmpty() && appConfig.getRetryFailAfterMaxAttemptsEnable()) {
            throw new IOException("exceeded maximum Sink retry attempts");
        }
        messagesAfterRetry.addAll(splitLists.get(Boolean.FALSE));
        return messagesAfterRetry;
    }

    private void logDebug(List<Message> messageList) throws IOException {
        if (firehoseInstrumentation.isDebugEnabled()) {
            switch (appConfig.getInputSchemaType()) {
                case PROTOBUF:
                    List<DynamicMessage> serializedBody = new ArrayList<>();
                    for (Message message : messageList) {
                        serializedBody.add(parser.parse(message));
                    }
                    firehoseInstrumentation.logDebug("Retry failed messages: \n{}", serializedBody.toString());
                    break;
                case JSON:
                    List<String> messages = messageList.stream().map(m -> {
                        if (appConfig.getKafkaRecordParserMode().equals("key")) {
                            return new String(m.getLogKey());
                        } else {
                            return new String(m.getLogMessage());
                        }
                    }).collect(Collectors.toList());
                    firehoseInstrumentation.logDebug("Retry failed messages: \n{}", messages.toString());
                    break;
                default:
                    throw new IllegalArgumentException("Unexpected value: " + appConfig.getInputSchemaType());
            }
        }
    }

    private void backOff(List<Message> messageList, int attemptCount) {
        if (messageList.isEmpty()) {
            return;
        }
        backOffProvider.backOff(attemptCount);
    }

    private List<Message> doRetry(List<Message> messages) throws IOException {
        List<Message> retryMessages = new LinkedList<>(messages);
        firehoseInstrumentation.logInfo("Maximum retry attempts: {}", appConfig.getRetryMaxAttempts());
        retryMessages.forEach(m -> {
            m.setDefaultErrorIfNotPresent();
            firehoseInstrumentation.captureMessageMetrics(RETRY_MESSAGES_TOTAL, Metrics.MessageType.TOTAL, m.getErrorInfo().getErrorType(), 1);
        });

        int attemptCount = 1;
        while ((attemptCount <= appConfig.getRetryMaxAttempts() && !retryMessages.isEmpty())
                || (appConfig.getRetryMaxAttempts() == Integer.MAX_VALUE && !retryMessages.isEmpty())) {
            firehoseInstrumentation.incrementCounter(RETRY_ATTEMPTS_TOTAL);
            firehoseInstrumentation.logInfo("Retrying messages attempt count: {}, Number of messages: {}", attemptCount, messages.size());
            logDebug(retryMessages);
            retryMessages = super.pushMessage(retryMessages);
            backOff(retryMessages, attemptCount);
            attemptCount++;
        }
        firehoseInstrumentation.captureMessageMetrics(RETRY_MESSAGES_TOTAL, Metrics.MessageType.SUCCESS, messages.size() - retryMessages.size());
        retryMessages.forEach(m -> firehoseInstrumentation.captureMessageMetrics(RETRY_MESSAGES_TOTAL, Metrics.MessageType.FAILURE, m.getErrorInfo().getErrorType(), 1));
        return retryMessages;
    }

    @Override
    public void close() throws IOException {
        super.close();
    }
}
