package com.gotocompany.firehose.sink;

import com.gotocompany.firehose.exception.ConfigurationException;
import com.gotocompany.firehose.exception.DeserializerException;
import com.gotocompany.firehose.exception.SinkException;
import com.gotocompany.firehose.message.Message;
import com.gotocompany.firehose.metrics.FirehoseInstrumentation;
import com.gotocompany.firehose.metrics.Metrics;
import lombok.AllArgsConstructor;

import java.io.Closeable;
import java.io.IOException;
import java.sql.SQLException;
import java.time.Instant;
import java.util.List;

/**
 * Abstract sink.
 * All other type of sink will implement this.
 */
@AllArgsConstructor
public abstract class AbstractSink implements Closeable, Sink {

    private final FirehoseInstrumentation firehoseInstrumentation;
    private final String sinkType;

    /**
     * Method to push messages to sink.
     *
     * @param messages the messages
     * @return the list of failed messages
     * @throws DeserializerException when invalid kafka message is encountered
     */
    public List<Message> pushMessage(List<Message> messages) {
        List<Message> failedMessages = messages;
        Instant executionStartTime = null;
        try {
            firehoseInstrumentation.logInfo("Preparing {} messages", messages.size());
            firehoseInstrumentation.captureMessageBatchSize(messages.size());
            firehoseInstrumentation.captureMessageMetrics(Metrics.SINK_MESSAGES_TOTAL, Metrics.MessageType.TOTAL, messages.size());
            prepare(messages);
            firehoseInstrumentation.capturePreExecutionLatencies(messages);
            executionStartTime = firehoseInstrumentation.startExecution();
            failedMessages = execute();
            firehoseInstrumentation.logInfo("Pushed {} messages", messages.size() - failedMessages.size());
        } catch (DeserializerException | ConfigurationException | NullPointerException | SinkException e) {
            throw e;
        } catch (Exception e) {
            if (!messages.isEmpty()) {
                firehoseInstrumentation.logWarn("Failed to push {} messages to sink", messages.size());
            }
            firehoseInstrumentation.captureNonFatalError("firehose_error_event", e, "caught {} {}", e.getClass(), e.getMessage());
            failedMessages = messages;
        } finally {
            // Process success,failure and error metrics
            if (executionStartTime != null) {
                firehoseInstrumentation.captureSinkExecutionTelemetry(sinkType, messages.size());
            }
            firehoseInstrumentation.captureMessageMetrics(Metrics.SINK_MESSAGES_TOTAL, Metrics.MessageType.SUCCESS, messages.size() - failedMessages.size());
            firehoseInstrumentation.captureGlobalMessageMetrics(Metrics.MessageScope.SINK, messages.size() - failedMessages.size());
            processFailedMessages(failedMessages);
        }
        return failedMessages;
    }

    private void processFailedMessages(List<Message> failedMessages) {
        if (failedMessages.size() > 0) {
            firehoseInstrumentation.logError("Failed to Push {} messages to sink ", failedMessages.size());
            failedMessages.forEach(m -> {
                m.setDefaultErrorIfNotPresent();
                firehoseInstrumentation.captureMessageMetrics(Metrics.SINK_MESSAGES_TOTAL, Metrics.MessageType.FAILURE, m.getErrorInfo().getErrorType(), 1);
                firehoseInstrumentation.captureErrorMetrics(m.getErrorInfo().getErrorType());
                firehoseInstrumentation.logError("Failed to Push message. Error: {},Topic: {}, Partition: {},Offset: {}",
                        m.getErrorInfo().getErrorType(),
                        m.getTopic(),
                        m.getPartition(),
                        m.getOffset());
            });
        }
    }

    /**
     * Gets instrumentation.
     *
     * @return the instrumentation
     */
    public FirehoseInstrumentation getFirehoseInstrumentation() {
        return firehoseInstrumentation;
    }

    /**
     * send messages to the sink.
     *
     * @return the list
     * @throws Exception the exception
     */
    protected abstract List<Message> execute() throws Exception;

    /**
     * process the messages before sending to the sink.
     *
     * @param messages the messages
     * @throws DeserializerException the deserializer exception
     * @throws IOException           the io exception
     * @throws SQLException          the sql exception
     */
    protected abstract void prepare(List<Message> messages) throws DeserializerException, IOException, SQLException;
}
