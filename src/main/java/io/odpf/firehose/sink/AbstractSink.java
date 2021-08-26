package io.odpf.firehose.sink;

import io.odpf.firehose.consumer.Message;
import io.odpf.firehose.error.ErrorInfo;
import io.odpf.firehose.error.ErrorType;
import io.odpf.firehose.exception.DeserializerException;
import io.odpf.firehose.exception.EglcConfigurationException;
import io.odpf.firehose.exception.SinkException;
import io.odpf.firehose.metrics.Instrumentation;
import io.odpf.firehose.metrics.Metrics;
import lombok.AllArgsConstructor;

import java.io.Closeable;
import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

import static io.odpf.firehose.metrics.Metrics.SINK_MESSAGES_TOTAL;

/**
 * Abstract sink.
 * All other type of sink will implement this.
 */
@AllArgsConstructor
public abstract class AbstractSink implements Closeable, Sink {

    private final Instrumentation instrumentation;
    private final String sinkType;

    /**
     * Method to push messages to sink.
     *
     * @param messages the messages
     * @return the list of failed messages
     * @throws DeserializerException when invalid kafka message is encountered
     */
    public List<Message> pushMessage(List<Message> messages) throws DeserializerException {
        List<Message> failedMessages = messages;
        try {
            instrumentation.logDebug("Preparing {} messages", messages.size());
            instrumentation.captureMessageBatchSize(messages.size());
            instrumentation.captureMessageMetrics(Metrics.SINK_MESSAGES_TOTAL, Metrics.MessageType.TOTAL, messages.size());
            prepare(messages);
            instrumentation.capturePreExecutionLatencies(messages);
            instrumentation.startExecution();
            failedMessages = execute();
            instrumentation.logInfo("Pushed {} messages", messages.size() - failedMessages.size());
        } catch (DeserializerException | EglcConfigurationException | NullPointerException | SinkException e) {
            throw e;
        } catch (Exception e) {
            if (!messages.isEmpty()) {
                instrumentation.logWarn("Failed to push {} messages to sink", messages.size());
            }
            instrumentation.captureNonFatalError(e, "caught {} {}", e.getClass(), e.getMessage());
            failedMessages = messages;
        } finally {
            // Process success,failure and error metrics
            instrumentation.captureSinkExecutionTelemetry(sinkType, messages.size());
            instrumentation.captureMessageMetrics(Metrics.SINK_MESSAGES_TOTAL, Metrics.MessageType.SUCCESS, messages.size() - failedMessages.size());
            instrumentation.captureGlobalMessageMetrics(Metrics.MessageScope.SINK, messages.size() - failedMessages.size());
            processFailedMessages(failedMessages);
        }
        return failedMessages;
    }

    private void processFailedMessages(List<Message> failedMessages) {
        if (failedMessages.size() > 0) {
            instrumentation.logError("Failed to Push {} messages to sink ", failedMessages.size());
            failedMessages.forEach(m -> {
                if (m.getErrorInfo() == null) {
                    m.setErrorInfo(new ErrorInfo(null, ErrorType.DEFAULT_ERROR));
                }
                instrumentation.captureMessageMetrics(SINK_MESSAGES_TOTAL, Metrics.MessageType.FAILURE, m.getErrorInfo().getErrorType(), 1);
                instrumentation.captureErrorMetrics(m.getErrorInfo().getErrorType());
                instrumentation.logError("Failed to Push message. Error: {},Topic: {}, Partition: {},Offset: {}",
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
    public Instrumentation getInstrumentation() {
        return instrumentation;
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
