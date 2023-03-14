package com.gotocompany.firehose.metrics;

import com.gotocompany.firehose.message.Message;
import com.gotocompany.depot.error.ErrorType;
import com.gotocompany.depot.metrics.Instrumentation;
import com.gotocompany.depot.metrics.StatsDReporter;
import org.slf4j.Logger;

import java.io.IOException;
import java.time.Instant;
import java.util.List;

import static com.gotocompany.firehose.metrics.Metrics.ERROR_MESSAGES_TOTAL;
import static com.gotocompany.firehose.metrics.Metrics.ERROR_TYPE_TAG;
import static com.gotocompany.firehose.metrics.Metrics.GLOBAL_MESSAGES_TOTAL;
import static com.gotocompany.firehose.metrics.Metrics.MESSAGE_SCOPE_TAG;
import static com.gotocompany.firehose.metrics.Metrics.MESSAGE_TYPE_TAG;
import static com.gotocompany.firehose.metrics.Metrics.MessageType;
import static com.gotocompany.firehose.metrics.Metrics.PIPELINE_END_LATENCY_MILLISECONDS;
import static com.gotocompany.firehose.metrics.Metrics.PIPELINE_EXECUTION_LIFETIME_MILLISECONDS;
import static com.gotocompany.firehose.metrics.Metrics.SINK_PUSH_BATCH_SIZE_TOTAL;
import static com.gotocompany.firehose.metrics.Metrics.SINK_RESPONSE_TIME_MILLISECONDS;
import static com.gotocompany.firehose.metrics.Metrics.SOURCE_KAFKA_MESSAGES_FILTER_TOTAL;
import static com.gotocompany.firehose.metrics.Metrics.SOURCE_KAFKA_PULL_BATCH_SIZE_TOTAL;

/**
 * Instrumentation.
 * <p>
 * Handle logging and metric capturing.
 */
public class FirehoseInstrumentation extends Instrumentation {

    private Instant startExecutionTime;

    /**
     * Instantiates a new Instrumentation.
     *
     * @param statsDReporter the stats d reporter
     * @param logger         the logger
     */
    public FirehoseInstrumentation(StatsDReporter statsDReporter, Logger logger) {
        super(statsDReporter, logger);
    }

    /**
     * Instantiates a new Instrumentation.
     *
     * @param statsDReporter the stats d reporter
     * @param clazz          the clazz
     */
    public FirehoseInstrumentation(StatsDReporter statsDReporter, Class clazz) {
        super(statsDReporter, clazz);
    }

    /**
     * Gets start execution time.
     *
     * @return the start execution time
     */
    public Instant getStartExecutionTime() {
        return startExecutionTime;
    }
    // =================== LOGGING ===================

    // ============== FILTER MESSAGES ==============

    /**
     * Captures batch message histogram.
     *
     * @param pulledMessageCount the pulled message count
     */
    public void capturePulledMessageHistogram(long pulledMessageCount) {
        captureHistogram(SOURCE_KAFKA_PULL_BATCH_SIZE_TOTAL, pulledMessageCount);
    }

    /**
     * Captures filtered message count.
     *
     * @param filteredMessageCount the filtered message count
     */
    public void captureFilteredMessageCount(long filteredMessageCount) {
        captureCount(SOURCE_KAFKA_MESSAGES_FILTER_TOTAL, filteredMessageCount);
    }


    // ================ SinkExecutionTelemetry ================

    public Instant startExecution() {
        startExecutionTime = Instant.now();
        return startExecutionTime;
    }

    /**
     * Logs total messages executions.
     *
     * @param sinkType        the sink type
     * @param messageListSize the message list size
     */
    public void captureSinkExecutionTelemetry(String sinkType, Integer messageListSize) {
        logInfo("Processed {} messages in {}.", messageListSize, sinkType);
        captureDurationSince(SINK_RESPONSE_TIME_MILLISECONDS, this.startExecutionTime);
    }

    /**
     * @param totalMessages total messages
     */
    public void captureMessageBatchSize(long totalMessages) {
        captureHistogram(SINK_PUSH_BATCH_SIZE_TOTAL, totalMessages);
    }

    public void captureErrorMetrics(List<ErrorType> errors) {
        errors.forEach(this::captureErrorMetrics);
    }

    public void captureErrorMetrics(ErrorType errorType) {
        captureCount(ERROR_MESSAGES_TOTAL, 1L, String.format(ERROR_TYPE_TAG, errorType.name()));
    }

    // =================== Retry and DLQ Telemetry ======================

    public void captureMessageMetrics(String metric, MessageType type, ErrorType errorType, long counter) {
        if (errorType != null) {
            captureCount(metric, counter, String.format(MESSAGE_TYPE_TAG, type.name()), String.format(ERROR_TYPE_TAG, errorType.name()));
        } else {
            captureCount(metric, counter, String.format(MESSAGE_TYPE_TAG, type.name()));
        }
    }

    public void captureGlobalMessageMetrics(Metrics.MessageScope scope, long counter) {
        captureCount(GLOBAL_MESSAGES_TOTAL, counter, String.format(MESSAGE_SCOPE_TAG, scope.name()));
    }

    public void captureMessageMetrics(String metric, MessageType type, int counter) {
        captureMessageMetrics(metric, type, null, counter);
    }

    public void captureDLQErrors(Message message, Exception e) {
        captureNonFatalError("firehose_error_event", e, "Unable to send record with key {} and message {} to DLQ", message.getLogKey(), message.getLogMessage());
    }

    // ===================== Latency / LifetimeTillSink =====================

    public void capturePreExecutionLatencies(List<Message> messages) {
        messages.forEach(message -> {
            captureDurationSince(PIPELINE_END_LATENCY_MILLISECONDS, Instant.ofEpochMilli(message.getTimestamp()));
            captureDurationSince(PIPELINE_EXECUTION_LIFETIME_MILLISECONDS, Instant.ofEpochMilli(message.getConsumeTimestamp()));
        });
    }

    public void captureSleepTime(String metric, int sleepTime) {
        captureValue(metric, sleepTime);
    }

    // ===================== closing =================

    public void close() throws IOException {
        super.close();
    }
}
