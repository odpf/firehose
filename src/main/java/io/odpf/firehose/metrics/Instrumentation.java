package io.odpf.firehose.metrics;

import io.odpf.firehose.consumer.Message;
import io.odpf.firehose.error.ErrorType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.util.List;

import static io.odpf.firehose.metrics.Metrics.*;

/**
 * Instrumentation.
 * <p>
 * Handle logging and metric capturing.
 */
public class Instrumentation {

    private final StatsDReporter statsDReporter;
    private final Logger logger;
    private Instant startExecutionTime;

    /**
     * Instantiates a new Instrumentation.
     *
     * @param statsDReporter the stats d reporter
     * @param logger         the logger
     */
    public Instrumentation(StatsDReporter statsDReporter, Logger logger) {
        this.statsDReporter = statsDReporter;
        this.logger = logger;
    }

    /**
     * Instantiates a new Instrumentation.
     *
     * @param statsDReporter the stats d reporter
     * @param clazz          the clazz
     */
    public Instrumentation(StatsDReporter statsDReporter, Class clazz) {
        this.statsDReporter = statsDReporter;
        this.logger = LoggerFactory.getLogger(clazz);
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

    public void logInfo(String message) {
        logger.info(message);
    }

    public void logInfo(String template, Object... t) {
        logger.info(template, t);
    }

    public void logWarn(String template, Object... t) {
        logger.warn(template, t);
    }

    public void logDebug(String template, Object... t) {
        logger.debug(template, t);
    }

    public void logError(String template, Object... t) {
        logger.error(template, t);
    }

    // ============== FILTER MESSAGES ==============

    /**
     * Captures batch message histogram.
     *
     * @param pulledMessageCount the pulled message count
     */
    public void capturePulledMessageHistogram(long pulledMessageCount) {
        statsDReporter.captureHistogram(SOURCE_KAFKA_PULL_BATCH_SIZE_TOTAL, pulledMessageCount);
    }

    /**
     * Captures filtered message count.
     *
     * @param filteredMessageCount the filtered message count
     * @param filterExpression     the filter expression
     */
    public void captureFilteredMessageCount(int filteredMessageCount, String filterExpression) {
        statsDReporter.captureCount(SOURCE_KAFKA_MESSAGES_FILTER_TOTAL, filteredMessageCount, "expr=" + filterExpression);
    }

    // =================== ERROR ===================

    public void captureNonFatalError(Exception e) {
        logger.warn(e.getMessage(), e);
        statsDReporter.recordEvent(ERROR_EVENT, NON_FATAL_ERROR, errorTag(e, NON_FATAL_ERROR));
    }

    public void captureNonFatalError(Exception e, String message) {
        logger.warn(message);
        captureNonFatalError(e);
    }

    public void captureNonFatalError(Exception e, String template, Object... t) {
        logger.warn(template, t);
        captureNonFatalError(e);
    }

    public void captureFatalError(Exception e) {
        logger.error(e.getMessage(), e);
        statsDReporter.recordEvent(ERROR_EVENT, FATAL_ERROR, errorTag(e, FATAL_ERROR));
    }

    public void captureFatalError(Exception e, String message) {
        logger.error(message);
        this.captureFatalError(e);
    }

    public void captureFatalError(Exception e, String template, Object... t) {
        logger.error(template, t);
        this.captureFatalError(e);
    }

    private String errorTag(Exception e, String errorType) {
        return ERROR_MESSAGE_CLASS_TAG + "=" + e.getClass().getName() + ",type=" + errorType;
    }

    // ================ SinkExecutionTelemetry ================

    public void startExecution() {
        startExecutionTime = statsDReporter.getClock().now();
    }

    /**
     * Logs total messages executions.
     *
     * @param sinkType        the sink type
     * @param messageListSize the message list size
     */
    public void captureSinkExecutionTelemetry(String sinkType, Integer messageListSize) {
        logger.info("Processed {} messages in {}.", messageListSize, sinkType);
        statsDReporter.captureDurationSince(SINK_RESPONSE_TIME_MILLISECONDS, this.startExecutionTime);
    }

    /**
     * @param totalMessages total messages
     */
    public void captureMessageBatchSize(int totalMessages) {
        statsDReporter.captureHistogramWithTags(SINK_PUSH_BATCH_SIZE_TOTAL, totalMessages);
    }

    public void captureErrorMetrics(List<ErrorType> errors) {
        errors.forEach(this::captureErrorMetrics);
    }

    public void captureErrorMetrics(ErrorType errorType) {
        statsDReporter.captureCount(ERROR_MESSAGES_TOTAL, 1, String.format(ERROR_TYPE_TAG, errorType.name()));
    }

    // =================== Retry and DLQ Telemetry ======================

    public void captureMessageMetrics(String metric, MessageType type, ErrorType errorType, int counter) {
        if (errorType != null) {
            statsDReporter.captureCount(metric, counter, String.format(MESSAGE_TYPE_TAG, type.name()), String.format(ERROR_TYPE_TAG, errorType.name()));
        } else {
            statsDReporter.captureCount(metric, counter, String.format(MESSAGE_TYPE_TAG, type.name()));
        }
    }

    public void captureGlobalMessageMetrics(MessageScope scope, int counter) {
        statsDReporter.captureCount(GLOBAL_MESSAGES_TOTAL, counter, String.format(MESSAGE_SCOPE_TAG, scope.name()));
    }

    public void captureMessageMetrics(String metric, MessageType type, int counter) {
        captureMessageMetrics(metric, type, null, counter);
    }

    public void captureDLQErrors(Message message, Exception e) {
        captureNonFatalError(e, "Unable to send record with key {} and message {} to DLQ", message.getLogKey(), message.getLogMessage());
    }

    // ===================== Latency / LifetimeTillSink =====================

    public void capturePreExecutionLatencies(List<Message> messages) {
        messages.forEach(message -> {
            statsDReporter.captureDurationSince(PIPELINE_END_LATENCY_MILLISECONDS, Instant.ofEpochMilli(message.getTimestamp()));
            statsDReporter.captureDurationSince(PIPELINE_EXECUTION_LIFETIME_MILLISECONDS, Instant.ofEpochMilli(message.getConsumeTimestamp()));
        });
    }

    public void captureDurationSince(String metric, Instant instant, String... tags) {
        statsDReporter.captureDurationSince(metric, instant, tags);
    }

    public void captureDuration(String metric, long duration, String... tags) {
        statsDReporter.captureDuration(metric, duration, tags);
    }

    public void captureSleepTime(String metric, int sleepTime) {
        statsDReporter.gauge(metric, sleepTime);
    }

    // ===================== CountTelemetry =================

    public void captureCount(String metric, Integer count, String... tags) {
        statsDReporter.captureCount(metric, count, tags);
    }

    public void captureCount(String metric, Long count, String... tags) {
        statsDReporter.captureCount(metric, count, tags);
    }

    public void incrementCounter(String metric, String... tags) {
        statsDReporter.increment(metric, tags);
    }

    public void incrementCounter(String metric) {
        statsDReporter.increment(metric);
    }

    public void captureValue(String metric, Integer value, String... tags) {
        statsDReporter.gauge(metric, value, tags);
    }

    // ===================== closing =================

    public void close() throws IOException {
        statsDReporter.close();
    }
}
