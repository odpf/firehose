package com.gojek.esb.metrics;

import com.gojek.esb.consumer.EsbMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.util.List;

import static com.gojek.esb.metrics.Metrics.*;

/**
 * Instrumentation
 * <p>
 * Handle logging and metric capturing.
 */
public class Instrumentation {

    private StatsDReporter statsDReporter;
    private Logger logger;


    public Instant getStartExecutionTime() {
        return startExecutionTime;
    }

    private Instant startExecutionTime;

    public Instrumentation(StatsDReporter statsDReporter, Logger logger) {
        this.statsDReporter = statsDReporter;
        this.logger = logger;
    }

    public Instrumentation(StatsDReporter statsDReporter, Class clazz) {
        this.statsDReporter = statsDReporter;
        this.logger = LoggerFactory.getLogger(clazz);
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

    public void capturePulledMessageHistogram(long pulledMessageCount) {
        statsDReporter.captureHistogram(PULLED_BATCH_SIZE, pulledMessageCount);
    }

    public void captureFilteredMessageCount(int filteredMessageCount, String filterExpression) {
        statsDReporter.captureCount(KAFKA_FILTERED_MESSAGE, filteredMessageCount, "expr=" + filterExpression);
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
        return ERROR_MESSAGE_TAG + "=" + e.getClass().getName() + ",type=" + errorType;
    }

    // ================ SinkExecutionTelemetry ================

    public void startExecution() {
        startExecutionTime = statsDReporter.getClock().now();
    }

    public void captureSuccessExecutionTelemetry(String sinkType, Integer messageListSize) {
        logger.info("Pushed {} messages to {}.", messageListSize, sinkType);
        statsDReporter.captureDurationSince(SINK_RESPONSE_TIME, this.startExecutionTime);
        statsDReporter.captureCount(MESSAGE_COUNT, messageListSize, SUCCESS_TAG);
        statsDReporter.captureHistogramWithTags(PUSHED_BATCH_SIZE, messageListSize, SUCCESS_TAG);
    }

    public void captureFailedExecutionTelemetry(Exception e, Integer messageListSize) {

        captureNonFatalError(e, "caught {} {}", e.getClass(), e.getMessage());
        statsDReporter.captureCount(MESSAGE_COUNT, messageListSize, FAILURE_TAG);
        statsDReporter.captureHistogramWithTags(PUSHED_BATCH_SIZE, messageListSize, FAILURE_TAG);
    }

    // =================== RetryTelemetry ======================

    public void incrementMessageSucceedCount() {
        statsDReporter.increment(RETRY_MESSAGE_COUNT, SUCCESS_TAG);
    }

    public void captureRetryAttempts() {
        statsDReporter.increment(RETRY_ATTEMPTS);
    }

    public void incrementMessageFailCount(EsbMessage message, Exception e) {
        statsDReporter.increment(RETRY_MESSAGE_COUNT, FAILURE_TAG);
        captureNonFatalError(e, "Unable to send record with key {} and message {} ", message.getLogKey(), message.getLogMessage());
    }

    // ===================== Latency / LifetimeTillSink =====================

    public void capturePreExecutionLatencies(List<EsbMessage> messages) {
        messages.forEach(message -> {
            statsDReporter.captureDurationSince(LIFETIME_TILL_EXECUTION, Instant.ofEpochMilli(message.getTimestamp()));
            statsDReporter.captureDurationSince(LATENCY_ACROSS_FIREHOSE, Instant.ofEpochMilli(message.getConsumeTimestamp()));
        });
    }

    public void captureDurationSince(String metric, Instant instant) {
        statsDReporter.captureDurationSince(metric, instant);
    }

    public void captureSleepTime(String metric, int sleepTime) {
        statsDReporter.gauge(metric, sleepTime);
    }

    // ===================== CountTelemetry =================

    public void captureCountWithTags(String metric, Integer count, String... tags) {
        statsDReporter.captureCount(metric, count, tags);
    }

    public void incrementCounterWithTags(String metric, String... tags) {
        statsDReporter.increment(metric, tags);
    }

    public void incrementCounter(String metric) {
        statsDReporter.increment(metric);
    }

    // ===================== closing =================

    public void close() throws IOException {
        statsDReporter.close();
    }
}
