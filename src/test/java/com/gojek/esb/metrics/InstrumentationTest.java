package com.gojek.esb.metrics;

import com.gojek.esb.consumer.Message;
import com.gojek.esb.util.Clock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.slf4j.Logger;

import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import java.util.List;

import static com.gojek.esb.metrics.Metrics.*;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class InstrumentationTest {
    @Mock
    private StatsDReporter statsDReporter;
    @Mock
    private Logger logger;
    @Mock
    private Message message;

    private Instrumentation instrumentation;
    private String testMessage;
    private String testTemplate;
    private Exception e;

    @Before
    public void setUp() {
        instrumentation = new Instrumentation(statsDReporter, logger);
        testMessage = "test";
        testTemplate = "test: {},{},{}";
        e = new Exception();
    }

    @Test
    public void shouldLogString() {
        instrumentation.logInfo(testMessage);
        verify(logger, times(1)).info(testMessage);
    }

    @Test
    public void shouldLogStringTemplate() {
        instrumentation.logInfo(testTemplate, 1, 2, 3);
        verify(logger, times(1)).info(testTemplate, 1, 2, 3);
    }

    @Test
    public void shouldLogWarnStringTemplate() {
        instrumentation.logWarn(testTemplate, 1, 2, 3);
        verify(logger, times(1)).warn(testTemplate, 1, 2, 3);
    }

    @Test
    public void shouldLogDebugStringTemplate() {
        instrumentation.logDebug(testTemplate, 1, 2, 3);
        verify(logger, times(1)).debug(testTemplate, 1, 2, 3);
    }

    @Test
    public void shouldLogErrorStringTemplate() {
        instrumentation.logError(testTemplate, 1, 2, 3);
        verify(logger, times(1)).error(testTemplate, 1, 2, 3);
    }

    @Test
    public void shouldCapturePulledMessageHistogram() {
        instrumentation.capturePulledMessageHistogram(1);
        verify(statsDReporter, times(1)).captureHistogram(PULLED_BATCH_SIZE, 1);
    }

    @Test
    public void shouldCaptureFilteredMessageCount() {
        String filterExpression = testMessage;
        instrumentation.captureFilteredMessageCount(1, filterExpression);
        verify(statsDReporter, times(1)).captureCount(KAFKA_FILTERED_MESSAGE, 1, "expr=" + filterExpression);
    }

    @Test
    public void shouldCaptureNonFatalErrorWithStringMessage() {
        instrumentation.captureNonFatalError(e, testMessage);
        verify(logger, times(1)).warn(testMessage);
        verify(logger, times(1)).warn(e.getMessage(), e);
        verify(statsDReporter, times(1)).recordEvent(ERROR_EVENT, NON_FATAL_ERROR, ERROR_MESSAGE_TAG + "=" + e.getClass().getName() + ",type=" + NON_FATAL_ERROR);
    }

    @Test
    public void shouldCaptureNonFatalErrorWithStringTemplate() {
        instrumentation.captureNonFatalError(e, testTemplate, 1, 2, 3);
        verify(logger, times(1)).warn(testTemplate, 1, 2, 3);
        verify(logger, times(1)).warn(e.getMessage(), e);
        verify(statsDReporter, times(1)).recordEvent(ERROR_EVENT, NON_FATAL_ERROR, ERROR_MESSAGE_TAG + "=" + e.getClass().getName() + ",type=" + NON_FATAL_ERROR);
    }

    @Test
    public void shouldCaptureFatalErrorWithStringMessage() {
        instrumentation.captureFatalError(e, testMessage);
        verify(logger, times(1)).error(testMessage);
        verify(logger, times(1)).error(e.getMessage(), e);
        verify(statsDReporter, times(1)).recordEvent(ERROR_EVENT, FATAL_ERROR, ERROR_MESSAGE_TAG + "=" + e.getClass().getName() + ",type=" + FATAL_ERROR);
    }

    @Test
    public void shouldCaptureFatalErrorWithStringTemplate() {
        instrumentation.captureFatalError(e, testTemplate, 1, 2, 3);
        verify(logger, times(1)).error(testTemplate, 1, 2, 3);
        verify(logger, times(1)).error(e.getMessage(), e);
        verify(statsDReporter, times(1)).recordEvent(ERROR_EVENT, FATAL_ERROR, ERROR_MESSAGE_TAG + "=" + e.getClass().getName() + ",type=" + FATAL_ERROR);
    }

    @Test
    public void shouldSetStartExecutionTime() {
        Clock clock = new Clock();
        when(statsDReporter.getClock()).thenReturn(clock);
        instrumentation.startExecution();
        Assert.assertEquals(instrumentation.getStartExecutionTime().getEpochSecond(), java.time.Instant.now().getEpochSecond());
    }

    @Test
    public void shouldCaptureSuccessExecutionTelemetry() {
        List<Message> messages = Collections.singletonList(message);
        instrumentation.captureSuccessExecutionTelemetry("test", messages.size());
        verify(logger, times(1)).info("Pushed {} messages to {}.", messages.size(), "test");
        verify(statsDReporter, times(1)).captureDurationSince("sink.response.time", instrumentation.getStartExecutionTime());
        verify(statsDReporter, times(1)).captureCount("messages.count", messages.size(), SUCCESS_TAG);
        verify(statsDReporter, times(1)).captureHistogramWithTags(PUSHED_BATCH_SIZE, messages.size(), SUCCESS_TAG);
    }

    @Test
    public void shouldCaptureFailedExecutionTelemetry() {
        List<Message> messages = Collections.singletonList(message);
        instrumentation.captureFailedExecutionTelemetry(e, messages.size());
        verify(statsDReporter, times(1)).captureCount("messages.count", messages.size(), FAILURE_TAG);
        verify(statsDReporter, times(1)).captureHistogramWithTags(PUSHED_BATCH_SIZE, messages.size(), FAILURE_TAG);
    }

    @Test
    public void shouldIncrementMessageSucceedCount() {
        instrumentation.incrementMessageSucceedCount();
        verify(statsDReporter, times(1)).increment(DLQ_MESSAGE_COUNT, SUCCESS_TAG);
    }

    @Test
    public void shouldCaptureRetryAttempts() {
        instrumentation.captureRetryAttempts();
        verify(statsDReporter, times(1)).increment(RETRY_ATTEMPTS);
    }

    @Test
    public void shouldIncrementMessageFailCount() {
        instrumentation.incrementMessageFailCount(message, e);
        verify(statsDReporter, times(1)).increment(DLQ_MESSAGE_COUNT, FAILURE_TAG);
        verify(logger, times(1)).warn(e.getMessage(), e);
    }

    @Test
    public void shouldCaptureLifetimeTillSink() {
        List<Message> messages = Collections.singletonList(message);
        instrumentation.capturePreExecutionLatencies(messages);
        verify(statsDReporter, times(messages.size())).captureDurationSince("lifetime.till.execution", Instant.ofEpochSecond(message.getTimestamp()));
    }

    @Test
    public void shouldCaptureLatencyAcrossFirehose() {
        List<Message> messages = Collections.singletonList(message);
        instrumentation.capturePreExecutionLatencies(messages);
        verify(statsDReporter, times(messages.size())).captureDurationSince("latency", Instant.ofEpochSecond(message.getConsumeTimestamp()));
    }

    @Test
    public void shouldCapturePartitionProcessTime() {
        Instant instant = Instant.now();
        instrumentation.captureDurationSince("kafka_process_partitions_time", instant);
        verify(statsDReporter, times(1)).captureDurationSince("kafka_process_partitions_time", instant);
    }

    @Test
    public void shouldCaptureBackoffSleepTime() {
        String metric = "backoff_sleep_time";
        int sleepTime = 10000;
        instrumentation.captureSleepTime(metric, sleepTime);
        verify(statsDReporter, times(1)).gauge(metric, sleepTime);
    }
    @Test
    public void shouldCaptureCountWithTags() {
        String metric = "test.metric";
        String urlTag = "url=test";
        String httpCodeTag = "status_code=200";
        instrumentation.captureCountWithTags(metric, 1, httpCodeTag, urlTag);
        verify(statsDReporter, times(1)).captureCount(metric, 1, httpCodeTag, urlTag);
    }

    @Test
    public void shouldIncrementCounterWithTags() {
        String metric = "test.metric";
        String httpCodeTag = "status_code=200";
        instrumentation.incrementCounterWithTags(metric, httpCodeTag);
        verify(statsDReporter, times(1)).increment(metric, httpCodeTag);
    }

    @Test
    public void shouldIncrementCounter() {
        String metric = "test.metric";
        instrumentation.incrementCounter(metric);
        verify(statsDReporter, times(1)).increment(metric);
    }

    @Test
    public void shouldClose() throws IOException {
        instrumentation.close();
        verify(statsDReporter, times(1)).close();
    }
}
