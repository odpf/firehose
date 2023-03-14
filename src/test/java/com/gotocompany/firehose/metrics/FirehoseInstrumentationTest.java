package com.gotocompany.firehose.metrics;

import com.gotocompany.firehose.message.Message;
import com.gotocompany.depot.metrics.StatsDReporter;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.slf4j.Logger;

import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import java.util.List;


import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class FirehoseInstrumentationTest {
    @Mock
    private StatsDReporter statsDReporter;
    @Mock
    private Logger logger;
    @Mock
    private Message message;

    private FirehoseInstrumentation firehoseInstrumentation;
    private String testMessage;
    private String testTemplate;
    private Exception e;

    @Before
    public void setUp() {
        firehoseInstrumentation = new FirehoseInstrumentation(statsDReporter, logger);
        testMessage = "test";
        testTemplate = "test: {},{},{}";
        e = new Exception();
    }

    @Test
    public void shouldLogString() {
        firehoseInstrumentation.logInfo(testMessage);
        verify(logger, times(1)).info(testMessage,  new Object[0]);
    }

    @Test
    public void shouldLogStringTemplate() {
        firehoseInstrumentation.logInfo(testTemplate, 1, 2, 3);
        verify(logger, times(1)).info(testTemplate, 1, 2, 3);
    }

    @Test
    public void shouldLogWarnStringTemplate() {
        firehoseInstrumentation.logWarn(testTemplate, 1, 2, 3);
        verify(logger, times(1)).warn(testTemplate, 1, 2, 3);
    }

    @Test
    public void shouldLogDebugStringTemplate() {
        firehoseInstrumentation.logDebug(testTemplate, 1, 2, 3);
        verify(logger, times(1)).debug(testTemplate, 1, 2, 3);
    }

    @Test
    public void shouldLogErrorStringTemplate() {
        firehoseInstrumentation.logError(testTemplate, 1, 2, 3);
        verify(logger, times(1)).error(testTemplate, 1, 2, 3);
    }

    @Test
    public void shouldCapturePulledMessageHistogram() {
        firehoseInstrumentation.capturePulledMessageHistogram(1);
        verify(statsDReporter, times(1)).captureHistogram(Metrics.SOURCE_KAFKA_PULL_BATCH_SIZE_TOTAL, 1);
    }

    @Test
    public void shouldCaptureFilteredMessageCount() {
        firehoseInstrumentation.captureFilteredMessageCount(1);
        verify(statsDReporter, times(1)).captureCount(Metrics.SOURCE_KAFKA_MESSAGES_FILTER_TOTAL, 1L);
    }

    @Test
    public void shouldCaptureNonFatalErrorWithStringMessage() {
        firehoseInstrumentation.captureNonFatalError("firehose_error_event", e, testMessage);
        verify(logger, times(1)).warn(testMessage,  new Object[0]);
        verify(logger, times(1)).warn(e.getMessage(), e);
        verify(statsDReporter, times(1)).recordEvent(Metrics.ERROR_EVENT, Metrics.NON_FATAL_ERROR, Metrics.ERROR_MESSAGE_CLASS_TAG + "=" + e.getClass().getName() + ",type=" + Metrics.NON_FATAL_ERROR);
    }

    @Test
    public void shouldCaptureNonFatalErrorWithStringTemplate() {
        firehoseInstrumentation.captureNonFatalError("firehose_error_event", e, testTemplate, 1, 2, 3);
        verify(logger, times(1)).warn(testTemplate, 1, 2, 3);
        verify(logger, times(1)).warn(e.getMessage(), e);
        verify(statsDReporter, times(1)).recordEvent(Metrics.ERROR_EVENT, Metrics.NON_FATAL_ERROR, Metrics.ERROR_MESSAGE_CLASS_TAG + "=" + e.getClass().getName() + ",type=" + Metrics.NON_FATAL_ERROR);
    }

    @Test
    public void shouldCaptureFatalErrorWithStringMessage() {
        firehoseInstrumentation.captureFatalError("firehose_error_event", e, testMessage);
        verify(logger, times(1)).error(testMessage, new Object[0]);
        verify(logger, times(1)).error(e.getMessage(), e);
        verify(statsDReporter, times(1)).recordEvent(Metrics.ERROR_EVENT, Metrics.FATAL_ERROR, Metrics.ERROR_MESSAGE_CLASS_TAG + "=" + e.getClass().getName() + ",type=" + Metrics.FATAL_ERROR);
    }

    @Test
    public void shouldCaptureFatalErrorWithStringTemplate() {
        firehoseInstrumentation.captureFatalError("firehose_error_event", e, testTemplate, 1, 2, 3);
        verify(logger, times(1)).error(testTemplate, 1, 2, 3);
        verify(logger, times(1)).error(e.getMessage(), e);
        verify(statsDReporter, times(1)).recordEvent(Metrics.ERROR_EVENT, Metrics.FATAL_ERROR, Metrics.ERROR_MESSAGE_CLASS_TAG + "=" + e.getClass().getName() + ",type=" + Metrics.FATAL_ERROR);
    }

    @Test
    public void shouldSetStartExecutionTime() {
        firehoseInstrumentation.startExecution();
        Assert.assertEquals(firehoseInstrumentation.getStartExecutionTime().getEpochSecond(), java.time.Instant.now().getEpochSecond());
    }

    @Test
    public void shouldReturnStartExecutionTime() {
        Instant time = firehoseInstrumentation.startExecution();
        Assert.assertEquals(firehoseInstrumentation.getStartExecutionTime().getEpochSecond(), time.getEpochSecond());
    }

    @Test
    public void shouldCaptureLifetimeTillSink() {
        List<Message> messages = Collections.singletonList(message);
        firehoseInstrumentation.capturePreExecutionLatencies(messages);
        verify(statsDReporter, times(messages.size())).captureDurationSince("firehose_pipeline_execution_lifetime_milliseconds", Instant.ofEpochSecond(message.getTimestamp()));
    }

    @Test
    public void shouldCaptureGlobalMetrics() {
        firehoseInstrumentation.captureGlobalMessageMetrics(Metrics.MessageScope.CONSUMER, 1);
        verify(statsDReporter, times(1)).captureCount(Metrics.GLOBAL_MESSAGES_TOTAL, 1L, String.format(Metrics.MESSAGE_SCOPE_TAG, Metrics.MessageScope.CONSUMER));
    }

    @Test
    public void shouldCaptureLatencyAcrossFirehose() {
        List<Message> messages = Collections.singletonList(message);
        firehoseInstrumentation.capturePreExecutionLatencies(messages);
        verify(statsDReporter, times(messages.size())).captureDurationSince("firehose_pipeline_end_latency_milliseconds", Instant.ofEpochSecond(message.getConsumeTimestamp()));
    }

    @Test
    public void shouldCapturePartitionProcessTime() {
        Instant instant = Instant.now();
        firehoseInstrumentation.captureDurationSince(Metrics.SOURCE_KAFKA_PARTITIONS_PROCESS_TIME_MILLISECONDS, instant);
        verify(statsDReporter, times(1)).captureDurationSince(Metrics.SOURCE_KAFKA_PARTITIONS_PROCESS_TIME_MILLISECONDS, instant);
    }

    @Test
    public void shouldCaptureBackoffSleepTime() {
        String metric = "firehose_retry_backoff_sleep_milliseconds";
        int sleepTime = 10000;
        firehoseInstrumentation.captureSleepTime(metric, sleepTime);
        verify(statsDReporter, times(1)).gauge(metric, sleepTime);
    }

    @Test
    public void shouldCaptureCountWithTags() {
        String metric = "test_metric";
        String urlTag = "url=test";
        String httpCodeTag = "status_code=200";
        firehoseInstrumentation.captureCount(metric, 1L, httpCodeTag, urlTag);
        verify(statsDReporter, times(1)).captureCount(metric, 1L, httpCodeTag, urlTag);
    }

    @Test
    public void shouldIncrementCounterWithTags() {
        String metric = "test_metric";
        String httpCodeTag = "status_code=200";
        firehoseInstrumentation.incrementCounter(metric, httpCodeTag);
        verify(statsDReporter, times(1)).increment(metric, httpCodeTag);
    }

    @Test
    public void shouldIncrementCounter() {
        String metric = "test_metric";
        firehoseInstrumentation.incrementCounter(metric);
        verify(statsDReporter, times(1)).increment(metric);
    }

    @Test
    public void shouldClose() throws IOException {
        firehoseInstrumentation.close();
        verify(statsDReporter, times(1)).close();
    }
}
