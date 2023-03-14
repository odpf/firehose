package com.gotocompany.firehose.sinkdecorator;

import com.gotocompany.firehose.metrics.FirehoseInstrumentation;

import static com.gotocompany.firehose.metrics.Metrics.RETRY_SLEEP_TIME_MILLISECONDS;
import static java.lang.Math.toIntExact;

/**
 * ExponentialBackOffProvider is a provider of exponential back-off algorithm.
 * <p>
 * The backoff time is computed as per the following formula.
 * Min{maximumExpiryTimeInMS, initialExpiryTimeInMs * Math.pow(backoffRate,
 * attemptCount)}
 */
public class ExponentialBackOffProvider implements BackOffProvider {

    private final int initialExpiryTimeInMs;
    private final int backoffRate;
    private final int maximumExpiryTimeInMS;
    private FirehoseInstrumentation firehoseInstrumentation;
    private final BackOff backOff;

    /**
     * Instantiates a new Exponential back off provider.
     *
     * @param initialExpiryTimeInMs the initial expiry time in ms
     * @param backoffRate           the backoff rate
     * @param maximumExpiryTimeInMS the maximum expiry time in ms
     * @param firehoseInstrumentation       the instrumentation
     * @param backOff               the back off
     */
    public ExponentialBackOffProvider(int initialExpiryTimeInMs, int backoffRate, int maximumExpiryTimeInMS,
                                      FirehoseInstrumentation firehoseInstrumentation, BackOff backOff) {
        this.initialExpiryTimeInMs = initialExpiryTimeInMs;
        this.backoffRate = backoffRate;
        this.maximumExpiryTimeInMS = maximumExpiryTimeInMS;
        this.firehoseInstrumentation = firehoseInstrumentation;
        this.backOff = backOff;
    }

    @Override
    public void backOff(int attemptCount) {
        long sleepTime = this.calculateDelay(attemptCount);
        firehoseInstrumentation.logWarn("backing off for {} milliseconds ", sleepTime);
        firehoseInstrumentation.captureSleepTime(RETRY_SLEEP_TIME_MILLISECONDS, toIntExact(sleepTime));
        backOff.inMilliSeconds(sleepTime);
    }

    private long calculateDelay(int attemptCount) {
        double exponentialBackOffTimeInMs = initialExpiryTimeInMs * Math.pow(backoffRate, attemptCount);
        return (long) Math.min(maximumExpiryTimeInMS, exponentialBackOffTimeInMs);
    }
}
