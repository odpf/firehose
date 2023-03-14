package com.gotocompany.firehose.sinkdecorator;

/**
 * Interface to provide back-off functionality.
 * Various implementations can use different backoff strategies
 * and plug it in for use in retry scenario.
 */
public interface BackOffProvider {
    /**
     * backs off for a specific duration depending on the number of attempts.
     *
     * @param attemptCount the number of attempt.
     */
    void backOff(int attemptCount);
}
