package com.gotocompany.firehose.evaluator;

/**
 * A generic interface for evaluating payloads.
 *
 * @param <T> the type of payload to be evaluated
 */
public interface PayloadEvaluator<T> {
    /**
     * Evaluates the given payload.
     *
     * @param payload the payload to be evaluated
     * @return true if the payload passes the evaluation, false otherwise
     */
    boolean evaluate(T payload);
}
