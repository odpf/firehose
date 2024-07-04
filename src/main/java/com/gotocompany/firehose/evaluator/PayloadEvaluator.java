package com.gotocompany.firehose.evaluator;

public interface PayloadEvaluator<T> {
    boolean evaluate(T payload);
}
