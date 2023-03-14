package com.gotocompany.firehose.sinkdecorator;

import com.gotocompany.firehose.metrics.FirehoseInstrumentation;

import lombok.AllArgsConstructor;

@AllArgsConstructor
public class BackOff {

    private FirehoseInstrumentation firehoseInstrumentation;

    public void inMilliSeconds(long milliseconds) {
        try {
            Thread.sleep(milliseconds);
        } catch (InterruptedException e) {
            firehoseInstrumentation.captureNonFatalError("firehose_error_event", e, "Backoff thread sleep for {} milliseconds interrupted : {} {}",
                    milliseconds, e.getClass(), e.getMessage());
        }
    }
}
