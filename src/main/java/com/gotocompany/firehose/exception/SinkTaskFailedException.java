package com.gotocompany.firehose.exception;

public class SinkTaskFailedException extends RuntimeException {
    public SinkTaskFailedException(Throwable throwable) {
        super(throwable);
    }
}
