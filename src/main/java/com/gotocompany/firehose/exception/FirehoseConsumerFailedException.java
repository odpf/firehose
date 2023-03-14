package com.gotocompany.firehose.exception;

public class FirehoseConsumerFailedException extends RuntimeException {
    public FirehoseConsumerFailedException(Throwable th) {
        super(th);
    }
}
