package io.odpf.firehose.consumer;

public class AsyncConsumerFailedException extends RuntimeException {
    public AsyncConsumerFailedException(Throwable throwable) {
        super(throwable);
    }
}
