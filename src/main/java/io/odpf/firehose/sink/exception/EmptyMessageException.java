package io.odpf.firehose.sink.exception;

import io.odpf.firehose.exception.DeserializerException;

/**
 * Empty thrown when the message is contains zero bytes.
 */
public class EmptyMessageException extends DeserializerException {
    public EmptyMessageException() {
        super("log message is empty");
    }
}
