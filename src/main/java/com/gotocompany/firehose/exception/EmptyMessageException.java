package com.gotocompany.firehose.exception;

/**
 * Empty thrown when the message is contains zero bytes.
 */
public class EmptyMessageException extends DeserializerException {
    public EmptyMessageException() {
        super("log message is empty");
    }
}
