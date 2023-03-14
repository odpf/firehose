package com.gotocompany.firehose.exception;

/**
 * Deserializer exception is thrown when message from proto is not deserializable into the Java object.
 */
public class DeserializerException extends RuntimeException {

    public DeserializerException(String message) {
        super(message);
    }

    public DeserializerException(String message, Exception e) {
        super(message, e);
    }
}
