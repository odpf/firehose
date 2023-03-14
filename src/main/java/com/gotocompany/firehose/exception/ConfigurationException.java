package com.gotocompany.firehose.exception;

/**
 * This exception is thrown when there is invalid configuration encountered.
 */
public class ConfigurationException extends RuntimeException {

    public ConfigurationException(String message) {
        super(message);
    }

    public ConfigurationException(String message, Exception e) {
        super(message, e);
    }
}

