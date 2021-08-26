package io.odpf.firehose.objectstorage;

import lombok.Getter;

/**
 * Should be thrown when there is exception thrown by object storage client.
 */
@Getter
public class ObjectStorageException extends Exception {
    private final String errorType;
    private final String message;

    public ObjectStorageException(String errorType, String message, Throwable cause) {
        super(message, cause);
        this.errorType = errorType;
        this.message = message;
    }
}
