package io.odpf.firehose.blobstorage;

import lombok.Getter;

/**
 * Should be thrown when there is exception thrown by blob storage client.
 */
@Getter
public class BlobStorageException extends Exception {
    private final String errorType;
    private final String message;

    public BlobStorageException(String errorType, String message, Throwable cause) {
        super(message, cause);
        this.errorType = errorType;
        this.message = message;
    }
}
