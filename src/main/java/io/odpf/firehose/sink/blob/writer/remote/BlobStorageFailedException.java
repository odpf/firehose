package io.odpf.firehose.sink.blob.writer.remote;

public class BlobStorageFailedException extends RuntimeException {
    public BlobStorageFailedException(Throwable th) {
        super(th);
    }
}
