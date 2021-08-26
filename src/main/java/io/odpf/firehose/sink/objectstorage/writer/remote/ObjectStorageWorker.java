package io.odpf.firehose.sink.objectstorage.writer.remote;

import io.odpf.firehose.objectstorage.ObjectStorage;
import io.odpf.firehose.objectstorage.ObjectStorageException;
import lombok.AllArgsConstructor;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.Callable;

/**
 * Uploads a local file to object-storage and returns the total time taken.
 */
@AllArgsConstructor
public class ObjectStorageWorker implements Callable<Long> {

    private final ObjectStorage objectStorage;
    private final String path;

    @Override
    public Long call() throws ObjectStorageException {
        Instant start = Instant.now();
        objectStorage.store(path);
        return Duration.between(start, Instant.now()).toMillis();
    }
}
