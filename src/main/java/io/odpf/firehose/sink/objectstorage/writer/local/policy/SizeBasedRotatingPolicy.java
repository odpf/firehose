package io.odpf.firehose.sink.objectstorage.writer.local.policy;

import io.odpf.firehose.sink.objectstorage.writer.local.LocalFileWriter;

public class SizeBasedRotatingPolicy implements WriterPolicy {

    private final long maxSize;

    public SizeBasedRotatingPolicy(long maxSize) {
        if (maxSize <= 0) {
            throw new IllegalArgumentException("The max size should be a positive integer");
        }
        this.maxSize = maxSize;
    }

    @Override
    public boolean shouldRotate(LocalFileWriter writer) {
        return writer.currentSize() >= maxSize;
    }
}
