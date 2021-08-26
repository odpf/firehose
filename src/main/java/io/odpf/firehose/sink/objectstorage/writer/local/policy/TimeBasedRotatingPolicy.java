package io.odpf.firehose.sink.objectstorage.writer.local.policy;

import io.odpf.firehose.sink.objectstorage.writer.local.LocalFileWriter;

public class TimeBasedRotatingPolicy implements WriterPolicy {

    private final long maxRotatingDurationMillis;

    public TimeBasedRotatingPolicy(long maxRotatingDurationMillis) {
        if (maxRotatingDurationMillis <= 0) {
            throw new IllegalArgumentException("The max duration should be a positive integer");
        }
        this.maxRotatingDurationMillis = maxRotatingDurationMillis;
    }

    @Override
    public boolean shouldRotate(LocalFileWriter writer) {
        return System.currentTimeMillis() - writer.getCreatedTimestampMillis() >= maxRotatingDurationMillis;
    }
}
