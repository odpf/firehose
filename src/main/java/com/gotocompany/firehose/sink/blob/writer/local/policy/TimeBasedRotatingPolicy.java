package com.gotocompany.firehose.sink.blob.writer.local.policy;

import com.gotocompany.firehose.sink.blob.writer.local.LocalFileMetadata;

public class TimeBasedRotatingPolicy implements WriterPolicy {

    private final long maxRotatingDurationMillis;

    public TimeBasedRotatingPolicy(long maxRotatingDurationMillis) {
        if (maxRotatingDurationMillis <= 0) {
            throw new IllegalArgumentException("The max duration should be a positive integer");
        }
        this.maxRotatingDurationMillis = maxRotatingDurationMillis;
    }

    @Override
    public boolean shouldRotate(LocalFileMetadata metadata) {
        return System.currentTimeMillis() - metadata.getCreatedTimestampMillis() >= maxRotatingDurationMillis;
    }
}
