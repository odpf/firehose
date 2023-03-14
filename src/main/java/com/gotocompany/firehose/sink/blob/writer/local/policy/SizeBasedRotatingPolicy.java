package com.gotocompany.firehose.sink.blob.writer.local.policy;

import com.gotocompany.firehose.sink.blob.writer.local.LocalFileMetadata;

public class SizeBasedRotatingPolicy implements WriterPolicy {

    private final long maxSize;

    public SizeBasedRotatingPolicy(long maxSize) {
        if (maxSize <= 0) {
            throw new IllegalArgumentException("The max size should be a positive integer");
        }
        this.maxSize = maxSize;
    }

    @Override
    public boolean shouldRotate(LocalFileMetadata metadata) {
        return metadata.getSize() >= maxSize;
    }
}
