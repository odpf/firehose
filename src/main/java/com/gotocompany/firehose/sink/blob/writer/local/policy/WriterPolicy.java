package com.gotocompany.firehose.sink.blob.writer.local.policy;

import com.gotocompany.firehose.sink.blob.writer.local.LocalFileMetadata;

public interface WriterPolicy {
    boolean shouldRotate(LocalFileMetadata metadata);
}
