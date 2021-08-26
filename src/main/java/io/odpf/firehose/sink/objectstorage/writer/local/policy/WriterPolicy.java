package io.odpf.firehose.sink.objectstorage.writer.local.policy;

import io.odpf.firehose.sink.objectstorage.writer.local.LocalFileWriter;

public interface WriterPolicy {
    boolean shouldRotate(LocalFileWriter writer);
}
