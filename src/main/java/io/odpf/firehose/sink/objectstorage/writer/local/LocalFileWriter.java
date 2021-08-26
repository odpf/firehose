package io.odpf.firehose.sink.objectstorage.writer.local;

import io.odpf.firehose.sink.objectstorage.message.Record;

import java.io.Closeable;
import java.io.IOException;

public interface LocalFileWriter extends Closeable {
    long currentSize();

    /**
     * @param record to write
     * @return true if write succeeds, false if the writer is closed.
     * @throws IOException if local file writing fails
     */
    boolean write(Record record) throws IOException;

    long getCreatedTimestampMillis();

    String getFullPath();

    Long getRecordCount();
}
