package com.gotocompany.firehose.sink.blob.writer.local;

import com.gotocompany.firehose.sink.blob.message.Record;

import java.io.Closeable;
import java.io.IOException;

public interface LocalFileWriter extends Closeable {
    /**
     * @param record to write
     * @return true if write succeeds, false if the writer is closed.
     * @throws IOException if local file writing fails
     */
    boolean write(Record record) throws IOException;

    LocalFileMetadata getMetadata();

    LocalFileMetadata closeAndFetchMetaData() throws IOException;
}
