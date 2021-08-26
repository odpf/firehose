package io.odpf.firehose.sink.objectstorage.writer.local;

import java.io.IOException;

public class LocalFileWriterFailedException extends RuntimeException {
    public LocalFileWriterFailedException(IOException e) {
        super(e);
    }
}
