package com.gotocompany.firehose.sink.blob.writer.local;

import java.io.IOException;

public class LocalFileWriterFailedException extends RuntimeException {
    public LocalFileWriterFailedException(IOException e) {
        super(e);
    }
}
