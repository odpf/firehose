package com.gotocompany.firehose.sink.blob.writer;

import lombok.Data;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledFuture;

@Data
public class WriterOrchestratorStatus implements Closeable {
    private boolean isClosed;
    private ScheduledFuture<?> localFileWriterFuture;
    private ScheduledFuture<?> objectStorageWriterFuture;
    private Throwable throwable;
    private Thread localFileWriterCompletionChecker;
    private Thread objectStorageWriterCompletionChecker;

    public WriterOrchestratorStatus(ScheduledFuture<?> localFileWriterFuture, ScheduledFuture<?> objectStorageWriterFuture) {
        this.localFileWriterFuture = localFileWriterFuture;
        this.objectStorageWriterFuture = objectStorageWriterFuture;
    }

    public void startCheckers() {
        localFileWriterCompletionChecker = new Thread(() -> {
            try {
                getLocalFileWriterFuture().get();
            } catch (InterruptedException e) {
                setThrowable(e);
            } catch (ExecutionException e) {
                setThrowable(e.getCause());
            } finally {
                setClosed(true);
            }
        });
        objectStorageWriterCompletionChecker = new Thread(() -> {
            try {
                getObjectStorageWriterFuture().get();
            } catch (InterruptedException e) {
                setThrowable(e);
            } catch (ExecutionException e) {
                setThrowable(e.getCause());
            } finally {
                setClosed(true);
            }
        });
        localFileWriterCompletionChecker.start();
        objectStorageWriterCompletionChecker.start();
    }

    @Override
    public void close() throws IOException {
        localFileWriterCompletionChecker.interrupt();
        objectStorageWriterCompletionChecker.interrupt();
    }
}
