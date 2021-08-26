package io.odpf.firehose.sink.objectstorage.writer;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledFuture;

@AllArgsConstructor
@Data
public class WriterOrchestratorStatus {
    private boolean isClosed;
    private ScheduledFuture<?> localFileWriterFuture;
    private ScheduledFuture<?> objectStorageWriterFuture;
    private Throwable throwable;

    public void startCheckerForLocalFileWriterCompletion() {
        new Thread(() -> {
            try {
                getLocalFileWriterFuture().get();
            } catch (InterruptedException e) {
                setThrowable(e);
            } catch (ExecutionException e) {
                setThrowable(e.getCause());
            } finally {
                setClosed(true);
            }
        }).start();
    }

    public void startCheckerForObjectStorageWriterCompletion() {
        new Thread(() -> {
            try {
                getObjectStorageWriterFuture().get();
            } catch (InterruptedException e) {
                setThrowable(e);
            } catch (ExecutionException e) {
                setThrowable(e.getCause());
            } finally {
                setClosed(true);
            }
        }).start();
    }
}
