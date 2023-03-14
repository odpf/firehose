package com.gotocompany.firehose.sink.blob.writer;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledFuture;

public class WriterOrchestratorStatusTest {
    @Test
    public void shouldCheckForLocalWriterCompletion() throws Exception {
        ScheduledFuture localWriterFuture = Mockito.mock(ScheduledFuture.class);
        ScheduledFuture objectStorageWriterFuture = Mockito.mock(ScheduledFuture.class);
        WriterOrchestratorStatus writerOrchestratorStatus = new WriterOrchestratorStatus(localWriterFuture, objectStorageWriterFuture);
        Mockito.when(localWriterFuture.get()).thenReturn(new Object());
        writerOrchestratorStatus.startCheckers();
        writerOrchestratorStatus.getLocalFileWriterCompletionChecker().join();
        Assert.assertTrue(writerOrchestratorStatus.isClosed());
        Assert.assertNull(writerOrchestratorStatus.getThrowable());
    }

    @Test
    public void shouldCheckForObjectStorageWriterCompletion() throws Exception {
        ScheduledFuture localWriterFuture = Mockito.mock(ScheduledFuture.class);
        ScheduledFuture objectStorageWriterFuture = Mockito.mock(ScheduledFuture.class);
        WriterOrchestratorStatus writerOrchestratorStatus = new WriterOrchestratorStatus(localWriterFuture, objectStorageWriterFuture);
        Mockito.when(objectStorageWriterFuture.get()).thenReturn(new Object());
        writerOrchestratorStatus.startCheckers();
        writerOrchestratorStatus.getObjectStorageWriterCompletionChecker().join();
        Assert.assertTrue(writerOrchestratorStatus.isClosed());
        Assert.assertNull(writerOrchestratorStatus.getThrowable());
    }

    @Test
    public void shouldCheckForLocalWriterCompletionWithException() throws Exception {
        ScheduledFuture localWriterFuture = Mockito.mock(ScheduledFuture.class);
        Throwable t = new IOException("test");
        ScheduledFuture objectStorageWriterFuture = Mockito.mock(ScheduledFuture.class);
        WriterOrchestratorStatus writerOrchestratorStatus = new WriterOrchestratorStatus(localWriterFuture, objectStorageWriterFuture);
        Mockito.when(localWriterFuture.get()).thenThrow(new ExecutionException(t));
        writerOrchestratorStatus.startCheckers();
        writerOrchestratorStatus.getLocalFileWriterCompletionChecker().join();
        Assert.assertTrue(writerOrchestratorStatus.isClosed());
        Assert.assertEquals(t, writerOrchestratorStatus.getThrowable());
    }

    @Test
    public void shouldCheckForObjectStorageCompletionWithException() throws Exception {
        ScheduledFuture localWriterFuture = Mockito.mock(ScheduledFuture.class);
        Throwable t = new IOException("test");
        ScheduledFuture objectStorageWriterFuture = Mockito.mock(ScheduledFuture.class);
        WriterOrchestratorStatus writerOrchestratorStatus = new WriterOrchestratorStatus(localWriterFuture, objectStorageWriterFuture);
        Mockito.when(objectStorageWriterFuture.get()).thenThrow(new ExecutionException(t));
        writerOrchestratorStatus.startCheckers();
        writerOrchestratorStatus.getObjectStorageWriterCompletionChecker().join();
        Assert.assertTrue(writerOrchestratorStatus.isClosed());
        Assert.assertEquals(t, writerOrchestratorStatus.getThrowable());
    }
}
