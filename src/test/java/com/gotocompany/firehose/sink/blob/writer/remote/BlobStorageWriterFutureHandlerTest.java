package com.gotocompany.firehose.sink.blob.writer.remote;

import com.gotocompany.firehose.metrics.BlobStorageMetrics;
import com.gotocompany.firehose.metrics.FirehoseInstrumentation;
import com.gotocompany.firehose.metrics.Metrics;
import com.gotocompany.firehose.sink.blob.writer.local.LocalFileMetadata;
import org.junit.Assert;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class BlobStorageWriterFutureHandlerTest {

    @Test
    public void shouldNotFilterUnfinishedFuture() {
        Future<Long> future = Mockito.mock(Future.class);
        LocalFileMetadata localFileMetadata = Mockito.mock(LocalFileMetadata.class);
        FirehoseInstrumentation firehoseInstrumentation = Mockito.mock(FirehoseInstrumentation.class);
        BlobStorageWriterFutureHandler handler = new BlobStorageWriterFutureHandler(future, localFileMetadata, firehoseInstrumentation);
        Mockito.when(future.isDone()).thenReturn(false);
        Assert.assertFalse(handler.isFinished());
    }

    @Test
    public void shouldFilterFinishedFuture() throws Exception {
        Future<Long> future = Mockito.mock(Future.class);
        LocalFileMetadata localFileMetadata = Mockito.mock(LocalFileMetadata.class);
        FirehoseInstrumentation firehoseInstrumentation = Mockito.mock(FirehoseInstrumentation.class);
        BlobStorageWriterFutureHandler handler = new BlobStorageWriterFutureHandler(future, localFileMetadata, firehoseInstrumentation);
        Mockito.when(future.isDone()).thenReturn(true);
        Mockito.when(future.get()).thenReturn(1000L);
        Mockito.when(localFileMetadata.getFullPath()).thenReturn("/tmp/test");
        Mockito.when(localFileMetadata.getSize()).thenReturn(1024L);
        Assert.assertTrue(handler.isFinished());
        Mockito.verify(firehoseInstrumentation, Mockito.times(1)).logInfo("Flushed to blob storage {}", "/tmp/test");
        Mockito.verify(firehoseInstrumentation, Mockito.times(1)).incrementCounter(BlobStorageMetrics.FILE_UPLOAD_TOTAL, Metrics.SUCCESS_TAG);
        Mockito.verify(firehoseInstrumentation, Mockito.times(1)).captureCount(BlobStorageMetrics.FILE_UPLOAD_BYTES, 1024L);
        Mockito.verify(firehoseInstrumentation, Mockito.times(1)).captureDuration(BlobStorageMetrics.FILE_UPLOAD_TIME_MILLISECONDS, 1000L);
    }

    @Test
    public void shouldThrowException() throws Exception {
        Future<Long> future = Mockito.mock(Future.class);
        LocalFileMetadata localFileMetadata = Mockito.mock(LocalFileMetadata.class);
        FirehoseInstrumentation firehoseInstrumentation = Mockito.mock(FirehoseInstrumentation.class);
        BlobStorageWriterFutureHandler handler = new BlobStorageWriterFutureHandler(future, localFileMetadata, firehoseInstrumentation);
        Mockito.when(future.isDone()).thenReturn(true);
        Mockito.when(future.get()).thenThrow(new ExecutionException(new IOException()));
        Mockito.when(localFileMetadata.getFullPath()).thenReturn("/tmp/test");
        Mockito.when(localFileMetadata.getSize()).thenReturn(1024L);
        Assertions.assertThrows(BlobStorageFailedException.class, () -> handler.isFinished());
    }
}
