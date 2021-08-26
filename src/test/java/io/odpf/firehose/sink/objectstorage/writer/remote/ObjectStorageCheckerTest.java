package io.odpf.firehose.sink.objectstorage.writer.remote;

import io.odpf.firehose.metrics.Instrumentation;
import io.odpf.firehose.objectstorage.ObjectStorage;
import io.odpf.firehose.objectstorage.ObjectStorageException;
import io.odpf.firehose.objectstorage.gcs.error.GCSErrorType;
import io.odpf.firehose.sink.objectstorage.Constants;
import io.odpf.firehose.sink.objectstorage.writer.local.FileMeta;
import io.odpf.firehose.sink.objectstorage.writer.local.Partition;
import io.odpf.firehose.sink.objectstorage.writer.local.PartitionConfig;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.*;

import static io.odpf.firehose.metrics.Metrics.*;
import static io.odpf.firehose.metrics.ObjectStorageMetrics.*;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class ObjectStorageCheckerTest {

    private final BlockingQueue<FileMeta> toBeFlushedToRemotePaths = new LinkedBlockingQueue<>();
    private final BlockingQueue<String> flushedToRemotePaths = new LinkedBlockingQueue<>();
    private final ExecutorService remoteUploadScheduler = Mockito.mock(ExecutorService.class);
    private final Set<ObjectStorageWriterWorkerFuture> remoteUploadFutures = new HashSet<>();
    @Rule
    public ExpectedException expectedException = ExpectedException.none();
    @Mock
    private ObjectStorage objectStorage;
    private ObjectStorageChecker worker;

    @Mock
    private Instrumentation instrumentation;
    private FileMeta fileMeta;

    @Before
    public void setup() {
        PartitionConfig partitionConfig = new PartitionConfig("UTC", Constants.PartitioningType.HOUR, "dt=", "hr=");
        Partition partition = new Partition("default", Instant.parse("2021-01-01T10:00:00.000Z"), partitionConfig);
        fileMeta = new FileMeta("/tmp/dt=2021-01-01/hr=10/random-filename",
                10L,
                128L,
                partition);
        worker = new ObjectStorageChecker(
                toBeFlushedToRemotePaths,
                flushedToRemotePaths,
                remoteUploadFutures,
                remoteUploadScheduler,
                objectStorage,
                instrumentation);
    }

    @Test
    public void shouldNotAddToFlushedIfUploadIsStillRunning() {
        toBeFlushedToRemotePaths.add(fileMeta);
        Future f = Mockito.mock(Future.class);
        when(f.isDone()).thenReturn(false);
        when(remoteUploadScheduler.submit(any(Callable.class))).thenReturn(f);
        worker.run();
        Assert.assertEquals(0, toBeFlushedToRemotePaths.size());
        Assert.assertEquals(0, flushedToRemotePaths.size());
        Assert.assertEquals(1, remoteUploadFutures.size());
        ArrayList<ObjectStorageWriterWorkerFuture> workerFutures = new ArrayList<>(remoteUploadFutures);
        assertEquals(f, workerFutures.get(0).getFuture());
        assertEquals(fileMeta, workerFutures.get(0).getFileMeta());
    }

    @Test
    public void shouldAddToFlushedIfUploadIsFinished() throws ExecutionException, InterruptedException {
        toBeFlushedToRemotePaths.add(fileMeta);
        Future f = Mockito.mock(Future.class);
        when(f.isDone()).thenReturn(false);
        when(remoteUploadScheduler.submit(any(Callable.class))).thenReturn(f);
        worker.run();
        Assert.assertEquals(0, toBeFlushedToRemotePaths.size());
        Assert.assertEquals(0, flushedToRemotePaths.size());
        Assert.assertEquals(1, remoteUploadFutures.size());

        ArrayList<ObjectStorageWriterWorkerFuture> workerFutures = new ArrayList<>(remoteUploadFutures);
        assertEquals(f, workerFutures.get(0).getFuture());
        assertEquals(fileMeta, workerFutures.get(0).getFileMeta());

        when(f.isDone()).thenReturn(true);
        when(f.get()).thenReturn(null);
        when(f.get()).thenReturn(19L);
        worker.run();
        Assert.assertEquals(0, toBeFlushedToRemotePaths.size());
        Assert.assertEquals(0, remoteUploadFutures.size());
        Assert.assertEquals(1, flushedToRemotePaths.size());
        Assert.assertNotNull(flushedToRemotePaths.peek());
        Assert.assertEquals(fileMeta.getFullPath(), flushedToRemotePaths.peek());
    }

    @Test(expected = RuntimeException.class)
    public void shouldThrowExceptionWhenObjectStorageThrowIOException() throws ObjectStorageException, IOException {
        ExecutorService executorService = Executors.newFixedThreadPool(1);
        worker = new ObjectStorageChecker(
                toBeFlushedToRemotePaths,
                flushedToRemotePaths,
                remoteUploadFutures,
                executorService,
                objectStorage,
                instrumentation);
        toBeFlushedToRemotePaths.add(fileMeta);

        doThrow(new RuntimeException(new IOException())).when(objectStorage).store(fileMeta.getFullPath());

        while (true) {
            worker.run();
        }
    }

    @Test(expected = RuntimeException.class)
    public void shouldThrowExceptionWhenObjectStorageThrowObjectStorageException() throws ObjectStorageException, IOException {
        ExecutorService executorService = Executors.newFixedThreadPool(1);
        worker = new ObjectStorageChecker(
                toBeFlushedToRemotePaths,
                flushedToRemotePaths,
                remoteUploadFutures,
                executorService,
                objectStorage,
                instrumentation);
        toBeFlushedToRemotePaths.add(fileMeta);

        doThrow(new RuntimeException(new ObjectStorageException(null, null, null))).when(objectStorage).store(fileMeta.getFullPath());

        while (true) {
            worker.run();
        }
    }

    @Test
    public void shouldRecordMetricOfFileUploadedCount() throws ExecutionException, InterruptedException {
        toBeFlushedToRemotePaths.add(fileMeta);
        Future f = Mockito.mock(Future.class);
        when(f.isDone()).thenReturn(true);
        when(f.get()).thenReturn(10L);
        when(remoteUploadScheduler.submit(any(Callable.class))).thenReturn(f);
        worker.run();

        verify(instrumentation, times(1)).incrementCounter(FILE_UPLOAD_TOTAL,
                SUCCESS_TAG,
                tag(TOPIC_TAG, fileMeta.getPartition().getTopic()),
                tag(PARTITION_TAG, fileMeta.getPartition().getDatetimePathWithoutPrefix()));
    }

    @Test
    public void shouldRecordMetricOfFileUploadBytes() throws ExecutionException, InterruptedException {
        toBeFlushedToRemotePaths.add(fileMeta);
        Future f = Mockito.mock(Future.class);
        when(f.isDone()).thenReturn(true);
        when(f.get()).thenReturn(10L);
        when(remoteUploadScheduler.submit(any(Callable.class))).thenReturn(f);
        worker.run();

        verify(instrumentation).captureCount(FILE_UPLOAD_BYTES, fileMeta.getFileSizeBytes(),
                tag(TOPIC_TAG, fileMeta.getPartition().getTopic()),
                tag(PARTITION_TAG, fileMeta.getPartition().getDatetimePathWithoutPrefix()));
    }

    @Test
    public void shouldRecordMetricOfUploadDuration() throws ExecutionException, InterruptedException {
        long totalTime = 10;
        toBeFlushedToRemotePaths.add(fileMeta);
        Future f = Mockito.mock(Future.class);
        when(f.isDone()).thenReturn(true);
        when(remoteUploadScheduler.submit(any(Callable.class))).thenReturn(f);
        when(f.get()).thenReturn(totalTime);
        worker.run();

        verify(instrumentation, (times(1))).captureDuration(FILE_UPLOAD_TIME_MILLISECONDS, totalTime,
                tag(TOPIC_TAG, fileMeta.getPartition().getTopic()),
                tag(PARTITION_TAG, fileMeta.getPartition().getDatetimePathWithoutPrefix()));
    }

    @Test
    public void shouldRecordMetricOfUploadFailedCountWhenUploadFutureThrowsInterruptedException() {
        toBeFlushedToRemotePaths.add(fileMeta);
        Future f = Mockito.mock(Future.class);
        when(f.isDone()).thenReturn(true);
        try {
            when(f.get()).thenThrow(new InterruptedException());
        } catch (InterruptedException | ExecutionException ignored) {
        }
        when(remoteUploadScheduler.submit(any(Callable.class))).thenReturn(f);
        try {
            worker.run();
        } catch (RuntimeException ignored) {
        }

        verify(instrumentation, times(1)).incrementCounter(FILE_UPLOAD_TOTAL,
                FAILURE_TAG,
                tag(OBJECT_STORE_ERROR_TYPE_TAG, ""),
                tag(TOPIC_TAG, fileMeta.getPartition().getTopic()),
                tag(PARTITION_TAG, fileMeta.getPartition().getDatetimePathWithoutPrefix()));
    }

    @Test
    public void shouldRecordMetricsWhenUploadFutureThrowsObjectStorageExceptionCausedByGCSException() throws ObjectStorageException {
        ExecutorService executorService = Executors.newFixedThreadPool(1);
        worker = new ObjectStorageChecker(
                toBeFlushedToRemotePaths,
                flushedToRemotePaths,
                remoteUploadFutures,
                executorService,
                objectStorage,
                instrumentation);
        toBeFlushedToRemotePaths.add(fileMeta);

        ObjectStorageException objectStorageException = new ObjectStorageException(GCSErrorType.FORBIDDEN.name(), "Test", new RuntimeException());
        doThrow(objectStorageException).when(objectStorage).store(fileMeta.getFullPath());

        while (true) {
            try {
                worker.run();
            } catch (RuntimeException ignored) {
                break;
            }
        }

        verify(instrumentation, times(1)).incrementCounter(FILE_UPLOAD_TOTAL,
                FAILURE_TAG,
                tag(OBJECT_STORE_ERROR_TYPE_TAG, GCSErrorType.FORBIDDEN.name()),
                tag(TOPIC_TAG, fileMeta.getPartition().getTopic()),
                tag(PARTITION_TAG, fileMeta.getPartition().getDatetimePathWithoutPrefix()));
    }

    @Test
    public void shouldRecordMetricOfRecordProcessingFailedWhenUploadFailedCausedByIOException() throws ObjectStorageException {
        ExecutorService executorService = Executors.newFixedThreadPool(1);
        worker = new ObjectStorageChecker(
                toBeFlushedToRemotePaths,
                flushedToRemotePaths,
                remoteUploadFutures,
                executorService,
                objectStorage,
                instrumentation);
        toBeFlushedToRemotePaths.add(fileMeta);

        doThrow(new ObjectStorageException(io.odpf.firehose.sink.objectstorage.writer.remote.Constants.FILE_IO_ERROR, "File Read error", new IOException(new Exception()))).when(objectStorage).store(fileMeta.getFullPath());

        while (true) {
            try {
                worker.run();
            } catch (RuntimeException ignored) {
                break;
            }
        }

        verify(instrumentation, times(1)).incrementCounter(FILE_UPLOAD_TOTAL,
                FAILURE_TAG,
                tag(OBJECT_STORE_ERROR_TYPE_TAG, io.odpf.firehose.sink.objectstorage.writer.remote.Constants.FILE_IO_ERROR),
                tag(TOPIC_TAG, fileMeta.getPartition().getTopic()),
                tag(PARTITION_TAG, fileMeta.getPartition().getDatetimePathWithoutPrefix()));
    }
}
