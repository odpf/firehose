package com.gotocompany.firehose.sink.blob.writer.local;

import com.gotocompany.firehose.metrics.BlobStorageMetrics;
import com.gotocompany.firehose.metrics.FirehoseInstrumentation;
import com.gotocompany.firehose.metrics.Metrics;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;

public class LocalFileCheckerTest {


    @Mock
    private LocalFileWriter writer1;

    @Mock
    private LocalFileWriter writer2;

    @Mock
    private LocalStorage localStorage;

    @Mock
    private FirehoseInstrumentation firehoseInstrumentation;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private final BlockingQueue<LocalFileMetadata> toBeFlushedToRemotePaths = new LinkedBlockingQueue<>();
    private final Map<Path, LocalFileWriter> writerMap = new ConcurrentHashMap<>();
    private LocalFileChecker worker;

    private final long fileSize = 1024L;
    private final long recordCount = 100L;

    @Before
    public void setup() throws IOException {
        initMocks(this);
        toBeFlushedToRemotePaths.clear();
        writerMap.clear();
        worker = new LocalFileChecker(toBeFlushedToRemotePaths, writerMap, localStorage, firehoseInstrumentation);
    }

    @Test
    public void shouldRotateBasedOnPolicy() throws IOException {
        writerMap.put(Paths.get("/tmp/a"), writer1);
        writerMap.put(Paths.get("/tmp/b"), writer2);
        when(localStorage.shouldRotate(writer1)).thenReturn(true);
        when(localStorage.shouldRotate(writer2)).thenReturn(true);

        when(writer1.getMetadata()).thenReturn(new LocalFileMetadata("/tmp", "/tmp/a/random-file-name-1", 1L, recordCount, fileSize));
        when(writer2.getMetadata()).thenReturn(new LocalFileMetadata("/tmp", "/tmp/a/random-file-name-2", 1L, recordCount, fileSize));
        when(writer1.closeAndFetchMetaData()).thenReturn(new LocalFileMetadata("/tmp", "/tmp/a/random-file-name-1", 1L, recordCount, fileSize));
        when(writer2.closeAndFetchMetaData()).thenReturn(new LocalFileMetadata("/tmp", "/tmp/a/random-file-name-2", 1L, recordCount, fileSize));

        worker.run();
        verify(writer1, times(1)).closeAndFetchMetaData();
        verify(writer2, times(1)).closeAndFetchMetaData();
        Assert.assertEquals(2, toBeFlushedToRemotePaths.size());
        Assert.assertEquals(0, writerMap.size());
    }

    @Test
    public void shouldProduceFileMeta() throws IOException {
        long fileSize1 = 128L;
        long fileSize2 = 129L;

        writerMap.put(Paths.get("/tmp/a"), writer1);
        writerMap.put(Paths.get("/tmp/b"), writer2);
        doNothing().when(writer1).close();
        when(localStorage.shouldRotate(writer1)).thenReturn(true);
        when(localStorage.shouldRotate(writer2)).thenReturn(true);

        when(writer1.getMetadata()).thenReturn(new LocalFileMetadata("/tmp", "/tmp/a/random-file-name-1", 1L, recordCount, fileSize1));
        when(writer2.getMetadata()).thenReturn(new LocalFileMetadata("/tmp", "/tmp/a/random-file-name-2", 1L, recordCount, fileSize2));
        when(writer1.closeAndFetchMetaData()).thenReturn(new LocalFileMetadata("/tmp", "/tmp/a/random-file-name-1", 1L, recordCount, fileSize1));
        when(writer2.closeAndFetchMetaData()).thenReturn(new LocalFileMetadata("/tmp", "/tmp/a/random-file-name-2", 1L, recordCount, fileSize2));

        worker.run();
        Assert.assertEquals(2, toBeFlushedToRemotePaths.size());
        Assert.assertTrue(toBeFlushedToRemotePaths.contains(new LocalFileMetadata("/tmp", "/tmp/a/random-file-name-1", 1L, recordCount, fileSize1)));
        Assert.assertTrue(toBeFlushedToRemotePaths.contains(new LocalFileMetadata("/tmp", "/tmp/a/random-file-name-2", 1L, recordCount, fileSize2)));
        Assert.assertEquals(0, writerMap.size());
    }

    @Test
    public void shouldRemoveFromMapIfCloseFails() throws Exception {
        expectedException.expect(LocalFileWriterFailedException.class);
        writerMap.put(Paths.get("/tmp/a"), writer1);
        writerMap.put(Paths.get("/tmp/b"), writer2);
        doThrow(new IOException("Failed")).when(writer1).closeAndFetchMetaData();
        when(localStorage.shouldRotate(writer1)).thenReturn(true);
        when(localStorage.shouldRotate(writer2)).thenReturn(false);
        worker.run();
    }

    @Test
    public void shouldNotRotateBaseOnPolicy() throws Exception {
        writerMap.put(Paths.get("/tmp/a"), writer1);
        writerMap.put(Paths.get("/tmp/b"), writer2);
        doNothing().when(writer1).close();
        when(localStorage.shouldRotate(writer2)).thenReturn(false);
        when(localStorage.shouldRotate(writer2)).thenReturn(false);
        worker.run();
        Assert.assertEquals(0, toBeFlushedToRemotePaths.size());
        Assert.assertEquals(2, writerMap.size());
        Assert.assertEquals(writer2, writerMap.get(Paths.get("/tmp/b")));
        Assert.assertEquals(writer1, writerMap.get(Paths.get("/tmp/a")));
    }

    @Test
    public void shouldRotateSomeBasedOnPolicy() throws Exception {
        writerMap.put(Paths.get("/tmp/a"), writer1);
        writerMap.put(Paths.get("/tmp/b"), writer2);
        doNothing().when(writer1).close();
        when(writer1.getMetadata()).thenReturn(new LocalFileMetadata("/tmp", "/tmp/a/random-file-name-1", 1L, recordCount, fileSize));
        when(writer2.getMetadata()).thenReturn(new LocalFileMetadata("/tmp", "/tmp/a/random-file-name-2", 1L, recordCount, fileSize));
        when(writer1.closeAndFetchMetaData()).thenReturn(new LocalFileMetadata("/tmp", "/tmp/a/random-file-name-1", 1L, recordCount, fileSize));
        when(writer2.closeAndFetchMetaData()).thenReturn(new LocalFileMetadata("/tmp", "/tmp/a/random-file-name-2", 1L, recordCount, fileSize));
        when(localStorage.shouldRotate(writer1)).thenReturn(true);
        when(localStorage.shouldRotate(writer2)).thenReturn(false);
        worker.run();
        Assert.assertEquals(1, toBeFlushedToRemotePaths.size());
        Assert.assertTrue(toBeFlushedToRemotePaths.contains(new LocalFileMetadata("/tmp", "/tmp/a/random-file-name-1", 1L, recordCount, fileSize)));
        Assert.assertEquals(1, writerMap.size());
        Assert.assertEquals(writer2, writerMap.get(Paths.get("/tmp/b")));
    }

    @Test
    public void shouldRecordMetricOfSuccessfullyClosedFiles() throws IOException {
        writerMap.put(Paths.get("/tmp/a"), writer1);
        when(localStorage.shouldRotate(writer1)).thenReturn(true);
        when(writer1.getMetadata()).thenReturn(new LocalFileMetadata("/tmp", "/tmp/a/random-file-name-1", 1L, recordCount, fileSize));
        when(writer1.closeAndFetchMetaData()).thenReturn(new LocalFileMetadata("/tmp", "/tmp/a/random-file-name-1", 1L, recordCount, fileSize));
        worker.run();
        verify(firehoseInstrumentation).incrementCounter(BlobStorageMetrics.LOCAL_FILE_CLOSE_TOTAL, Metrics.SUCCESS_TAG);
    }

    @Test
    public void shouldRecordMetricOfClosingTimeDuration() throws IOException {
        writerMap.put(Paths.get("/tmp/a"), writer1);
        doNothing().when(writer1).close();
        when(localStorage.shouldRotate(writer1)).thenReturn(true);
        when(writer1.getMetadata()).thenReturn(new LocalFileMetadata("/tmp", "/tmp/a/random-file-name-1", 1L, recordCount, fileSize));
        when(writer1.closeAndFetchMetaData()).thenReturn(new LocalFileMetadata("/tmp", "/tmp/a/random-file-name-1", 1L, recordCount, fileSize));
        worker.run();

        verify(firehoseInstrumentation, times(1)).captureDurationSince(eq(BlobStorageMetrics.LOCAL_FILE_CLOSING_TIME_MILLISECONDS), any(Instant.class));
    }

    @Test
    public void shouldRecordMetricOfFileSizeInBytes() throws IOException {
        writerMap.put(Paths.get("/tmp/a"), writer1);
        doNothing().when(writer1).close();
        when(localStorage.shouldRotate(writer1)).thenReturn(true);
        when(writer1.getMetadata()).thenReturn(new LocalFileMetadata("/tmp", "/tmp/a/random-file-name-1", 1L, recordCount, fileSize));
        when(writer1.closeAndFetchMetaData()).thenReturn(new LocalFileMetadata("/tmp", "/tmp/a/random-file-name-1", 1L, recordCount, fileSize));
        worker.run();

        verify(firehoseInstrumentation, times(1)).captureCount(BlobStorageMetrics.LOCAL_FILE_SIZE_BYTES, fileSize);
    }

    @Test
    public void shouldRecordMetricOfFailedClosedFiles() throws IOException {
        writerMap.put(Paths.get("/tmp/a"), writer1);
        writerMap.put(Paths.get("/tmp/b"), writer2);
        doThrow(new IOException("Failed")).when(writer1).closeAndFetchMetaData();
        when(localStorage.shouldRotate(writer1)).thenReturn(true);
        when(localStorage.shouldRotate(writer2)).thenReturn(false);

        try {
            worker.run();
        } catch (LocalFileWriterFailedException ignored) {
        }

        verify(firehoseInstrumentation, times(1)).incrementCounter(BlobStorageMetrics.LOCAL_FILE_CLOSE_TOTAL, Metrics.FAILURE_TAG);
    }

    @Test
    public void shouldCaptureValueOfFileOpenCount() throws IOException {
        writerMap.put(Paths.get("/tmp/a"), writer1);
        writerMap.put(Paths.get("/tmp/b"), writer2);
        when(localStorage.shouldRotate(writer1)).thenReturn(false).thenReturn(true);
        when(localStorage.shouldRotate(writer2)).thenReturn(false).thenReturn(true);


        when(writer1.getMetadata()).thenReturn(new LocalFileMetadata("/tmp", "/tmp/a/random-file-name-1", 1L, recordCount, fileSize));
        when(writer2.getMetadata()).thenReturn(new LocalFileMetadata("/tmp", "/tmp/a/random-file-name-2", 1L, recordCount, fileSize));

        when(writer1.closeAndFetchMetaData()).thenReturn(new LocalFileMetadata("/tmp", "/tmp/a/random-file-name-1", 1L, recordCount, fileSize));
        when(writer2.closeAndFetchMetaData()).thenReturn(new LocalFileMetadata("/tmp", "/tmp/a/random-file-name-2", 1L, recordCount, fileSize));
        worker.run();
        worker.run();
        worker.run();

        verify(writer1, times(1)).closeAndFetchMetaData();
        verify(writer2, times(1)).closeAndFetchMetaData();
        Assert.assertEquals(2, toBeFlushedToRemotePaths.size());
        Assert.assertEquals(0, writerMap.size());
        verify(firehoseInstrumentation, times(3)).captureValue(BlobStorageMetrics.LOCAL_FILE_OPEN_TOTAL, 2);
        verify(firehoseInstrumentation, times(3)).captureValue(BlobStorageMetrics.LOCAL_FILE_OPEN_TOTAL, 0);
    }
}
