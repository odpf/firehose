package io.odpf.firehose.sink.objectstorage.writer.local;

import io.odpf.firehose.metrics.Instrumentation;
import io.odpf.firehose.sink.objectstorage.Constants;
import io.odpf.firehose.sink.objectstorage.writer.local.policy.WriterPolicy;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import static io.odpf.firehose.metrics.Metrics.*;
import static io.odpf.firehose.metrics.ObjectStorageMetrics.*;
import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;

public class LocalFileCheckerTest {

    @Mock
    private PartitionFactory partitionFactory;

    @Mock
    private LocalFileWriter writer1;

    @Mock
    private LocalFileWriter writer2;

    @Mock
    private LocalStorage localStorage;

    @Mock
    private Instrumentation instrumentation;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private BlockingQueue<FileMeta> toBeFlushedToRemotePaths = new LinkedBlockingQueue<>();
    private Map<String, LocalFileWriter> writerMap = new ConcurrentHashMap<>();
    private List<WriterPolicy> policies = new ArrayList<>();
    private LocalFileChecker worker;

    private final long fileSize = 1024L;
    private final long recordCount = 100L;
    private final Instant startTime = Instant.parse("2021-01-01T00:10:00.000Z");

    private final Partition partition = new Partition("default", Instant.ofEpochSecond(1L), new PartitionConfig("UTC", Constants.PartitioningType.HOUR, "dt=", "hr="));

    @Before
    public void setup() throws IOException {
        initMocks(this);
        toBeFlushedToRemotePaths.clear();
        writerMap.clear();
        policies.clear();
        when(localStorage.getFileSize(anyString())).thenReturn(fileSize);
        when(writer1.getRecordCount()).thenReturn(recordCount);
        when(writer2.getRecordCount()).thenReturn(recordCount);
        when(partitionFactory.fromPartitionPath(anyString())).thenReturn(partition);
        when(localStorage.getPartitionFactory()).thenReturn(partitionFactory);
        worker = new LocalFileChecker(toBeFlushedToRemotePaths, writerMap, localStorage, instrumentation);
    }

    @Test
    public void shouldRotateBasedOnPolicy() throws IOException {
        writerMap.put("/tmp/a", writer1);
        writerMap.put("/tmp/b", writer2);
        when(localStorage.shouldRotate(writer1)).thenReturn(true);
        when(localStorage.shouldRotate(writer2)).thenReturn(true);

        when(writer1.getFullPath()).thenReturn("/tmp/a/random-file-name-1");
        when(writer2.getFullPath()).thenReturn("/tmp/b/random-file-name-2");

        worker.run();
        verify(writer1, times(1)).close();
        verify(writer2, times(1)).close();
        Assert.assertEquals(2, toBeFlushedToRemotePaths.size());
        Assert.assertEquals(0, writerMap.size());
    }

    @Test
    public void shouldProduceFileMeta() throws IOException {
        long fileSize1 = 128L;
        long fileSize2 = 129L;

        writerMap.put("/tmp/a", writer1);
        writerMap.put("/tmp/b", writer2);
        doNothing().when(writer1).close();
        when(localStorage.shouldRotate(writer1)).thenReturn(true);
        when(localStorage.shouldRotate(writer2)).thenReturn(true);

        when(writer1.getFullPath()).thenReturn("/tmp/a/random-file-name");
        when(writer2.getFullPath()).thenReturn("/tmp/a/random-file-name-2");

        when(localStorage.getFileSize("/tmp/a/random-file-name")).thenReturn(fileSize1);
        when(localStorage.getFileSize("/tmp/a/random-file-name-2")).thenReturn(fileSize2);

        worker.run();
        Assert.assertEquals(2, toBeFlushedToRemotePaths.size());
        verify(writer1, times(1)).getRecordCount();
        verify(writer2, times(1)).getRecordCount();
        Assert.assertTrue(toBeFlushedToRemotePaths.contains(new FileMeta("/tmp/a/random-file-name", recordCount, fileSize1, partition)));
        Assert.assertTrue(toBeFlushedToRemotePaths.contains(new FileMeta("/tmp/a/random-file-name-2", recordCount, fileSize2, partition)));
        Assert.assertEquals(0, writerMap.size());
    }

    @Test
    public void shouldRemoveFromMapIfCloseFails() throws Exception {
        expectedException.expect(LocalFileWriterFailedException.class);
        writerMap.put("/tmp/a", writer1);
        writerMap.put("/tmp/b", writer2);
        doThrow(new IOException("Failed")).when(writer1).close();
        when(localStorage.shouldRotate(writer1)).thenReturn(true);
        when(localStorage.shouldRotate(writer2)).thenReturn(false);
        worker.run();
    }

    @Test
    public void shouldNotRotateBaseOnPolicy() throws Exception {
        writerMap.put("/tmp/a", writer1);
        writerMap.put("/tmp/b", writer2);
        doNothing().when(writer1).close();
        when(localStorage.shouldRotate(writer2)).thenReturn(false);
        when(localStorage.shouldRotate(writer2)).thenReturn(false);
        worker.run();
        Assert.assertEquals(0, toBeFlushedToRemotePaths.size());
        Assert.assertEquals(2, writerMap.size());
        Assert.assertEquals(writer2, writerMap.get("/tmp/b"));
        Assert.assertEquals(writer1, writerMap.get("/tmp/a"));
    }

    @Test
    public void shouldRotateSomeBasedOnPolicy() throws Exception {
        writerMap.put("/tmp/a", writer1);
        writerMap.put("/tmp/b", writer2);
        doNothing().when(writer1).close();
        when(localStorage.shouldRotate(writer1)).thenReturn(true);

        when(localStorage.shouldRotate(writer2)).thenReturn(false);

        when(writer1.getFullPath()).thenReturn("/tmp/a/random-file-name");
        when(writer2.getFullPath()).thenReturn("/tmp/a/random-file-name-2");
        worker.run();
        Assert.assertEquals(1, toBeFlushedToRemotePaths.size());
        Assert.assertTrue(toBeFlushedToRemotePaths.contains(new FileMeta("/tmp/a/random-file-name", recordCount, fileSize, partition)));
        Assert.assertEquals(1, writerMap.size());
        Assert.assertEquals(writer2, writerMap.get("/tmp/b"));
    }

    @Test
    public void shouldRecordMetricOfSuccessfullyClosedFiles() throws IOException {
        writerMap.put("/tmp/a", writer1);
        doNothing().when(writer1).close();
        when(localStorage.shouldRotate(writer1)).thenReturn(true);
        when(writer1.getFullPath()).thenReturn("/tmp/a/random-file-name");
        worker.run();
        verify(instrumentation).incrementCounter(LOCAL_FILE_CLOSE_TOTAL,
                SUCCESS_TAG,
                tag(TOPIC_TAG, partition.getTopic()),
                tag(PARTITION_TAG, partition.getDatetimePathWithoutPrefix()));
    }

    @Test
    public void shouldRecordMetricOfClosingTimeDuration() throws IOException {
        writerMap.put("/tmp/a", writer1);
        doNothing().when(writer1).close();
        when(localStorage.shouldRotate(writer1)).thenReturn(true);
        when(writer1.getFullPath()).thenReturn("/tmp/a/random-file-name");
        worker.run();

        verify(instrumentation, times(1)).captureDurationSince(eq(LOCAL_FILE_CLOSING_TIME_MILLISECONDS), any(Instant.class),
                eq(tag(TOPIC_TAG, partition.getTopic())),
                eq(tag(PARTITION_TAG, partition.getDatetimePathWithoutPrefix())));
    }

    @Test
    public void shouldRecordMetricOfFileSizeInBytes() throws IOException {
        writerMap.put("/tmp/a", writer1);
        doNothing().when(writer1).close();
        when(localStorage.shouldRotate(writer1)).thenReturn(true);
        when(writer1.getFullPath()).thenReturn("/tmp/a/random-file-name");
        worker.run();

        verify(instrumentation, times(1)).captureCount(LOCAL_FILE_SIZE_BYTES, fileSize,
                tag(TOPIC_TAG, partition.getTopic()),
                tag(PARTITION_TAG, partition.getDatetimePathWithoutPrefix()));
    }

    @Test
    public void shouldRecordMetricOfFailedClosedFiles() throws IOException {
        writerMap.put("/tmp/a", writer1);
        writerMap.put("/tmp/b", writer2);
        doThrow(new IOException("Failed")).when(writer1).close();
        when(localStorage.shouldRotate(writer1)).thenReturn(true);
        when(localStorage.shouldRotate(writer2)).thenReturn(false);

        try {
            worker.run();
        } catch (LocalFileWriterFailedException e) {
        }

        verify(instrumentation, times(1)).incrementCounter(LOCAL_FILE_CLOSE_TOTAL,
                FAILURE_TAG,
                tag(TOPIC_TAG, partition.getTopic()),
                tag(PARTITION_TAG, partition.getDatetimePathWithoutPrefix()));
    }

    @Test
    public void shouldCaptureValueOfFileOpenCount() throws IOException {
        writerMap.put("/tmp/a", writer1);
        writerMap.put("/tmp/b", writer2);
        when(localStorage.shouldRotate(writer1)).thenReturn(false).thenReturn(true);
        when(localStorage.shouldRotate(writer2)).thenReturn(false).thenReturn(true);

        when(writer1.getFullPath()).thenReturn("/tmp/a/random-file-name-1");
        when(writer2.getFullPath()).thenReturn("/tmp/b/random-file-name-2");

        worker.run();
        verify(instrumentation, times(1)).captureValue(LOCAL_FILE_OPEN_TOTAL, 2);
        worker.run();

        verify(writer1, times(1)).close();
        verify(writer2, times(1)).close();
        Assert.assertEquals(2, toBeFlushedToRemotePaths.size());
        Assert.assertEquals(0, writerMap.size());
        verify(instrumentation, times(1)).captureValue(LOCAL_FILE_OPEN_TOTAL, 0);
    }
}
