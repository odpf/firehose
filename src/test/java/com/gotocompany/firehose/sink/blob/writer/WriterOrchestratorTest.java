package com.gotocompany.firehose.sink.blob.writer;

import com.gotocompany.firehose.config.BlobSinkConfig;
import com.gotocompany.firehose.sink.blob.Constants;
import com.gotocompany.firehose.sink.blob.TestProtoMessage;
import com.gotocompany.firehose.sink.blob.message.Record;
import com.gotocompany.firehose.sink.blob.writer.local.LocalFileMetadata;
import com.gotocompany.firehose.sink.blob.writer.local.LocalFileWriter;
import com.gotocompany.firehose.sink.blob.writer.local.LocalFileWriterFailedException;
import com.gotocompany.firehose.sink.blob.writer.local.LocalStorage;
import com.gotocompany.firehose.sink.blob.writer.local.path.TimePartitionedPathUtils;
import com.gotocompany.firehose.sink.common.blobstorage.BlobStorage;
import com.gotocompany.depot.metrics.StatsDReporter;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;
import java.time.Instant;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;

@RunWith(MockitoJUnitRunner.class)
public class WriterOrchestratorTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();
    @Mock
    private LocalFileWriter localFileWriter1;
    @Mock
    private LocalFileWriter localFileWriter2;
    @Mock
    private LocalStorage localStorage;
    @Mock
    private BlobStorage blobStorage;
    @Mock
    private StatsDReporter statsDReporter;

    private final String zone = "UTC";
    private final String timeStampFieldName = TestProtoMessage.CREATED_TIME_FIELD_NAME;
    private final String datePrefix = "dt=";
    private final String hourPrefix = "hr=";
    private final String defaultTopic = "booking-log";
    @Mock
    private BlobSinkConfig sinkConfig;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        this.sinkConfig = Mockito.mock(BlobSinkConfig.class);
        Mockito.when(sinkConfig.getFilePartitionProtoTimestampTimezone()).thenReturn(zone);
        Mockito.when(sinkConfig.getOutputKafkaMetadataColumnName()).thenReturn("");
        Mockito.when(sinkConfig.getFilePartitionProtoTimestampFieldName()).thenReturn(timeStampFieldName);
        Mockito.when(sinkConfig.getFilePartitionTimeGranularityType()).thenReturn(Constants.FilePartitionType.HOUR);
        Mockito.when(sinkConfig.getFilePartitionTimeDatePrefix()).thenReturn(datePrefix);
        Mockito.when(sinkConfig.getFilePartitionTimeHourPrefix()).thenReturn(hourPrefix);
    }

    @Test
    public void shouldCreateLocalFileWriter() throws Exception {
        Record record = Mockito.mock(Record.class);
        Mockito.when(record.getTimestamp(timeStampFieldName)).thenReturn(Instant.ofEpochMilli(1L));
        Mockito.when(record.getTopic("")).thenReturn(defaultTopic);
        Mockito.when(localFileWriter1.getMetadata()).thenReturn(new LocalFileMetadata("/tmp/", "/tmp/test", 0, 0, 0));
        Mockito.when(localStorage.createLocalFileWriter(TimePartitionedPathUtils.getTimePartitionedPath(record, sinkConfig))).thenReturn(localFileWriter1);
        Mockito.when(localFileWriter1.write(record)).thenReturn(true);
        try (WriterOrchestrator writerOrchestrator = new WriterOrchestrator(sinkConfig, localStorage, blobStorage, statsDReporter)) {
            String path = writerOrchestrator.write(record);
            Assert.assertEquals("/tmp/test", path);
        }
    }

    @Test
    public void shouldCreateMultipleWriterBasedOnPartition() throws Exception {
        Record record1 = Mockito.mock(Record.class);
        Mockito.when(record1.getTimestamp(timeStampFieldName)).thenReturn(Instant.ofEpochMilli(3600000L));
        Mockito.when(record1.getTopic("")).thenReturn(defaultTopic);
        Mockito.when(localStorage.createLocalFileWriter(TimePartitionedPathUtils.getTimePartitionedPath(record1, sinkConfig))).thenReturn(localFileWriter1);
        Mockito.when(localFileWriter1.write(record1)).thenReturn(true);
        Mockito.when(localFileWriter1.getMetadata()).thenReturn(new LocalFileMetadata("/tmp/", "/tmp/test1", 0, 0, 0));

        Record record2 = Mockito.mock(Record.class);
        Mockito.when(record2.getTimestamp(timeStampFieldName)).thenReturn(Instant.ofEpochMilli(7200000L));
        Mockito.when(record2.getTopic("")).thenReturn(defaultTopic);
        Mockito.when(localStorage.createLocalFileWriter(TimePartitionedPathUtils.getTimePartitionedPath(record2, sinkConfig))).thenReturn(localFileWriter2);
        Mockito.when(localFileWriter2.write(record2)).thenReturn(true);
        Mockito.when(localFileWriter2.getMetadata()).thenReturn(new LocalFileMetadata("/tmp/", "/tmp/test2", 0, 0, 0));

        try (WriterOrchestrator writerOrchestrator = new WriterOrchestrator(sinkConfig, localStorage, blobStorage, statsDReporter)) {
            Set<String> paths = new HashSet<>();
            paths.add(writerOrchestrator.write(record1));
            paths.add(writerOrchestrator.write(record2));
            assertEquals(2, paths.size());
        }
    }

    @Test(expected = IOException.class)
    public void shouldThrowIOExceptionWhenWriteThrowsException() throws Exception {
        Record record = Mockito.mock(Record.class);
        Mockito.when(record.getTimestamp(timeStampFieldName)).thenReturn(Instant.ofEpochMilli(3600000L));
        Mockito.when(record.getTopic("")).thenReturn(defaultTopic);
        Mockito.when(localFileWriter1.getMetadata()).thenReturn(new LocalFileMetadata("/tmp/", "/tmp/test1", 0, 0, 0));
        Mockito.when(localStorage.createLocalFileWriter(TimePartitionedPathUtils.getTimePartitionedPath(record, sinkConfig))).thenReturn(localFileWriter1);
        try (WriterOrchestrator writerOrchestrator = new WriterOrchestrator(sinkConfig, localStorage, blobStorage, statsDReporter)) {
            Mockito.doThrow(new IOException("")).when(localFileWriter1).write(record);
            writerOrchestrator.write(record);
        }
    }

    @Test
    public void shouldThrowIOExceptionWhenOpenNewWriterFailed() throws Exception {
        expectedException.expect(LocalFileWriterFailedException.class);
        Record record = Mockito.mock(Record.class);
        Mockito.when(record.getTimestamp(timeStampFieldName)).thenReturn(Instant.ofEpochMilli(3600000L));
        Mockito.when(record.getTopic("")).thenReturn(defaultTopic);
        Mockito.when(localFileWriter1.getMetadata()).thenReturn(new LocalFileMetadata("/tmp/", "/tmp/test1", 0, 0, 0));
        Mockito.when(localStorage.createLocalFileWriter(TimePartitionedPathUtils.getTimePartitionedPath(record, sinkConfig))).thenThrow(new LocalFileWriterFailedException(new IOException("Some error")));
        try (WriterOrchestrator writerOrchestrator = new WriterOrchestrator(sinkConfig, localStorage, blobStorage, statsDReporter)) {
            writerOrchestrator.write(record);
        }
    }

    @Test
    public void shouldGetEmptyFlushedPath() throws Exception {
        Record record = Mockito.mock(Record.class);
        Mockito.when(record.getTimestamp(timeStampFieldName)).thenReturn(Instant.ofEpochMilli(1L));
        Mockito.when(record.getTopic("")).thenReturn(defaultTopic);
        Mockito.when(localFileWriter1.getMetadata()).thenReturn(new LocalFileMetadata("/tmp/", "/tmp/test", 0, 0, 0));
        Mockito.when(localStorage.createLocalFileWriter(TimePartitionedPathUtils.getTimePartitionedPath(record, sinkConfig))).thenReturn(localFileWriter1);
        Mockito.when(localFileWriter1.write(record)).thenReturn(true);
        try (WriterOrchestrator writerOrchestrator = new WriterOrchestrator(sinkConfig, localStorage, blobStorage, statsDReporter)) {
            String path = writerOrchestrator.write(record);
            Assert.assertEquals("/tmp/test", path);
            Assert.assertEquals(new HashSet<>(), writerOrchestrator.getFlushedPaths());
        }
    }
}
