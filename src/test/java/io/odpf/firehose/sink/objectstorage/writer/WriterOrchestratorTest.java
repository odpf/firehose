package io.odpf.firehose.sink.objectstorage.writer;

import io.odpf.firehose.metrics.StatsDReporter;
import io.odpf.firehose.objectstorage.ObjectStorage;
import io.odpf.firehose.sink.objectstorage.TestUtils;
import io.odpf.firehose.sink.objectstorage.message.Record;
import io.odpf.firehose.sink.objectstorage.writer.local.LocalFileWriter;
import io.odpf.firehose.sink.objectstorage.writer.local.LocalFileWriterFailedException;
import io.odpf.firehose.sink.objectstorage.writer.local.LocalStorage;
import io.odpf.firehose.sink.objectstorage.writer.local.Partition;
import io.odpf.firehose.sink.objectstorage.writer.local.PartitionFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class WriterOrchestratorTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();
    @Mock
    private PartitionFactory partitionFactory;
    @Mock
    private LocalFileWriter localFileWriter1;
    @Mock
    private LocalFileWriter localFileWriter2;
    @Mock
    private LocalStorage localStorage;
    @Mock
    private ObjectStorage objectStorage;
    @Mock
    private StatsDReporter statsDReporter;

    private final String defaultTopic = "default";

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        Mockito.when(localStorage.getPartitionFactory()).thenReturn(partitionFactory);
    }

    @Test
    public void shouldCreateLocalFileWriter() throws Exception {
        String date = "dt=2021-01-01";
        String partitionPathString = "default/dt=2021-01-01";
        Record record = TestUtils.createRecordWithMetadata("abc", defaultTopic, 1, 1, Instant.ofEpochMilli(1L));

        Partition partition = Mockito.mock(Partition.class);
        Mockito.when(partition.getTopic()).thenReturn(defaultTopic);
        Mockito.when(partition.getPath()).thenReturn(Paths.get(defaultTopic, date));
        Mockito.when(partition.toString()).thenReturn(partitionPathString);

        Mockito.when(partitionFactory.getPartition(any(Record.class))).thenReturn(partition);
        Mockito.when(localFileWriter1.getFullPath()).thenReturn("test");
        Mockito.when(localStorage.createLocalFileWriter(Paths.get(partitionPathString))).thenReturn(localFileWriter1);
        Mockito.when(localFileWriter1.write(record)).thenReturn(true);
        try (WriterOrchestrator writerOrchestrator = new WriterOrchestrator(localStorage, objectStorage, statsDReporter)) {
            String path = writerOrchestrator.write(record);
            verify(partitionFactory, Mockito.times(1)).getPartition(record);
            Assert.assertEquals("test", path);
        }
    }

    @Test
    public void shouldCreateMultipleWriterBasedOnPartition() throws Exception {
        String date1 = "dt=2021-01-01";
        String date2 = "dt=2021-01-02";
        String partitionPathString1 = "default/dt=2021-01-01";
        String partitionPathString2 = "default/dt=2021-01-02";

        Partition partition1 = Mockito.mock(Partition.class);
        Mockito.when(partition1.getTopic()).thenReturn(defaultTopic);
        Mockito.when(partition1.getPath()).thenReturn(Paths.get(defaultTopic, date1));
        Mockito.when(partition1.toString()).thenReturn(partitionPathString1);

        Partition partition2 = Mockito.mock(Partition.class);
        Mockito.when(partition2.getTopic()).thenReturn(defaultTopic);
        Mockito.when(partition2.getPath()).thenReturn(Paths.get(defaultTopic, date2));
        Mockito.when(partition2.toString()).thenReturn(partitionPathString2);

        Instant timestamp1 = Instant.parse("2020-01-01T10:00:00.000Z");
        Record record1 = TestUtils.createRecordWithMetadata("abc", "default", 1, 1, timestamp1);
        Mockito.when(partitionFactory.getPartition(record1)).thenReturn(partition1);
        Mockito.when(localFileWriter1.getFullPath()).thenReturn("test1");
        Mockito.when(localStorage.createLocalFileWriter(Paths.get(partitionPathString1))).thenReturn(localFileWriter1);
        Mockito.when(localFileWriter1.write(record1)).thenReturn(true);

        Instant timestamp2 = Instant.parse("2020-01-02T10:00:00.000Z");
        Record record2 = TestUtils.createRecordWithMetadata("abc", "default", 1, 1, timestamp2);
        Mockito.when(partitionFactory.getPartition(record2)).thenReturn(partition2);
        Mockito.when(localFileWriter2.getFullPath()).thenReturn("test2");
        Mockito.when(localStorage.createLocalFileWriter(Paths.get(partitionPathString2))).thenReturn(localFileWriter2);
        Mockito.when(localFileWriter2.write(record2)).thenReturn(true);

        try (WriterOrchestrator writerOrchestrator = new WriterOrchestrator(localStorage, objectStorage, statsDReporter)) {
            Set<String> paths = new HashSet<>();
            paths.add(writerOrchestrator.write(record1));
            paths.add(writerOrchestrator.write(record2));
            verify(partitionFactory, Mockito.times(2)).getPartition(any(Record.class));
            assertEquals(2, paths.size());
        }
    }

    @Test(expected = IOException.class)
    public void shouldThrowIOExceptionWhenWriteThrowsException() throws Exception {
        String date = "dt=2021-01-01";
        String partition = "default/dt=2021-01-01";

        Partition partitionPath = Mockito.mock(Partition.class);
        Mockito.when(partitionPath.getTopic()).thenReturn(defaultTopic);
        Mockito.when(partitionPath.getPath()).thenReturn(Paths.get(defaultTopic, date));
        Mockito.when(partitionPath.toString()).thenReturn(partition);

        Record record = TestUtils.createRecordWithMetadata("abc", "default", 1, 1, Instant.now());
        Mockito.when(partitionFactory.getPartition(record)).thenReturn(partitionPath);
        Mockito.when(localFileWriter1.getFullPath()).thenReturn("test1");
        Mockito.when(localStorage.createLocalFileWriter(Paths.get(partition))).thenReturn(localFileWriter1);
        try (WriterOrchestrator writerOrchestrator = new WriterOrchestrator(localStorage, objectStorage, statsDReporter)) {
            Mockito.doThrow(new IOException("")).when(localFileWriter1).write(record);
            writerOrchestrator.write(record);
        }
    }

    @Test
    public void shouldThrowIOExceptionWhenOpenNewWriterFailed() throws Exception {
        String date = "dt=2021-01-01";
        String partition = "default/dt=2021-01-01";

        Partition partitionPath = Mockito.mock(Partition.class);
        Mockito.when(partitionPath.getTopic()).thenReturn(defaultTopic);
        Mockito.when(partitionPath.getPath()).thenReturn(Paths.get(defaultTopic, date));
        Mockito.when(partitionPath.toString()).thenReturn(partition);

        expectedException.expect(LocalFileWriterFailedException.class);
        Record record = TestUtils.createRecordWithMetadata("abc", "default", 1, 1, Instant.now());
        Mockito.when(partitionFactory.getPartition(record)).thenReturn(partitionPath);
        Mockito.when(localFileWriter1.getFullPath()).thenReturn("test1");
        Mockito.when(localStorage.createLocalFileWriter(Paths.get(partition))).thenThrow(new LocalFileWriterFailedException(new IOException("Some error")));
        try (WriterOrchestrator writerOrchestrator = new WriterOrchestrator(localStorage, objectStorage, statsDReporter)) {
            writerOrchestrator.write(record);
        }
    }
}
