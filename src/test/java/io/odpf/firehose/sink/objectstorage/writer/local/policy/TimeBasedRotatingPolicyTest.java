package io.odpf.firehose.sink.objectstorage.writer.local.policy;

import io.odpf.firehose.sink.objectstorage.writer.local.LocalParquetFileWriter;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.junit.Assert.*;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TimeBasedRotatingPolicyTest {

    @Mock
    private LocalParquetFileWriter fileWriter;

    private TimeBasedRotatingPolicy rotatingPolicy = new TimeBasedRotatingPolicy(200);

    @Test
    public void shouldRotateWhenElapsedTimeGreaterThanMaxRotatingDuration() throws InterruptedException {
        long createdTimestamp = System.currentTimeMillis();
        when(fileWriter.getCreatedTimestampMillis()).thenReturn(createdTimestamp);
        Thread.sleep(300);
        boolean shouldRotate = rotatingPolicy.shouldRotate(fileWriter);
        assertTrue(shouldRotate);
    }

    @Test
    public void shouldNotRotateWhenElapsedTimeLessThanMaxRotatingDuration() throws InterruptedException {
        long createdTimestamp = System.currentTimeMillis();
        when(fileWriter.getCreatedTimestampMillis()).thenReturn(createdTimestamp);
        Thread.sleep(100);

        boolean shouldRotate = rotatingPolicy.shouldRotate(fileWriter);
        assertFalse(shouldRotate);
    }
}
