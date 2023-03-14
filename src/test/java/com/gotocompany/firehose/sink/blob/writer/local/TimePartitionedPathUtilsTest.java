package com.gotocompany.firehose.sink.blob.writer.local;

import com.google.protobuf.DynamicMessage;
import com.gotocompany.firehose.config.BlobSinkConfig;
import com.gotocompany.firehose.sink.blob.Constants;
import com.gotocompany.firehose.sink.blob.TestProtoMessage;
import com.gotocompany.firehose.sink.blob.TestUtils;
import com.gotocompany.firehose.sink.blob.message.Record;
import com.gotocompany.firehose.sink.blob.writer.local.path.TimePartitionedPathUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;

import static org.junit.Assert.assertEquals;

@RunWith(MockitoJUnitRunner.class)
public class TimePartitionedPathUtilsTest {

    private final String zone = "UTC";
    private final String timeStampFieldName = TestProtoMessage.CREATED_TIME_FIELD_NAME;
    private final String datePrefix = "dt=";
    private final String hourPrefix = "hr=";
    private final Instant defaultTimestamp = Instant.parse("2020-01-01T10:00:00.000Z");
    private final int defaultOrderNumber = 100;
    private final long defaultOffset = 1L;
    private final int defaultPartition = 1;
    private final String defaultTopic = "booking-log";

    @Test
    public void shouldCreateDayPartitioningPath() {
        String kafkaMetadataFieldName = "";
        DynamicMessage message = TestUtils.createMessage(defaultTimestamp, defaultOrderNumber);
        DynamicMessage metadata = TestUtils.createMetadata(kafkaMetadataFieldName, defaultTimestamp, defaultOffset, defaultPartition, defaultTopic);
        Record record = new Record(message, metadata);
        BlobSinkConfig sinkConfig = Mockito.mock(BlobSinkConfig.class);
        Mockito.when(sinkConfig.getFilePartitionProtoTimestampTimezone()).thenReturn(zone);
        Mockito.when(sinkConfig.getFilePartitionProtoTimestampFieldName()).thenReturn(timeStampFieldName);
        Mockito.when(sinkConfig.getFilePartitionTimeGranularityType()).thenReturn(Constants.FilePartitionType.DAY);
        Mockito.when(sinkConfig.getOutputKafkaMetadataColumnName()).thenReturn(kafkaMetadataFieldName);
        Mockito.when(sinkConfig.getFilePartitionTimeDatePrefix()).thenReturn("date=");
        Mockito.when(sinkConfig.getFilePartitionTimeHourPrefix()).thenReturn("");
        Path path = TimePartitionedPathUtils.getTimePartitionedPath(record, sinkConfig);
        assertEquals(Paths.get("booking-log/date=2020-01-01"), path);
    }

    @Test
    public void shouldCreateHourPartitioningPath() {
        String kafkaMetadataFieldName = "";
        DynamicMessage message = TestUtils.createMessage(defaultTimestamp, defaultOrderNumber);
        DynamicMessage metadata = TestUtils.createMetadata(kafkaMetadataFieldName, defaultTimestamp, defaultOffset, defaultPartition, defaultTopic);
        Record record = new Record(message, metadata);
        BlobSinkConfig sinkConfig = Mockito.mock(BlobSinkConfig.class);
        Mockito.when(sinkConfig.getFilePartitionProtoTimestampTimezone()).thenReturn(zone);
        Mockito.when(sinkConfig.getOutputKafkaMetadataColumnName()).thenReturn(kafkaMetadataFieldName);
        Mockito.when(sinkConfig.getFilePartitionProtoTimestampFieldName()).thenReturn(timeStampFieldName);
        Mockito.when(sinkConfig.getFilePartitionTimeGranularityType()).thenReturn(Constants.FilePartitionType.HOUR);
        Mockito.when(sinkConfig.getFilePartitionTimeDatePrefix()).thenReturn(datePrefix);
        Mockito.when(sinkConfig.getFilePartitionTimeHourPrefix()).thenReturn(hourPrefix);
        Path path = TimePartitionedPathUtils.getTimePartitionedPath(record, sinkConfig);
        assertEquals(Paths.get("booking-log/dt=2020-01-01/hr=10"), path);
    }

    @Test
    public void shouldCreateNonePartitioningPath() {
        String kafkaMetadataFieldName = "";
        DynamicMessage message = TestUtils.createMessage(defaultTimestamp, defaultOrderNumber);
        DynamicMessage metadata = TestUtils.createMetadata(kafkaMetadataFieldName, defaultTimestamp, defaultOffset, defaultPartition, defaultTopic);
        Record record = new Record(message, metadata);
        BlobSinkConfig sinkConfig = Mockito.mock(BlobSinkConfig.class);
        Mockito.when(sinkConfig.getFilePartitionProtoTimestampTimezone()).thenReturn(zone);
        Mockito.when(sinkConfig.getOutputKafkaMetadataColumnName()).thenReturn(kafkaMetadataFieldName);
        Mockito.when(sinkConfig.getFilePartitionProtoTimestampFieldName()).thenReturn(timeStampFieldName);
        Mockito.when(sinkConfig.getFilePartitionTimeGranularityType()).thenReturn(Constants.FilePartitionType.NONE);
        Mockito.when(sinkConfig.getFilePartitionTimeDatePrefix()).thenReturn(datePrefix);
        Mockito.when(sinkConfig.getFilePartitionTimeHourPrefix()).thenReturn(hourPrefix);
        Path path = TimePartitionedPathUtils.getTimePartitionedPath(record, sinkConfig);
        assertEquals(Paths.get("booking-log"), path);
    }

    @Test
    public void shouldCreatePartitionPathWhenKafkaMetadataIsNotNested() {
        String kafkaMetadataFieldName = "";
        DynamicMessage message = TestUtils.createMessage(defaultTimestamp, defaultOrderNumber);
        DynamicMessage metadata = TestUtils.createMetadata(kafkaMetadataFieldName, defaultTimestamp, defaultOffset, defaultPartition, defaultTopic);
        Record record = new Record(message, metadata);
        BlobSinkConfig sinkConfig = Mockito.mock(BlobSinkConfig.class);
        Mockito.when(sinkConfig.getFilePartitionProtoTimestampTimezone()).thenReturn(zone);
        Mockito.when(sinkConfig.getFilePartitionTimeGranularityType()).thenReturn(Constants.FilePartitionType.DAY);
        Mockito.when(sinkConfig.getFilePartitionProtoTimestampFieldName()).thenReturn(timeStampFieldName);
        Mockito.when(sinkConfig.getFilePartitionTimeDatePrefix()).thenReturn(datePrefix);
        Mockito.when(sinkConfig.getFilePartitionTimeHourPrefix()).thenReturn(hourPrefix);
        Mockito.when(sinkConfig.getOutputKafkaMetadataColumnName()).thenReturn(kafkaMetadataFieldName);
        Path path = TimePartitionedPathUtils.getTimePartitionedPath(record, sinkConfig);
        assertEquals(Paths.get("booking-log/dt=2020-01-01"), path);
    }

    @Test
    public void shouldCreatePartitioningPathForNestedKafkaMetadata() {
        String kafkaMetadataFieldName = "meta";
        DynamicMessage message = TestUtils.createMessage(defaultTimestamp, defaultOrderNumber);
        DynamicMessage metadata = TestUtils.createMetadata(kafkaMetadataFieldName, defaultTimestamp, defaultOffset, defaultPartition, defaultTopic);
        Record record = new Record(message, metadata);
        BlobSinkConfig sinkConfig = Mockito.mock(BlobSinkConfig.class);
        Mockito.when(sinkConfig.getFilePartitionProtoTimestampTimezone()).thenReturn(zone);
        Mockito.when(sinkConfig.getFilePartitionTimeGranularityType()).thenReturn(Constants.FilePartitionType.DAY);
        Mockito.when(sinkConfig.getFilePartitionProtoTimestampFieldName()).thenReturn(timeStampFieldName);
        Mockito.when(sinkConfig.getFilePartitionTimeDatePrefix()).thenReturn(datePrefix);
        Mockito.when(sinkConfig.getFilePartitionTimeHourPrefix()).thenReturn(hourPrefix);
        Mockito.when(sinkConfig.getOutputKafkaMetadataColumnName()).thenReturn(kafkaMetadataFieldName);
        Path path = TimePartitionedPathUtils.getTimePartitionedPath(record, sinkConfig);
        assertEquals(Paths.get("booking-log/dt=2020-01-01"), path);
    }
}
