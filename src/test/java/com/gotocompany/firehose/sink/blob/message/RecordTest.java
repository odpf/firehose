package com.gotocompany.firehose.sink.blob.message;

import com.google.protobuf.DynamicMessage;
import com.gotocompany.firehose.sink.blob.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.time.Instant;

public class RecordTest {

    private final Instant defaultTimestamp = Instant.parse("2020-01-01T10:00:00.000Z");
    private final int defaultOrderNumber = 100;
    private final long defaultOffset = 1L;
    private final int defaultPartition = 1;
    private final String defaultTopic = "booking-log";


    @Test
    public void shouldGetTopicFromMetaData() {
        DynamicMessage message = TestUtils.createMessage(defaultTimestamp, defaultOrderNumber);
        DynamicMessage metadata = TestUtils.createMetadata("", defaultTimestamp, defaultOffset, defaultPartition, defaultTopic);
        Record record = new Record(message, metadata);
        Assert.assertEquals("booking-log", record.getTopic(""));
    }

    @Test
    public void shouldGetTopicFromNestedMetaData() {
        DynamicMessage message = TestUtils.createMessage(defaultTimestamp, defaultOrderNumber);
        DynamicMessage metadata = TestUtils.createMetadata("nested_field", defaultTimestamp, defaultOffset, defaultPartition, defaultTopic);
        Record record = new Record(message, metadata);
        Assert.assertEquals("booking-log", record.getTopic("nested_field"));
    }

    @Test
    public void shouldGetTimeStampFromMessage() {
        DynamicMessage message = TestUtils.createMessage(defaultTimestamp, defaultOrderNumber);
        DynamicMessage metadata = TestUtils.createMetadata("nested_field", defaultTimestamp, defaultOffset, defaultPartition, defaultTopic);
        Record record = new Record(message, metadata);
        Assert.assertEquals(defaultTimestamp, record.getTimestamp("created_time"));
    }
}
