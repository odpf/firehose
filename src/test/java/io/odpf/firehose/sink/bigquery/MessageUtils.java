package io.odpf.firehose.sink.bigquery;

import com.google.api.client.util.DateTime;
import io.odpf.firehose.TestKeyBQ;
import io.odpf.firehose.TestMessageBQ;
import io.odpf.firehose.consumer.Message;

import java.sql.Date;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class MessageUtils {
    private long timestamp;
    private String topic;
    private int partition;
    private long offset;

    public MessageUtils() {
        this.topic = "default-topic";
        this.partition = 1;
        this.offset = 1;
        this.timestamp = Instant.now().toEpochMilli();
    }

    public MessageUtils withOffset(int value) {
        offset = value;
        return this;
    }

    public MessageUtils withPartition(int value) {
        partition = value;
        return this;
    }

    public MessageUtils withTopic(String value) {
        topic = value;
        return this;
    }

    public MessageUtils withOffsetInfo(OffsetInfo offsetInfo) {
        this.topic = offsetInfo.getTopic();
        this.partition = offsetInfo.getPartition();
        this.offset = offsetInfo.getOffset();
        this.timestamp = offsetInfo.getTimestamp();
        return this;
    }

    public Message createConsumerRecord(String orderNumber, String orderUrl, String orderDetails) {
        TestKeyBQ key = TestKeyBQ.newBuilder()
                .setOrderNumber(orderNumber)
                .setOrderUrl(orderUrl)
                .build();
        TestMessageBQ message = TestMessageBQ.newBuilder()
                .setOrderNumber(orderNumber)
                .setOrderUrl(orderUrl)
                .setOrderDetails(orderDetails)
                .build();
        return new Message(key.toByteArray(), message.toByteArray(), topic, partition, offset, null, timestamp, 0);
    }

    public Message createEmptyValueConsumerRecord(String orderNumber, String orderUrl) {
        TestKeyBQ key = TestKeyBQ.newBuilder()
                .setOrderNumber(orderNumber)
                .setOrderUrl(orderUrl)
                .build();
        return new Message(key.toByteArray(), null, topic, partition, offset);
    }

    public Map<String, Object> metadataColumns(OffsetInfo offsetInfo, Instant now) {
        Map<String, Object> metadataColumns = new HashMap<>();
        metadataColumns.put("message_partition", offsetInfo.getPartition());
        metadataColumns.put("message_offset", offsetInfo.getOffset());
        metadataColumns.put("message_topic", offsetInfo.getTopic());
        metadataColumns.put("message_timestamp", new DateTime(offsetInfo.getTimestamp()));
        metadataColumns.put("load_time", new DateTime(Date.from(now)));
        return metadataColumns;
    }

    public void assertMetadata(Map<String, Object> recordColumns, OffsetInfo offsetInfo, long nowEpochMillis) {
        assertEquals("partition metadata mismatch", recordColumns.get("message_partition"), offsetInfo.getPartition());
        assertEquals("offset metadata mismatch", recordColumns.get("message_offset"), offsetInfo.getOffset());
        assertEquals("topic metadata mismatch", recordColumns.get("message_topic"), offsetInfo.getTopic());
        assertEquals("message timestamp metadata mismatch", recordColumns.get("message_timestamp"), new DateTime(offsetInfo.getTimestamp()));
        assertEquals("load time metadata mismatch", recordColumns.get("load_time"), new DateTime(nowEpochMillis));
    }
}
