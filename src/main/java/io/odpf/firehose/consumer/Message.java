package io.odpf.firehose.consumer;


import io.odpf.firehose.error.ErrorInfo;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import org.apache.kafka.common.header.Headers;

import java.util.Base64;

/**
 * A class to hold a single protobuf message in binary format.
 */
@Getter
@EqualsAndHashCode
@AllArgsConstructor
public class Message {
    private byte[] logKey;
    private byte[] logMessage;
    private String topic;
    private int partition;
    private long offset;
    private Headers headers;
    private long timestamp;
    private long consumeTimestamp;
    @Setter
    private ErrorInfo errorInfo;

    /**
     * Instantiates a new Message.
     *
     * @param logKey     the log key
     * @param logMessage the log message
     * @param topic      the topic
     * @param partition  the partition
     * @param offset     the offset
     */
    public Message(byte[] logKey, byte[] logMessage, String topic, int partition, long offset) {
        this.logKey = logKey;
        this.logMessage = logMessage;
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
    }

    /**
     * Instantiates a new Message without providing errorType.
     *
     * @param logKey
     * @param logMessage
     * @param topic
     * @param partition
     * @param offset
     * @param headers
     * @param timestamp
     * @param consumeTimestamp
     */
    public Message(byte[] logKey, byte[] logMessage, String topic, int partition, long offset, Headers headers, long timestamp, long consumeTimestamp) {
        this(logKey, logMessage, topic, partition, offset, headers, timestamp, consumeTimestamp, null);
    }

    public Message(Message message, ErrorInfo errorInfo) {
        this(message.getLogKey(),
                message.getLogMessage(),
                message.getTopic(),
                message.getPartition(),
                message.getOffset(),
                message.getHeaders(),
                message.getTimestamp(),
                message.getConsumeTimestamp(),
                errorInfo);
    }

    /**
     * Gets serialized key.
     *
     * @return the serialized key
     */
    public String getSerializedKey() {
        return encodedSerializedStringFrom(logKey);
    }

    /**
     * Gets serialized message.
     *
     * @return the serialized message
     */
    public String getSerializedMessage() {
        return encodedSerializedStringFrom(logMessage);
    }

    private static String encodedSerializedStringFrom(byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            return "";
        }
        return new String(Base64.getEncoder().encode(bytes));
    }
}
