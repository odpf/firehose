package io.odpf.firehose.sink.bigquery;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

@EqualsAndHashCode
@ToString
@Getter
@AllArgsConstructor
public class OffsetInfo {
    private final String topic;
    private final int partition;
    private final long offset;
    private final long timestamp;

    public OffsetAndMetadata getOffsetMetadata() {
        return new OffsetAndMetadata(offset);
    }

    public TopicPartition getTopicPartition() {
        return new TopicPartition(topic, partition);
    }
}
