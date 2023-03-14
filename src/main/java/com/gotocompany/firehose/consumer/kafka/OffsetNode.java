package com.gotocompany.firehose.consumer.kafka;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

@Data
@AllArgsConstructor
public class OffsetNode {
    private TopicPartition topicPartition;
    private OffsetAndMetadata offsetAndMetadata;
    private boolean isCommittable;
    private boolean isRemovable;

    public OffsetNode(TopicPartition topicPartition, OffsetAndMetadata offsetAndMetadata) {
        this(topicPartition, offsetAndMetadata, false, false);
    }
}
