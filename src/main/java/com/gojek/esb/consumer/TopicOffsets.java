package com.gojek.esb.consumer;

import com.gojek.esb.config.KafkaConsumerConfig;
import com.gojek.esb.metrics.Instrumentation;
import lombok.AllArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import static com.gojek.esb.metrics.Metrics.SOURCE_KAFKA_MESSAGES_COMMIT_TOTAL;
import static com.gojek.esb.metrics.Metrics.FAILURE_TAG;
import static com.gojek.esb.metrics.Metrics.SUCCESS_TAG;

@AllArgsConstructor
public class TopicOffsets implements Offsets {

    private KafkaConsumer kafkaConsumer;
    private KafkaConsumerConfig kafkaConsumerConfig;
    private Instrumentation instrumentation;

    @Override
    public void commit(ConsumerRecords<byte[], byte[]> records) {
        if (kafkaConsumerConfig.isSourceKafkaAsyncCommitEnable()) {
            commitAsync();
        } else {
            kafkaConsumer.commitSync();
        }
    }

    private void commitAsync() {
        kafkaConsumer.commitAsync((offsets, exception) -> {
            if (exception != null) {
                instrumentation.incrementCounterWithTags(SOURCE_KAFKA_MESSAGES_COMMIT_TOTAL, FAILURE_TAG);
            } else {
                instrumentation.incrementCounterWithTags(SOURCE_KAFKA_MESSAGES_COMMIT_TOTAL, SUCCESS_TAG);
            }
        });
    }
}
