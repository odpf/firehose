package com.gotocompany.firehose.consumer.kafka;

import com.gotocompany.firehose.config.KafkaConsumerConfig;
import com.gotocompany.firehose.message.Message;
import com.gotocompany.firehose.metrics.FirehoseInstrumentation;
import com.gotocompany.firehose.sink.Sink;

import java.io.IOException;
import java.util.List;

/**
 * This class has APIs to read from kafka and also provide offset management.
 * There are 2 use cases for this class.
 * 1. FirehoseConsumer:
 * consumerOffsetManager.readMessagesFromKafka(); // Read messages from kafka.
 * consumerOffsetManager.addOffsetsAndSetCommittable(messages); // add offsets for messages.
 * consumerOffsetManager.commit(); // commit all committable offsets for all partitions.
 * <p>
 * 2. FirehoseAsyncConsumer:
 * consumerOffsetManager.readMessagesFromKafka();
 * consumerOffsetManager.addOffsets(key, messages);
 * consumerOffsetManager.setCommittable(key);
 * consumerOffsetManager.commit();
 * <p>
 * <p>
 * OffsetManager is shared between consumer and the sink.
 * So the offsets added there will be available here to commit.
 * <p>
 * consumerOffsetManager.commit() calls the sink method to calculate committable offsets.
 * then it fetches the offsets from offsetManager.getCommittableOffsets() and uses kafka api to commit.
 */
public class ConsumerAndOffsetManager implements AutoCloseable {
    private final OffsetManager offsetManager;
    private final List<Sink> sinks;
    private final FirehoseKafkaConsumer firehoseKafkaConsumer;
    private final KafkaConsumerConfig kafkaConsumerConfig;
    private final FirehoseInstrumentation firehoseInstrumentation;
    private final boolean canSinkManageOffsets;
    private long lastCommitTimeStamp = 0;

    public ConsumerAndOffsetManager(
            List<Sink> sinks,
            OffsetManager offsetManager,
            FirehoseKafkaConsumer firehoseKafkaConsumer,
            KafkaConsumerConfig kafkaConsumerConfig,
            FirehoseInstrumentation firehoseInstrumentation) {
        this.sinks = sinks;
        this.offsetManager = offsetManager;
        this.firehoseKafkaConsumer = firehoseKafkaConsumer;
        this.kafkaConsumerConfig = kafkaConsumerConfig;
        this.firehoseInstrumentation = firehoseInstrumentation;
        this.canSinkManageOffsets = sinks.get(0).canManageOffsets();
    }

    public void addOffsets(Object key, List<Message> messages) {
        if (!canSinkManageOffsets) {
            offsetManager.addOffsetToBatch(key, messages);
        }
    }

    public void setCommittable(Object key) {
        if (!canSinkManageOffsets) {
            offsetManager.setCommittable(key);
        }
    }

    public void addOffsetsAndSetCommittable(List<Message> messages) {
        if (!canSinkManageOffsets) {
            offsetManager.addOffsetsAndSetCommittable(messages);
        }
    }

    /**
     * Force-Update the offsets into offset manager regardless of sink managing the offsets.
     *
     * @param messages list of messages set to be committable
     */
    public void forceAddOffsetsAndSetCommittable(List<Message> messages) {
        offsetManager.addOffsetsAndSetCommittable(messages);
    }

    public List<Message> readMessages() {
        return firehoseKafkaConsumer.readMessages();
    }

    public void commit() {
        long currentTimeStamp = System.currentTimeMillis();
        if (currentTimeStamp - lastCommitTimeStamp > kafkaConsumerConfig.getSourceKafkaConsumerManualCommitMinIntervalMs()) {
            if (kafkaConsumerConfig.isSourceKafkaCommitOnlyCurrentPartitionsEnable()) {
                sinks.forEach(Sink::calculateCommittableOffsets);
                firehoseKafkaConsumer.commit(offsetManager.getCommittableOffset());
            } else {
                firehoseKafkaConsumer.commit();
            }
            lastCommitTimeStamp = currentTimeStamp;
        }
    }

    @Override
    public void close() throws IOException {
        if (firehoseKafkaConsumer != null) {
            firehoseInstrumentation.logInfo("closing consumer");
            firehoseKafkaConsumer.close();
        }
    }

}
