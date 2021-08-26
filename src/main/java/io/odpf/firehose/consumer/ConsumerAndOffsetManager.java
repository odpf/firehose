package io.odpf.firehose.consumer;

import io.odpf.firehose.config.KafkaConsumerConfig;
import io.odpf.firehose.consumer.offset.OffsetManager;
import io.odpf.firehose.filter.FilterException;
import io.odpf.firehose.metrics.Instrumentation;
import io.odpf.firehose.sink.Sink;

import java.io.IOException;
import java.util.List;

/**
 * This class have APIs to read from kafka and also provide offset management.
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
 * consumerOffsetManager.commit() commits offsets returned from sink if the sink can manages its own offsets
 * otherwise it commits offsets added to this class.
 */
public class ConsumerAndOffsetManager implements AutoCloseable {
    private static final String SYNC_BATCH_KEY = "sync_batch_key";
    private final OffsetManager offsetManager = new OffsetManager();
    private final List<Sink> sinks;
    private final GenericConsumer genericConsumer;
    private final KafkaConsumerConfig kafkaConsumerConfig;
    private final Instrumentation instrumentation;
    private final boolean canSinkManageOffsets;

    public ConsumerAndOffsetManager(
            List<Sink> sinks,
            GenericConsumer genericConsumer,
            KafkaConsumerConfig kafkaConsumerConfig,
            Instrumentation instrumentation) {
        this.sinks = sinks;
        this.genericConsumer = genericConsumer;
        this.kafkaConsumerConfig = kafkaConsumerConfig;
        this.instrumentation = instrumentation;
        this.canSinkManageOffsets = sinks.get(0).canManageOffsets();
    }

    public void addOffsets(Object key, List<Message> messages) {
        offsetManager.addOffsetToBatch(key, messages);
    }


    public void addOffsetsAndSetCommittable(List<Message> messages) {
        if (!canSinkManageOffsets) {
            addOffsets(SYNC_BATCH_KEY, messages);
            setCommittable(SYNC_BATCH_KEY);
        }
    }

    public void setCommittable(Object key) {
        offsetManager.setCommittable(key);
    }

    public List<Message> readMessagesFromKafka() throws FilterException {
        return genericConsumer.readMessages();
    }

    public void commit() {
        if (kafkaConsumerConfig.isSourceKafkaCommitOnlyCurrentPartitionsEnable()) {
            if (canSinkManageOffsets) {
                sinks.forEach(s -> genericConsumer.commit(s.getCommittableOffsets()));
            } else {
                genericConsumer.commit(offsetManager.getCommittableOffset());
            }
        } else {
            genericConsumer.commit();
        }
    }

    @Override
    public void close() throws IOException {
        if (genericConsumer != null) {
            instrumentation.logInfo("closing consumer");
            genericConsumer.close();
        }
    }

}
