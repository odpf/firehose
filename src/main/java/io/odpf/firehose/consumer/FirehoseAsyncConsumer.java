package io.odpf.firehose.consumer;

import io.odpf.firehose.filter.FilterException;
import io.odpf.firehose.metrics.Instrumentation;
import io.odpf.firehose.tracer.SinkTracer;
import io.odpf.firehose.util.Clock;
import lombok.AllArgsConstructor;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.Future;

import static io.odpf.firehose.metrics.Metrics.SOURCE_KAFKA_PARTITIONS_PROCESS_TIME_MILLISECONDS;

@AllArgsConstructor
public class FirehoseAsyncConsumer implements KafkaConsumer {
    private final SinkPool sinkPool;
    private final Clock clock;
    private final SinkTracer tracer;
    private final ConsumerAndOffsetManager consumerAndOffsetManager;
    private final Instrumentation instrumentation;

    @Override
    public void process() throws FilterException {
        Instant beforeCall = clock.now();
        try {
            List<Message> messages = consumerAndOffsetManager.readMessagesFromKafka();
            if (!messages.isEmpty()) {
                Future<List<Message>> scheduledTask = scheduleTask(messages);
                consumerAndOffsetManager.addOffsets(scheduledTask, messages);
            }
            sinkPool.fetchFinishedSinkTasks().forEach(consumerAndOffsetManager::setCommittable);
            consumerAndOffsetManager.commit();
        } finally {
            instrumentation.captureDurationSince(SOURCE_KAFKA_PARTITIONS_PROCESS_TIME_MILLISECONDS, beforeCall);
        }
    }

    private Future<List<Message>> scheduleTask(List<Message> messages) {
        while (true) {
            Future<List<Message>> scheduledTask = sinkPool.submitTask(messages);
            if (scheduledTask == null) {
                instrumentation.logInfo("The Queue is full");
                sinkPool.fetchFinishedSinkTasks().forEach(consumerAndOffsetManager::setCommittable);
            } else {
                instrumentation.logInfo("Adding sink task");
                return scheduledTask;
            }
        }
    }

    @Override
    public void close() throws IOException {
        consumerAndOffsetManager.close();
        tracer.close();
        sinkPool.close();
        instrumentation.close();
    }
}
