package com.gotocompany.firehose.consumer;

import com.gotocompany.firehose.consumer.kafka.ConsumerAndOffsetManager;
import com.gotocompany.firehose.exception.FirehoseConsumerFailedException;
import com.gotocompany.firehose.message.Message;
import com.gotocompany.firehose.metrics.FirehoseInstrumentation;
import com.gotocompany.firehose.sink.SinkPool;
import com.gotocompany.firehose.filter.FilterException;
import com.gotocompany.firehose.filter.FilteredMessages;
import com.gotocompany.firehose.tracer.SinkTracer;
import io.opentracing.Span;
import lombok.AllArgsConstructor;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.Future;

import static com.gotocompany.firehose.metrics.Metrics.SOURCE_KAFKA_PARTITIONS_PROCESS_TIME_MILLISECONDS;

@AllArgsConstructor
public class FirehoseAsyncConsumer implements FirehoseConsumer {
    private final SinkPool sinkPool;
    private final SinkTracer tracer;
    private final ConsumerAndOffsetManager consumerAndOffsetManager;
    private final FirehoseFilter firehoseFilter;
    private final FirehoseInstrumentation firehoseInstrumentation;

    @Override
    public void process() {
        Instant beforeCall = Instant.now();
        try {
            List<Message> messages = consumerAndOffsetManager.readMessages();
            List<Span> spans = tracer.startTrace(messages);
            FilteredMessages filteredMessages = firehoseFilter.applyFilter(messages);
            if (filteredMessages.sizeOfInvalidMessages() > 0) {
                consumerAndOffsetManager.forceAddOffsetsAndSetCommittable(filteredMessages.getInvalidMessages());
            }
            if (filteredMessages.sizeOfValidMessages() > 0) {
                List<Message> validMessages = filteredMessages.getValidMessages();
                Future<List<Message>> scheduledTask = scheduleTask(validMessages);
                consumerAndOffsetManager.addOffsets(scheduledTask, validMessages);
            }
            sinkPool.fetchFinishedSinkTasks().forEach(consumerAndOffsetManager::setCommittable);
            consumerAndOffsetManager.commit();
            tracer.finishTrace(spans);
        } catch (FilterException e) {
            throw new FirehoseConsumerFailedException(e);
        } finally {
            firehoseInstrumentation.captureDurationSince(SOURCE_KAFKA_PARTITIONS_PROCESS_TIME_MILLISECONDS, beforeCall);
        }
    }

    private Future<List<Message>> scheduleTask(List<Message> messages) {
        while (true) {
            Future<List<Message>> scheduledTask = sinkPool.submitTask(messages);
            if (scheduledTask == null) {
                firehoseInstrumentation.logInfo("The Queue is full");
                sinkPool.fetchFinishedSinkTasks().forEach(consumerAndOffsetManager::setCommittable);
            } else {
                firehoseInstrumentation.logInfo("Adding sink task");
                return scheduledTask;
            }
        }
    }

    @Override
    public void close() throws IOException {
        sinkPool.close();
        consumerAndOffsetManager.close();
        tracer.close();
        firehoseInstrumentation.close();
    }
}
