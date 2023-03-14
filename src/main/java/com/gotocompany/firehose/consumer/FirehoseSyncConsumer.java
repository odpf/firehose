package com.gotocompany.firehose.consumer;

import com.gotocompany.firehose.consumer.kafka.ConsumerAndOffsetManager;
import com.gotocompany.firehose.exception.FirehoseConsumerFailedException;
import com.gotocompany.firehose.message.Message;
import com.gotocompany.firehose.filter.FilterException;
import com.gotocompany.firehose.filter.FilteredMessages;
import com.gotocompany.firehose.metrics.FirehoseInstrumentation;
import com.gotocompany.firehose.sink.Sink;
import com.gotocompany.firehose.tracer.SinkTracer;
import io.opentracing.Span;
import lombok.AllArgsConstructor;

import java.io.IOException;
import java.time.Instant;
import java.util.List;

import static com.gotocompany.firehose.metrics.Metrics.SOURCE_KAFKA_PARTITIONS_PROCESS_TIME_MILLISECONDS;

/**
 * Firehose consumer reads messages from Generic consumer and pushes messages to the configured sink.
 */
@AllArgsConstructor
public class FirehoseSyncConsumer implements FirehoseConsumer {

    private final Sink sink;
    private final SinkTracer tracer;
    private final ConsumerAndOffsetManager consumerAndOffsetManager;
    private final FirehoseFilter firehoseFilter;
    private final FirehoseInstrumentation firehoseInstrumentation;

    @Override
    public void process() throws IOException {
        Instant beforeCall = Instant.now();
        try {
            List<Message> messages = consumerAndOffsetManager.readMessages();
            List<Span> spans = tracer.startTrace(messages);
            FilteredMessages filteredMessages = firehoseFilter.applyFilter(messages);
            if (filteredMessages.sizeOfInvalidMessages() > 0) {
                consumerAndOffsetManager.forceAddOffsetsAndSetCommittable(filteredMessages.getInvalidMessages());
            }
            if (filteredMessages.sizeOfValidMessages() > 0) {
                sink.pushMessage(filteredMessages.getValidMessages());
                consumerAndOffsetManager.addOffsetsAndSetCommittable(filteredMessages.getValidMessages());
            }
            consumerAndOffsetManager.commit();
            firehoseInstrumentation.logInfo("Processed {} records in consumer", messages.size());
            tracer.finishTrace(spans);
        } catch (FilterException e) {
            throw new FirehoseConsumerFailedException(e);
        } finally {
            firehoseInstrumentation.captureDurationSince(SOURCE_KAFKA_PARTITIONS_PROCESS_TIME_MILLISECONDS, beforeCall);
        }
    }

    @Override
    public void close() throws IOException {
        tracer.close();
        consumerAndOffsetManager.close();
        firehoseInstrumentation.close();
        sink.close();
    }
}
