package io.odpf.firehose.sinkdecorator;

import io.odpf.firehose.consumer.Message;
import io.odpf.firehose.exception.DeserializerException;
import io.odpf.firehose.sink.Sink;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Sink decorator provides internal processing on the use provided sink type.
 */
public class SinkDecorator implements Sink {

    private final Sink sink;

    /**
     * Instantiates a new Sink decorator.
     *
     * @param sink wrapped sink object
     */
    public SinkDecorator(Sink sink) {
        this.sink = sink;
    }

    @Override
    public List<Message> pushMessage(List<Message> message) throws IOException, DeserializerException {
        return sink.pushMessage(message);
    }

    @Override
    public void close() throws IOException {
        sink.close();
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> getCommittableOffsets() {
        return sink.getCommittableOffsets();
    }

    @Override
    public boolean canManageOffsets() {
        return sink.canManageOffsets();
    }

    @Override
    public void addOffsets(Object key, List<Message> messages) {
        sink.addOffsets(key, messages);
    }

    @Override
    public void setCommittable(Object key) {
        sink.setCommittable(key);
    }
}
