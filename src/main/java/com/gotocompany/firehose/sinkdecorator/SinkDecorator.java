package com.gotocompany.firehose.sinkdecorator;

import com.gotocompany.firehose.message.Message;
import com.gotocompany.firehose.exception.DeserializerException;
import com.gotocompany.firehose.sink.Sink;

import java.io.IOException;
import java.util.List;

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
    public void calculateCommittableOffsets() {
        sink.calculateCommittableOffsets();
    }

    @Override
    public boolean canManageOffsets() {
        return sink.canManageOffsets();
    }

    @Override
    public void addOffsetsAndSetCommittable(List<Message> messageList) {
        sink.addOffsetsAndSetCommittable(messageList);
    }
}
