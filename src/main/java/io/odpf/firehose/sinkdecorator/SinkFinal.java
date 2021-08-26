package io.odpf.firehose.sinkdecorator;

import io.odpf.firehose.consumer.Message;
import io.odpf.firehose.exception.DeserializerException;
import io.odpf.firehose.metrics.Instrumentation;
import io.odpf.firehose.metrics.Metrics;
import io.odpf.firehose.sink.Sink;

import java.io.IOException;
import java.util.List;

public class SinkFinal extends SinkDecorator {
    private final Instrumentation instrumentation;

    /**
     * Instantiates a new Sink decorator.
     *
     * @param sink wrapped sink object
     */

    public SinkFinal(Sink sink, Instrumentation instrumentation) {
        super(sink);
        this.instrumentation = instrumentation;
    }

    @Override
    public List<Message> pushMessage(List<Message> inputMessages) throws IOException, DeserializerException {
        List<Message> failedMessages = super.pushMessage(inputMessages);
        if (failedMessages.size() > 0) {
            instrumentation.logInfo("Ignoring messages {}", failedMessages.size());
            instrumentation.captureGlobalMessageMetrics(Metrics.MessageScope.IGNORED, failedMessages.size());
        }
        return failedMessages;
    }
}
