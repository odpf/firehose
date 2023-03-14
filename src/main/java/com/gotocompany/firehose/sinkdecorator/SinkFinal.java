package com.gotocompany.firehose.sinkdecorator;

import com.gotocompany.firehose.message.Message;
import com.gotocompany.firehose.exception.DeserializerException;
import com.gotocompany.firehose.metrics.FirehoseInstrumentation;
import com.gotocompany.firehose.metrics.Metrics;
import com.gotocompany.firehose.sink.Sink;

import java.io.IOException;
import java.util.List;

public class SinkFinal extends SinkDecorator {
    private final FirehoseInstrumentation firehoseInstrumentation;

    /**
     * Instantiates a new Sink decorator.
     *
     * @param sink wrapped sink object
     */

    public SinkFinal(Sink sink, FirehoseInstrumentation firehoseInstrumentation) {
        super(sink);
        this.firehoseInstrumentation = firehoseInstrumentation;
    }

    @Override
    public List<Message> pushMessage(List<Message> inputMessages) throws IOException, DeserializerException {
        List<Message> failedMessages = super.pushMessage(inputMessages);
        if (failedMessages.size() > 0) {
            firehoseInstrumentation.logInfo("Ignoring messages {}", failedMessages.size());
            firehoseInstrumentation.captureGlobalMessageMetrics(Metrics.MessageScope.IGNORED, failedMessages.size());
        }
        return failedMessages;
    }
}
