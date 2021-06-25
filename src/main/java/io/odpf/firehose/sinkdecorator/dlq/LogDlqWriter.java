package io.odpf.firehose.sinkdecorator.dlq;

import io.odpf.firehose.consumer.Message;
import io.odpf.firehose.metrics.Instrumentation;

import java.io.IOException;
import java.util.List;

public class LogDlqWriter implements DlqWriter {
    private final Instrumentation instrumentation;

    public LogDlqWriter(Instrumentation instrumentation) {
        this.instrumentation = instrumentation;
    }

    @Override
    public List<Message> write(List<Message> messages) throws IOException {
        for (Message message : messages) {
            instrumentation.logInfo("key: {}\nvalue: {}", new String(message.getLogKey()), new String(message.getLogMessage()));
        }
        return null;
    }

}
