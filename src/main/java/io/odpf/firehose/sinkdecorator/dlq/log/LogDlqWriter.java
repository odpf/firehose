package io.odpf.firehose.sinkdecorator.dlq.log;

import io.odpf.firehose.error.ErrorInfo;
import io.odpf.firehose.consumer.Message;
import io.odpf.firehose.metrics.Instrumentation;
import io.odpf.firehose.sinkdecorator.dlq.DlqWriter;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

public class LogDlqWriter implements DlqWriter {
    private final Instrumentation instrumentation;

    public LogDlqWriter(Instrumentation instrumentation) {
        this.instrumentation = instrumentation;
    }

    @Override
    public List<Message> write(List<Message> messages) throws IOException {
        for (Message message : messages) {
            String key = new String(message.getLogKey());
            String value = new String(message.getLogMessage());

            String error = "";
            ErrorInfo errorInfo = message.getErrorInfo();
            if (errorInfo != null) {
                if (errorInfo.getException() != null) {
                    error = ExceptionUtils.getStackTrace(errorInfo.getException());
                }
            }

            instrumentation.logInfo("key: {}\nvalue: {}\nerror: {}", key, value, error);
        }
        return new LinkedList<>();
    }

}
