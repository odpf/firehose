package io.odpf.firehose.sink.bigquery.models;

import io.odpf.firehose.consumer.Message;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.Map;

@AllArgsConstructor
@Getter
public class Record {
    private final Message message;
    private final Map<String, Object> columns;

    public String getId() {
        return String.format("%s_%d_%d", message.getTopic(), message.getPartition(), message.getOffset());
    }

    public long getSize() {
        return (message.getLogKey() == null ? 0 : message.getLogKey().length) + (message.getLogMessage() == null ? 0 : message.getLogMessage().length);
    }
}
