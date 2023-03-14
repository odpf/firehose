package com.gotocompany.firehose.sink;

import com.gotocompany.depot.Sink;
import com.gotocompany.depot.SinkResponse;
import com.gotocompany.firehose.exception.DeserializerException;
import com.gotocompany.firehose.message.FirehoseMessageUtils;
import com.gotocompany.firehose.message.Message;
import com.gotocompany.firehose.metrics.FirehoseInstrumentation;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class GenericSink extends AbstractSink {
    private final List<Message> messageList = new ArrayList<>();
    private final Sink sink;

    public GenericSink(FirehoseInstrumentation firehoseInstrumentation, String sinkType, Sink sink) {
        super(firehoseInstrumentation, sinkType);
        this.sink = sink;
    }

    @Override
    protected List<Message> execute() throws Exception {
        List<com.gotocompany.depot.message.Message> messages = FirehoseMessageUtils.convertToDepotMessage(messageList);
        SinkResponse response = sink.pushToSink(messages);
        return response.getErrors().keySet().stream()
                .map(index -> {
                    Message message = messageList.get(index.intValue());
                    message.setErrorInfo(response.getErrorsFor(index));
                    return message;
                }).collect(Collectors.toList());
    }

    @Override
    protected void prepare(List<Message> messages) throws DeserializerException, IOException, SQLException {
        messageList.clear();
        messageList.addAll(messages);
    }

    @Override
    public void close() throws IOException {

    }
}
