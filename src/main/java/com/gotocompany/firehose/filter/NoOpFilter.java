package com.gotocompany.firehose.filter;

import com.gotocompany.firehose.message.Message;
import com.gotocompany.firehose.metrics.FirehoseInstrumentation;

import java.util.List;

public class NoOpFilter implements Filter {

    public NoOpFilter(FirehoseInstrumentation firehoseInstrumentation) {
        firehoseInstrumentation.logInfo("No filter is selected");
    }

    /**
     * The method used for filtering the messages.
     *
     * @param messages the protobuf records in binary format that are wrapped in {@link Message}
     * @return filtered messages.
     * @throws FilterException the filter exception
     */
    @Override
    public FilteredMessages filter(List<Message> messages) throws FilterException {
        FilteredMessages filteredMessages = new FilteredMessages();
        messages.forEach(filteredMessages::addToValidMessages);
        return filteredMessages;
    }
}
