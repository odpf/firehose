package com.gotocompany.firehose.consumer;

import com.gotocompany.firehose.message.Message;
import com.gotocompany.firehose.filter.Filter;
import com.gotocompany.firehose.filter.FilterException;
import com.gotocompany.firehose.filter.FilteredMessages;
import com.gotocompany.firehose.metrics.FirehoseInstrumentation;
import com.gotocompany.firehose.metrics.Metrics;
import lombok.AllArgsConstructor;

import java.util.List;

@AllArgsConstructor
public class FirehoseFilter {
    private final Filter filter;
    private final FirehoseInstrumentation firehoseInstrumentation;

    public FilteredMessages applyFilter(List<Message> messages) throws FilterException {
        FilteredMessages filteredMessage = filter.filter(messages);
        int filteredMessageCount = filteredMessage.sizeOfInvalidMessages();
        if (filteredMessageCount > 0) {
            firehoseInstrumentation.captureFilteredMessageCount(filteredMessageCount);
            firehoseInstrumentation.captureGlobalMessageMetrics(Metrics.MessageScope.FILTERED, filteredMessageCount);
        }
        return filteredMessage;
    }
}
