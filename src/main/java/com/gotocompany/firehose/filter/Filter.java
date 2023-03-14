package com.gotocompany.firehose.filter;

import com.gotocompany.firehose.message.Message;

import java.util.List;

/**
 * Interface for filtering the messages.
 */
public interface Filter {

    /**
     * The method used for filtering the messages.
     *
     * @param messages the protobuf records in binary format that are wrapped in {@link Message}
     * @return filtered messages.
     * @throws FilterException the filter exception
     */
    FilteredMessages filter(List<Message> messages) throws FilterException;

}
