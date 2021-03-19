package io.odpf.firehose.filter;

import io.odpf.firehose.consumer.Message;

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
     * @throws FilterException
     */
    List<Message> filter(List<Message> messages) throws FilterException;
}
