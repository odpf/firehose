package com.gotocompany.firehose.sink;

import com.gotocompany.firehose.message.Message;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

/**
 * An interface for developing custom Sinks to FirehoseConsumer.
 */
public interface Sink extends Closeable {

    /**
     * method to write batch of messages read from kafka.
     * The logic of how to persist the data goes in here.
     * in the future this method should return Response object instead of list of messages
     *
     * @param message list of {@see EsbMessage}
     * @return the list of failed messages
     * @throws IOException           in case of error conditions while persisting it to the custom sink.
     */
    List<Message> pushMessage(List<Message> message) throws IOException;

    /**
     * Method that inform that sink is managing kafka offset to be committed by its own.
     *
     * @return
     */
    default boolean canManageOffsets() {
        return false;
    }

    /**
     * Method to register kafka offsets and setting it to be committed.
     * This method should be implemented when sink manages the commit offsets by themselves.
     * Or when {@link Sink#canManageOffsets()} return true
     *
     * @param messageList messages to be committed
     */
    default void addOffsetsAndSetCommittable(List<Message> messageList) {

    }

    /**
     * Method to calculate offsets ready to be committed.
     * This method should be implemented when sink manages the commit offsets by themselves.
     */
    default void calculateCommittableOffsets() {
    }
}
