package io.odpf.firehose.sink;

import io.odpf.firehose.consumer.Message;
import io.odpf.firehose.exception.DeserializerException;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * An interface for developing custom Sinks to FirehoseConsumer.
 */
public interface Sink extends Closeable {

    /**
     * method to write batch of messages read from kafka.
     * The logic of how to persist the data goes in here.
     * in the future this method should return Response object instead of list of messages
     * @param message list of {@see EsbMessage}
     * @return the list of failed messages
     * @throws IOException           in case of error conditions while persisting it to the custom sink.
     * @throws DeserializerException in case of problems with deserialising the message into a protobuf object.
     */
    List<Message> pushMessage(List<Message> message) throws IOException, DeserializerException;

    /**
     * Method that inform that sink is managing kafka offset to be committed by its own.
     * @return
     */
    default boolean canManageOffsets() {
        return false;
    }

    /**
     * Method to register kafka offsets that need to be committed.
     * This method should be implemented when sink manages the commit offsets by themself.
     * Or when {@link Sink#canManageOffsets()} return true
     * @param key
     * @param messageList
     */
    default void addOffsets(Object key, List<Message> messageList) {
    }

    /**
     * Method to mark the registered offsets ready to be committed.
     * This method should be implemented when sink manages the commit offsets by themself.
     * @param key
     */
    default void setCommittable(Object key) {
    }

    /**
     * Method to obtain offsets ready to be committed
     * This method should be implemented when sink manages the commit offsets by themself.
     * @return
     */
    default Map<TopicPartition, OffsetAndMetadata> getCommittableOffsets() {
        return new HashMap<>();
    }
}
