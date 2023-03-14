package com.gotocompany.firehose.sink.dlq;

import com.gotocompany.firehose.message.Message;

import java.io.IOException;
import java.util.List;

public interface DlqWriter {

    /**
     * Method to write messages to dead letter queues destination.
     * @param messages is collection of message that need to be sent to dead letter queue
     * @return collection of messages that failed to be processed
     * @throws IOException can be thrown for non retry able error
     */
    List<Message> write(List<Message> messages) throws IOException;
}
