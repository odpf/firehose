package com.gotocompany.firehose.sink.mongodb;

import com.gotocompany.firehose.message.Message;
import com.gotocompany.firehose.metrics.FirehoseInstrumentation;
import com.gotocompany.firehose.sink.mongodb.request.MongoRequestHandler;
import com.mongodb.bulk.BulkWriteError;
import com.mongodb.client.model.WriteModel;
import com.gotocompany.firehose.sink.AbstractSink;
import com.gotocompany.firehose.sink.mongodb.client.MongoSinkClient;
import org.bson.Document;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * MongoDB sink.
 * This class contains methods to create and execute a bulk request
 * and obtain a list of messages which failed to push to the sink.
 *
 * @since 0.1
 */
public class MongoSink extends AbstractSink {

    private final MongoRequestHandler mongoRequestHandler;
    private final List<WriteModel<Document>> requests = new ArrayList<>();
    private final MongoSinkClient mongoSinkClient;
    private List<Message> messages;

    /**
     * Instantiates a new Mongo sink.
     *
     * @param firehoseInstrumentation     the instrumentation
     * @param sinkType            the sink type
     * @param mongoRequestHandler the mongo request handler
     * @since 0.1
     */
    public MongoSink(FirehoseInstrumentation firehoseInstrumentation, String sinkType, MongoRequestHandler mongoRequestHandler,
                     MongoSinkClient mongoSinkClient) {
        super(firehoseInstrumentation, sinkType);
        this.mongoRequestHandler = mongoRequestHandler;
        this.mongoSinkClient = mongoSinkClient;
    }

    /**
     * This method gets the WriteModel request for each message from
     * the MongoRequestHandler and adds the WriteModel request of
     * each message to the list of requests, thus creating a
     * bulk request.
     *
     * @param messageList the list of messages to be sent to Mongo sink
     * @since 0.1
     */
    @Override
    protected void prepare(List<Message> messageList) {
        this.messages = messageList;
        requests.clear();
        messages.forEach(message -> requests.add(mongoRequestHandler.getRequest(message)));
    }

    /**
     * This method processes the bulk request and retrieves the BulkWriteErrors
     * whose status codes are not present in the retry status codes blacklist.
     * It then retrieves the message corresponding to that error and adds it to
     * a list which is returned for retry or DLQ operations.
     *
     * @return list of messages which failed to push to the MongoDB sink
     * but excluding those whose error codes were present in the
     * retry status codes blacklist.
     * @since 0.1
     */
    @Override
    protected List<Message> execute() {
        List<BulkWriteError> writeErrors = mongoSinkClient.processRequest(requests);
        return writeErrors.stream()
                .map(writeError -> messages.get(writeError.getIndex()))
                .collect(Collectors.toList());
    }

    /**
     * This method gets the WriteModel request for each message from
     * the MongoRequestHandler and adds the WriteModel request of
     * each message to the list of requests, thus creating a
     * bulk request.
     *
     * @since 0.1
     */
    @Override
    public void close() throws IOException {
        getFirehoseInstrumentation().logInfo("MongoDB connection closing");
        this.mongoSinkClient.close();
    }
}
