package com.gotocompany.firehose.sink.mongodb.client;

import com.gotocompany.firehose.config.MongoSinkConfig;
import com.gotocompany.firehose.metrics.FirehoseInstrumentation;
import com.gotocompany.firehose.metrics.Metrics;
import com.mongodb.MongoBulkWriteException;
import com.mongodb.MongoClient;
import com.mongodb.MongoCommandException;
import com.mongodb.bulk.BulkWriteError;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.WriteModel;
import lombok.AllArgsConstructor;
import org.bson.Document;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * The Mongo Sink Client.
 * This class is responsible for creating and closing the MongoDB sink
 * as well as performing bulk writes to the MongoDB collection.
 * It also logs to the instrumentation whether the bulk write has
 * succeeded or failed, as well as the cause of the failures.
 *
 * @since 0.1
 */
@AllArgsConstructor
public class MongoSinkClient implements Closeable {

    private MongoCollection<Document> mongoCollection;
    private final FirehoseInstrumentation firehoseInstrumentation;
    private final List<Integer> mongoRetryStatusCodeBlacklist;
    private final MongoClient mongoClient;
    private final MongoSinkConfig mongoSinkConfig;

    /**
     * Instantiates a new Mongo sink client.
     *
     * @param mongoSinkConfig the mongo sink config
     * @param firehoseInstrumentation the instrumentation
     * @since 0.1
     */
    public MongoSinkClient(MongoSinkConfig mongoSinkConfig, FirehoseInstrumentation firehoseInstrumentation, MongoClient mongoClient) {
        this.mongoSinkConfig = mongoSinkConfig;
        this.firehoseInstrumentation = firehoseInstrumentation;
        this.mongoClient = mongoClient;
        mongoRetryStatusCodeBlacklist = MongoSinkClientUtil.getStatusCodesAsList(mongoSinkConfig.getSinkMongoRetryStatusCodeBlacklist());

    }

    /**
     * Creates the MongoDatabase and MongoCollection if they do not exist already.
     * and connects to the database and collection
     */
    public void prepare() {
        String databaseName = mongoSinkConfig.getSinkMongoDBName();
        String collectionName = mongoSinkConfig.getSinkMongoCollectionName();

        boolean doesDBExist = MongoSinkClientUtil.checkDatabaseExists(databaseName, mongoClient, firehoseInstrumentation);
        MongoDatabase database = mongoClient.getDatabase(databaseName);
        boolean doesCollectionExist = MongoSinkClientUtil.checkCollectionExists(collectionName, database, firehoseInstrumentation);
        if (!doesCollectionExist) {
            try {
                database.createCollection(collectionName);
            } catch (MongoCommandException e) {
                if (!doesDBExist) {
                    firehoseInstrumentation.logError("Failed to create database");
                }

                firehoseInstrumentation.logError("Failed to create collection. Cause: " + e.getErrorMessage());
                throw e;
            }
            if (!doesDBExist) {
                firehoseInstrumentation.logInfo("Database: " + databaseName + " was successfully created");
            }
            firehoseInstrumentation.logInfo("Collection: " + collectionName + " was successfully created");
        }
        mongoCollection = database.getCollection(collectionName);
        firehoseInstrumentation.logInfo("Successfully connected to Mongo namespace : " + mongoCollection.getNamespace().getFullName());
    }

    /**
     * Processes the bulk request list of WriteModel.
     * This method performs a bulk write operation on the MongoCollection
     * If bulk write succeeds, an empty list is returned
     * If bulk write fails, then failure count is logged to instrumentation
     * and returns a list of BulkWriteErrors, whose status codes are
     * not present in retry status code blacklist
     *
     * @param request the bulk request
     * @return the list of non-blacklisted Bulk Write errors, if any, else returns empty list
     * @since 0.1
     */
    public List<BulkWriteError> processRequest(List<WriteModel<Document>> request) {

        try {
            logResults(mongoCollection.bulkWrite(request), request.size());
            return Collections.emptyList();
        } catch (MongoBulkWriteException writeException) {
            firehoseInstrumentation.logWarn("Bulk request failed");
            List<BulkWriteError> writeErrors = writeException.getWriteErrors();

            logErrors(writeErrors);
            return writeErrors.stream()
                    .filter(writeError -> !mongoRetryStatusCodeBlacklist.contains(writeError.getCode()))
                    .collect(Collectors.toList());
        }
    }

    private void logResults(BulkWriteResult writeResult, int messageCount) {

        int totalWriteCount = writeResult.getInsertedCount() + writeResult.getModifiedCount() + writeResult.getUpserts().size();
        int failureCount = messageCount - totalWriteCount;
        int totalInsertedCount = writeResult.getInsertedCount() + writeResult.getUpserts().size();

        if (totalWriteCount == 0) {
            firehoseInstrumentation.logWarn("Bulk request failed");
        } else if (totalWriteCount == messageCount) {
            firehoseInstrumentation.logInfo("Bulk request succeeded");
        } else {
            firehoseInstrumentation.logWarn("Bulk request partially succeeded");
        }

        if (totalWriteCount != messageCount) {
            firehoseInstrumentation.logWarn("Bulk request failures count: {}", failureCount);
            if (mongoSinkConfig.isSinkMongoModeUpdateOnlyEnable()) {

                for (int i = 0; i < failureCount; i++) {
                    firehoseInstrumentation.incrementCounter(Metrics.SINK_MESSAGES_DROP_TOTAL, "cause=Primary Key value not found");
                }
                firehoseInstrumentation.logWarn("Some Messages were dropped because their Primary Key values had no matches");
            } else {
                for (int i = 0; i < failureCount; i++) {
                    firehoseInstrumentation.incrementCounter(Metrics.SINK_MESSAGES_DROP_TOTAL);
                }
            }
        }

        if (writeResult.wasAcknowledged()) {
            firehoseInstrumentation.logInfo("Bulk Write operation was successfully acknowledged");

        } else {
            firehoseInstrumentation.logWarn("Bulk Write operation was not acknowledged");
        }
        firehoseInstrumentation.logInfo(
                "Inserted Count = {}. Matched Count = {}. Deleted Count = {}. Updated Count = {}. Total Modified Count = {}",
                totalInsertedCount,
                writeResult.getMatchedCount(),
                writeResult.getDeletedCount(),
                writeResult.getModifiedCount(),
                totalWriteCount);

        for (int i = 0; i < totalInsertedCount; i++) {
            firehoseInstrumentation.incrementCounter(Metrics.SINK_MONGO_INSERTED_TOTAL);
        }
        for (int i = 0; i < writeResult.getModifiedCount(); i++) {
            firehoseInstrumentation.incrementCounter(Metrics.SINK_MONGO_UPDATED_TOTAL);
        }
        for (int i = 0; i < totalWriteCount; i++) {
            firehoseInstrumentation.incrementCounter(Metrics.SINK_MONGO_MODIFIED_TOTAL);
        }
    }

    /**
     * This method logs errors.
     * It also checks whether the status code of a bulk write error
     * belongs to blacklist or not. If so, then it logs that the
     * message has been dropped and will not be retried, due to
     * blacklisted status code.
     *
     * @param writeErrors the write errors
     * @since 0.1
     */
    private void logErrors(List<BulkWriteError> writeErrors) {

        writeErrors.stream()
                .filter(writeError -> mongoRetryStatusCodeBlacklist.contains(writeError.getCode()))
                .forEach(writeError -> {
                    firehoseInstrumentation.logWarn("Non-retriable error due to response status: {} is under blacklisted status code", writeError.getCode());
                    firehoseInstrumentation.incrementCounter(Metrics.SINK_MESSAGES_DROP_TOTAL, "cause=" + writeError.getMessage());
                    firehoseInstrumentation.logInfo("Message dropped because of status code: " + writeError.getCode());
                });

        firehoseInstrumentation.logWarn("Bulk request failed count: {}", writeErrors.size());
    }

    @Override
    public void close() throws IOException {
        mongoClient.close();
    }
}
