package com.gotocompany.firehose.sink.mongodb.client;

import com.gotocompany.firehose.config.MongoSinkConfig;
import com.gotocompany.firehose.metrics.FirehoseInstrumentation;
import com.gotocompany.firehose.metrics.Metrics;
import com.mongodb.MongoBulkWriteException;
import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;
import com.mongodb.bulk.BulkWriteError;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.bulk.BulkWriteUpsert;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.WriteModel;
import org.bson.BsonDocument;
import org.bson.Document;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;

public class MongoSinkClientTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Mock
    private FirehoseInstrumentation firehoseInstrumentation;
    @Mock
    private MongoSinkConfig mongoSinkConfig;


    @Mock
    private MongoCollection<Document> mongoCollection;

    @Mock
    private MongoClient mongoClient;

    private List<WriteModel<Document>> request;
    private final List<Integer> mongoRetryStatusCodeBlacklist = new ArrayList<>();

    @Before
    public void setUp() {
        initMocks(this);

        mongoRetryStatusCodeBlacklist.add(11000);
        mongoRetryStatusCodeBlacklist.add(502);
        request = new ArrayList<>();

        request.add(new ReplaceOneModel<>(
                new Document("customer_id", "35452"),
                new Document(),
                new ReplaceOptions().upsert(true)));

        request.add(new ReplaceOneModel<>(
                new Document("customer_id", "35452"),
                new Document()));
    }

    @Test
    public void shouldReturnEmptyArrayListWhenBulkResponseExecutedSuccessfully() {
        MongoSinkClient mongoSinkClient = new MongoSinkClient(mongoCollection, firehoseInstrumentation,
                mongoRetryStatusCodeBlacklist, mongoClient, mongoSinkConfig);
        when(mongoCollection.bulkWrite(request)).thenReturn(new BulkWriteResultMock(true, 1, 1, 0));
        List<BulkWriteError> nonBlacklistedErrors = mongoSinkClient.processRequest(request);
        Assert.assertEquals(0, nonBlacklistedErrors.size());
    }


    @Test
    public void shouldReturnNonBlacklistedErrorsWhenBulkResponseHasFailuresAndEmptyBlacklist() {
        BulkWriteError writeError1 = new BulkWriteError(400, "DB not found", new BsonDocument(), 0);
        BulkWriteError writeError2 = new BulkWriteError(400, "DB not found", new BsonDocument(), 1);
        List<BulkWriteError> writeErrors = Arrays.asList(writeError1, writeError2);
        MongoSinkClient mongoSinkClient = new MongoSinkClient(mongoCollection, firehoseInstrumentation,
                new ArrayList<>(), mongoClient, mongoSinkConfig);

        when(mongoCollection.bulkWrite(request)).thenThrow(new MongoBulkWriteException(new BulkWriteResultMock(false, 0, 0, 0), writeErrors, null, new ServerAddress()));
        List<BulkWriteError> nonBlacklistedErrors = mongoSinkClient.processRequest(request);
        Assert.assertEquals(writeErrors.get(0), nonBlacklistedErrors.get(0));
        Assert.assertEquals(writeErrors.get(1), nonBlacklistedErrors.get(1));
    }

    @Test
    public void shouldReturnNonBlacklistedErrorsIfNoneOfTheFailuresBelongToBlacklist() {
        BulkWriteError writeError1 = new BulkWriteError(400, "DB not found", new BsonDocument(), 0);
        BulkWriteError writeError2 = new BulkWriteError(400, "DB not found", new BsonDocument(), 1);
        List<BulkWriteError> writeErrors = Arrays.asList(writeError1, writeError2);
        MongoSinkClient mongoSinkClient = new MongoSinkClient(mongoCollection, firehoseInstrumentation,
                mongoRetryStatusCodeBlacklist, mongoClient, mongoSinkConfig);

        when(mongoCollection.bulkWrite(request)).thenThrow(new MongoBulkWriteException(new BulkWriteResultMock(false, 0, 0, 0), writeErrors, null, new ServerAddress()));
        List<BulkWriteError> nonBlacklistedErrors = mongoSinkClient.processRequest(request);
        Assert.assertEquals(writeErrors.get(0), nonBlacklistedErrors.get(0));
        Assert.assertEquals(writeErrors.get(1), nonBlacklistedErrors.get(1));
    }

    @Test
    public void shouldReportTelemetryIfTheResponsesBelongToBlacklistStatusCode() {
        BulkWriteError writeError1 = new BulkWriteError(11000, "Duplicate Key Error", new BsonDocument(), 0);
        BulkWriteError writeError2 = new BulkWriteError(11000, "Duplicate Key Error", new BsonDocument(), 1);
        List<BulkWriteError> writeErrors = Arrays.asList(writeError1, writeError2);
        MongoSinkClient mongoSinkClient = new MongoSinkClient(mongoCollection, firehoseInstrumentation,
                mongoRetryStatusCodeBlacklist, mongoClient, mongoSinkConfig);
        when(mongoCollection.bulkWrite(request)).thenThrow(new MongoBulkWriteException(new BulkWriteResultMock(false, 0, 0, 0), writeErrors, null, new ServerAddress()));
        mongoSinkClient.processRequest(request);

        verify(firehoseInstrumentation, times(2)).logWarn("Non-retriable error due to response status: {} is under blacklisted status code", 11000);
        verify(firehoseInstrumentation, times(2)).logInfo("Message dropped because of status code: 11000");
        verify(firehoseInstrumentation, times(2)).incrementCounter("firehose_sink_messages_drop_total", "cause=Duplicate Key Error");
    }

    @Test
    public void shouldReportTelemetryIfSomeOfTheFailuresDontBelongToBlacklist() {
        BulkWriteError writeError1 = new BulkWriteError(400, "Duplicate Key Error", new BsonDocument(), 0);
        BulkWriteError writeError2 = new BulkWriteError(11000, "Duplicate Key Error", new BsonDocument(), 1);
        List<BulkWriteError> writeErrors = Arrays.asList(writeError1, writeError2);
        MongoSinkClient mongoSinkClient = new MongoSinkClient(mongoCollection, firehoseInstrumentation,
                mongoRetryStatusCodeBlacklist, mongoClient, mongoSinkConfig);
        when(mongoCollection.bulkWrite(request)).thenThrow(new MongoBulkWriteException(new BulkWriteResultMock(false, 0, 0, 0), writeErrors, null, new ServerAddress()));
        mongoSinkClient.processRequest(request);

        verify(firehoseInstrumentation, times(1)).logWarn("Non-retriable error due to response status: {} is under blacklisted status code", 11000);
        verify(firehoseInstrumentation, times(1)).logInfo("Message dropped because of status code: 11000");
        verify(firehoseInstrumentation, times(1)).incrementCounter("firehose_sink_messages_drop_total", "cause=Duplicate Key Error");
    }

    @Test
    public void shouldReturnNonBlacklistedErrorsIfSomeOfTheFailuresDontBelongToBlacklist() {
        BulkWriteError writeError1 = new BulkWriteError(11000, "Duplicate Key Error", new BsonDocument(), 0);
        BulkWriteError writeError2 = new BulkWriteError(400, "DB not found", new BsonDocument(), 1);
        BulkWriteError writeError3 = new BulkWriteError(502, "Collection not found", new BsonDocument(), 2);

        List<BulkWriteError> writeErrors = Arrays.asList(writeError1, writeError2, writeError3);
        MongoSinkClient mongoSinkClient = new MongoSinkClient(mongoCollection, firehoseInstrumentation,
                mongoRetryStatusCodeBlacklist, mongoClient, mongoSinkConfig);

        request.add(new ReplaceOneModel<>(
                new Document("customer_id", "35452"),
                new Document(),
                new ReplaceOptions().upsert(true)));

        when(mongoCollection.bulkWrite(request)).thenThrow(new MongoBulkWriteException(new BulkWriteResultMock(false, 0, 0, 0),
                writeErrors, null, new ServerAddress()));

        List<BulkWriteError> nonBlacklistedErrors = mongoSinkClient.processRequest(request);

        verify(firehoseInstrumentation, times(2)).incrementCounter(any(String.class), any(String.class));
        Assert.assertEquals(1, nonBlacklistedErrors.size());
        Assert.assertEquals(writeErrors.get(1), nonBlacklistedErrors.get(0));

    }

    @Test
    public void shouldLogBulkRequestFailedWhenBulkResponsesHasFailures() {
        BulkWriteError writeError1 = new BulkWriteError(11000, "Duplicate Key Error", new BsonDocument(), 0);
        BulkWriteError writeError2 = new BulkWriteError(11000, "Duplicate Key Error", new BsonDocument(), 1);
        List<BulkWriteError> writeErrors = Arrays.asList(writeError1, writeError2);
        MongoSinkClient mongoSinkClient = new MongoSinkClient(mongoCollection, firehoseInstrumentation,
                mongoRetryStatusCodeBlacklist, mongoClient, mongoSinkConfig);
        when(mongoCollection.bulkWrite(request)).thenThrow(new MongoBulkWriteException(new BulkWriteResultMock(false, 0, 0, 0), writeErrors, null, new ServerAddress()));
        mongoSinkClient.processRequest(request);
        verify(firehoseInstrumentation, times(1)).logWarn("Bulk request failed count: {}", 2);
    }

    @Test
    public void shouldNotLogBulkRequestFailedWhenBulkResponsesHasNoFailures() {
        List<BulkWriteError> writeErrors = new ArrayList<>();

        MongoSinkClient mongoSinkClient = new MongoSinkClient(mongoCollection, firehoseInstrumentation,
                mongoRetryStatusCodeBlacklist, mongoClient, mongoSinkConfig);
        when(mongoCollection.bulkWrite(request)).thenThrow(new MongoBulkWriteException(new BulkWriteResultMock(false, 0, 0, 0), writeErrors, null, new ServerAddress()));
        mongoSinkClient.processRequest(request);

        verify(firehoseInstrumentation, times(0)).logWarn("Bulk request failed count: {}", 2);
    }

    @Test
    public void shouldLogBulkRequestFailedWhenPrimaryKeyNotFoundForAllMessages() {
        MongoSinkClient mongoSinkClient = new MongoSinkClient(mongoCollection, firehoseInstrumentation,
                mongoRetryStatusCodeBlacklist, mongoClient, mongoSinkConfig);

        when(mongoSinkConfig.isSinkMongoModeUpdateOnlyEnable()).thenReturn(true);
        when(mongoCollection.bulkWrite(request)).thenReturn(new BulkWriteResultMock(true, 0, 0, 0));
        mongoSinkClient.processRequest(request);

        verify(firehoseInstrumentation, times(1)).logWarn("Bulk request failed");
        verify(firehoseInstrumentation, times(1)).logWarn("Bulk request failures count: {}", 2);
        verify(firehoseInstrumentation, times(1)).logWarn("Some Messages were dropped because their Primary Key values had no matches");
    }

    @Test
    public void shouldLogBulkRequestPartiallySucceededWhenPrimaryKeyNotFoundForSomeMessages() {
        MongoSinkClient mongoSinkClient = new MongoSinkClient(mongoCollection, firehoseInstrumentation,
                mongoRetryStatusCodeBlacklist, mongoClient, mongoSinkConfig);

        when(mongoSinkConfig.isSinkMongoModeUpdateOnlyEnable()).thenReturn(true);
        when(mongoCollection.bulkWrite(request)).thenReturn(new BulkWriteResultMock(true, 0, 1, 0));
        mongoSinkClient.processRequest(request);

        verify(firehoseInstrumentation, times(1)).logWarn("Bulk request partially succeeded");
        verify(firehoseInstrumentation, times(1)).logWarn("Bulk request failures count: {}", 1);
        verify(firehoseInstrumentation, times(1)).logWarn("Some Messages were dropped because their Primary Key values had no matches");
    }

    @Test
    public void shouldLogBulkRequestsNotAcknowledgedWhenNoAcknowledgementReceived() {
        MongoSinkClient mongoSinkClient = new MongoSinkClient(mongoCollection, firehoseInstrumentation,
                mongoRetryStatusCodeBlacklist, mongoClient, mongoSinkConfig);

        when(mongoSinkConfig.isSinkMongoModeUpdateOnlyEnable()).thenReturn(true);
        when(mongoCollection.bulkWrite(request)).thenReturn(new BulkWriteResultMock(false, 0, 1, 0));
        mongoSinkClient.processRequest(request);

        verify(firehoseInstrumentation, times(1)).logWarn("Bulk Write operation was not acknowledged");
    }

    @Test
    public void shouldLogBulkRequestsSucceededWhenNoFailuresForUpdateOnly() {
        MongoSinkClient mongoSinkClient = new MongoSinkClient(mongoCollection, firehoseInstrumentation,
                mongoRetryStatusCodeBlacklist, mongoClient, mongoSinkConfig);

        when(mongoSinkConfig.isSinkMongoModeUpdateOnlyEnable()).thenReturn(true);
        when(mongoCollection.bulkWrite(request)).thenReturn(new BulkWriteResultMock(true, 0, 2, 0));
        mongoSinkClient.processRequest(request);

        verify(firehoseInstrumentation, times(1)).logInfo("Bulk request succeeded");
        verify(firehoseInstrumentation, times(1)).logInfo("Bulk Write operation was successfully acknowledged");
        verify(firehoseInstrumentation, times(1)).logInfo(
                "Inserted Count = {}. Matched Count = {}. Deleted Count = {}. Updated Count = {}. Total Modified Count = {}",
                0, 2, 0, 2, 2);
    }

    @Test
    public void shouldLogBulkRequestsSucceededWhenNoFailuresForInsertOnly() {
        MongoSinkClient mongoSinkClient = new MongoSinkClient(mongoCollection, firehoseInstrumentation,
                mongoRetryStatusCodeBlacklist, mongoClient, mongoSinkConfig);

        when(mongoCollection.bulkWrite(request)).thenReturn(new BulkWriteResultMock(true, 2, 0, 0));
        mongoSinkClient.processRequest(request);

        verify(firehoseInstrumentation, times(1)).logInfo("Bulk request succeeded");
        verify(firehoseInstrumentation, times(1)).logInfo("Bulk Write operation was successfully acknowledged");

        verify(firehoseInstrumentation, times(1)).logInfo(
                "Inserted Count = {}. Matched Count = {}. Deleted Count = {}. Updated Count = {}. Total Modified Count = {}",
                2, 0, 0, 0, 2);
    }

    @Test
    public void shouldLogBulkRequestsSucceededWhenNoFailuresForBothUpdateAndInsert() {
        MongoSinkClient mongoSinkClient = new MongoSinkClient(mongoCollection, firehoseInstrumentation,
                mongoRetryStatusCodeBlacklist, mongoClient, mongoSinkConfig);

        when(mongoCollection.bulkWrite(request)).thenReturn(new BulkWriteResultMock(true, 1, 1, 0));
        mongoSinkClient.processRequest(request);

        verify(firehoseInstrumentation, times(1)).logInfo("Bulk request succeeded");
        verify(firehoseInstrumentation, times(1)).logInfo("Bulk Write operation was successfully acknowledged");
        verify(firehoseInstrumentation, times(1)).logInfo(
                "Inserted Count = {}. Matched Count = {}. Deleted Count = {}. Updated Count = {}. Total Modified Count = {}",
                1, 1, 0, 1, 2);

    }

    @Test
    public void shouldIncrementFailureCounterTagWhenPrimaryKeyNotFoundInUpdateOnlyMode() {

        MongoSinkClient mongoSinkClient = new MongoSinkClient(mongoCollection, firehoseInstrumentation,
                mongoRetryStatusCodeBlacklist, mongoClient, mongoSinkConfig);
        when(mongoSinkConfig.isSinkMongoModeUpdateOnlyEnable()).thenReturn(true);
        when(mongoCollection.bulkWrite(request)).thenReturn(new BulkWriteResultMock(true, 0, 1, 0));
        mongoSinkClient.processRequest(request);

        verify(firehoseInstrumentation, times(1)).incrementCounter(Metrics.SINK_MESSAGES_DROP_TOTAL, "cause=Primary Key value not found");
    }

    @Test
    public void shouldIncrementInsertedCounterTagOnSuccessfulInsertion() {

        MongoSinkClient mongoSinkClient = new MongoSinkClient(mongoCollection, firehoseInstrumentation,
                mongoRetryStatusCodeBlacklist, mongoClient, mongoSinkConfig);

        when(mongoCollection.bulkWrite(request)).thenReturn(new BulkWriteResultMock(true, 3, 0, 0));
        mongoSinkClient.processRequest(request);

        verify(firehoseInstrumentation, times(3)).incrementCounter(Metrics.SINK_MONGO_INSERTED_TOTAL);
        verify(firehoseInstrumentation, times(3)).incrementCounter(Metrics.SINK_MONGO_MODIFIED_TOTAL);

    }

    @Test
    public void shouldIncrementUpdatedCounterTagOnSuccessfulUpdation() {

        MongoSinkClient mongoSinkClient = new MongoSinkClient(mongoCollection, firehoseInstrumentation,
                mongoRetryStatusCodeBlacklist, mongoClient, mongoSinkConfig);

        when(mongoSinkConfig.isSinkMongoModeUpdateOnlyEnable()).thenReturn(true);
        when(mongoCollection.bulkWrite(request)).thenReturn(new BulkWriteResultMock(true, 0, 3, 0));
        mongoSinkClient.processRequest(request);

        verify(firehoseInstrumentation, times(3)).incrementCounter(Metrics.SINK_MONGO_UPDATED_TOTAL);
        verify(firehoseInstrumentation, times(3)).incrementCounter(Metrics.SINK_MONGO_MODIFIED_TOTAL);
    }

    @Test
    public void shouldIncrementInsertedCounterTagOnSuccessfulInsertionInUpsertMode() {

        MongoSinkClient mongoSinkClient = new MongoSinkClient(mongoCollection, firehoseInstrumentation,
                mongoRetryStatusCodeBlacklist, mongoClient, mongoSinkConfig);

        when(mongoSinkConfig.isSinkMongoModeUpdateOnlyEnable()).thenReturn(true);
        when(mongoCollection.bulkWrite(request)).thenReturn(new BulkWriteResultMock(true, 0, 0, 3));
        mongoSinkClient.processRequest(request);

        verify(firehoseInstrumentation, times(3)).incrementCounter(Metrics.SINK_MONGO_INSERTED_TOTAL);
        verify(firehoseInstrumentation, times(3)).incrementCounter(Metrics.SINK_MONGO_MODIFIED_TOTAL);
    }


    public static class BulkWriteResultMock extends BulkWriteResult {

        @Mock
        private BulkWriteUpsert bulkWriteUpsert;

        private final boolean wasAcknowledged;
        private final int insertedCount;
        private final int modifiedCount;
        private final int upsertedCount;

        public BulkWriteResultMock(boolean wasAcknowledged, int insertedCount, int modifiedCount, int upsertedCount) {

            initMocks(this);
            this.wasAcknowledged = wasAcknowledged;
            this.insertedCount = insertedCount;
            this.modifiedCount = modifiedCount;
            this.upsertedCount = upsertedCount;
        }

        @Override
        public boolean wasAcknowledged() {
            return wasAcknowledged;
        }

        @Override
        public int getInsertedCount() {
            return insertedCount;
        }

        @Override
        public int getMatchedCount() {
            return modifiedCount;
        }

        @Override
        public int getDeletedCount() {
            return 0;
        }

        @Override
        public boolean isModifiedCountAvailable() {
            return true;
        }

        @Override
        public int getModifiedCount() {
            return modifiedCount;
        }

        @Override
        public List<BulkWriteUpsert> getUpserts() {
            return new ArrayList<>(Collections.nCopies(upsertedCount, bulkWriteUpsert));
        }
    }
}

