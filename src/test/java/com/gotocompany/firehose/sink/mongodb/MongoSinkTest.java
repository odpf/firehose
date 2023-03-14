package com.gotocompany.firehose.sink.mongodb;

import com.gotocompany.firehose.config.enums.SinkType;
import com.gotocompany.firehose.message.Message;
import com.gotocompany.firehose.metrics.FirehoseInstrumentation;
import com.gotocompany.firehose.sink.mongodb.client.MongoSinkClient;
import com.gotocompany.firehose.sink.mongodb.request.MongoRequestHandler;
import com.mongodb.bulk.BulkWriteError;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.WriteModel;
import org.bson.BsonDocument;
import org.bson.Document;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;

public class MongoSinkTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Mock
    private FirehoseInstrumentation firehoseInstrumentation;

    @Mock
    private MongoRequestHandler mongoRequestHandler;

    @Mock
    private MongoSinkClient mongoSinkClient;

    private List<Message> messages;
    private List<ReplaceOneModel<Document>> requests;
    private final List<Integer> mongoRetryStatusCodeBlacklist = new ArrayList<>();

    @Before
    public void setUp() {
        initMocks(this);

        String jsonString = "{\"customer_id\":\"544131618\",\"categories\":[{\"category\":\"COFFEE_SHOP\",\"merchant_visits_4_weeks\":1,\"orders_4_weeks\":0,\"orders_24_weeks\":0,\"allocated\":0.0,\"redeemed\":0.0},{\"category\":\"PIZZA_PASTA\",\"merchant_visits_4_weeks\":0,\"orders_4_weeks\":1,\"orders_24_weeks\":1,\"allocated\":0.0,\"redeemed\":0.0},{\"category\":\"ROTI\",\"merchant_visits_4_weeks\":1,\"orders_4_weeks\":0,\"orders_24_weeks\":0,\"allocated\":0.0,\"redeemed\":0.0},{\"category\":\"FASTFOOD\",\"merchant_visits_4_weeks\":0,\"orders_4_weeks\":1,\"orders_24_weeks\":1,\"allocated\":0.0,\"redeemed\":0.0}],\"merchants\":[{\"merchant_id\":\"542629489\",\"merchant_uuid\":\"62598e60-1e5b-497c-b971-5a2bb0efb745\",\"merchant_visits_4_weeks\":1,\"orders_4_weeks\":0,\"orders_24_weeks\":0,\"allocated\":0.0,\"redeemed\":0.0,\"days_since_last_order\":2000},{\"merchant_id\":\"542777412\",\"merchant_uuid\":\"0a84a08b-8a53-47f4-9e62-7b7c2316dd08\",\"merchant_visits_4_weeks\":1,\"orders_4_weeks\":0,\"orders_24_weeks\":0,\"allocated\":0.0,\"redeemed\":0.0,\"days_since_last_order\":2000},{\"merchant_id\":\"542675785\",\"merchant_uuid\":\"daf41597-27d4-4475-b7c7-4f11563adcdb\",\"merchant_visits_4_weeks\":0,\"orders_4_weeks\":1,\"orders_24_weeks\":1,\"allocated\":0.0,\"redeemed\":0.0,\"days_since_last_order\":1},{\"merchant_id\":\"542704646\",\"merchant_uuid\":\"9b522ca0-3ff0-4591-b60b-0e84b48d6d12\",\"merchant_visits_4_weeks\":1,\"orders_4_weeks\":0,\"orders_24_weeks\":0,\"allocated\":0.0,\"redeemed\":0.0,\"days_since_last_order\":2000},{\"merchant_id\":\"542809106\",\"merchant_uuid\":\"b902f7ba-ab5e-4de1-9755-56648f556265\",\"merchant_visits_4_weeks\":0,\"orders_4_weeks\":1,\"orders_24_weeks\":1,\"allocated\":0.0,\"redeemed\":0.0,\"days_since_last_order\":1}],\"brands\":[{\"brand_id\":\"e9f7c4b2-4fa6-489a-ab20-a1bb4638ad29\",\"merchant_visits_4_weeks\":1,\"orders_4_weeks\":0,\"orders_24_weeks\":0,\"allocated\":0.0,\"redeemed\":0.0},{\"brand_id\":\"336eb59c-621a-4704-811c-e1024f970e2e\",\"merchant_visits_4_weeks\":0,\"orders_4_weeks\":1,\"orders_24_weeks\":1,\"allocated\":0.0,\"redeemed\":0.0},{\"brand_id\":\"0f30e2ca-f97f-43ec-895c-0d9d729e4cca\",\"merchant_visits_4_weeks\":0,\"orders_4_weeks\":1,\"orders_24_weeks\":1,\"allocated\":0.0,\"redeemed\":0.0},{\"brand_id\":\"901af18e-f5b7-43c5-9e67-4906d6ccce51\",\"merchant_visits_4_weeks\":1,\"orders_4_weeks\":0,\"orders_24_weeks\":0,\"allocated\":0.0,\"redeemed\":0.0},{\"brand_id\":\"da07057d-7fe1-47de-8713-4c1edcfc9afc\",\"merchant_visits_4_weeks\":1,\"orders_4_weeks\":0,\"orders_24_weeks\":0,\"allocated\":0.0,\"redeemed\":0.0}],\"orders_4_weeks\":2,\"orders_24_weeks\":2,\"merchant_visits_4_weeks\":4,\"app_version_major\":\"3\",\"app_version_minor\":\"30\",\"app_version_patch\":\"2\",\"current_country\":\"ID\",\"os\":\"Android\",\"wallet_id\":\"16230097256391350739\",\"dag_run_time\":\"2019-06-27T07:27:00+00:00\"}";
        Message messageWithJSON = new Message(null, jsonString.getBytes(), "", 0, 1);
        String logMessage = "CgYIyOm+xgUSBgiE6r7GBRgNIICAgIDA9/y0LigCMAM\u003d";
        Message messageWithProto = new Message(null, Base64.getDecoder().decode(logMessage.getBytes()), "sample-topic", 0, 100);

        messages = new ArrayList<>();
        messages.add(messageWithJSON);
        messages.add(messageWithProto);

        mongoRetryStatusCodeBlacklist.add(11000);
        mongoRetryStatusCodeBlacklist.add(502);

        ReplaceOneModel<Document> replaceOneModel1 = new ReplaceOneModel<>(
                new Document("customer_id", "35452"),
                new Document(),
                new ReplaceOptions().upsert(true));

        ReplaceOneModel<Document> replaceOneModel2 = new ReplaceOneModel<>(
                new Document("customer_id", "35452"),
                new Document(),
                new ReplaceOptions().upsert(true));
        requests = Arrays.asList(replaceOneModel1, replaceOneModel2);
        when(mongoRequestHandler.getRequest(messageWithJSON)).thenReturn(replaceOneModel1);
        when(mongoRequestHandler.getRequest(messageWithProto)).thenReturn(replaceOneModel2);
    }

    @Test
    public void shouldGetRequestForEachMessageInEsbMessagesList() {
        MongoSink mongoSink = new MongoSink(firehoseInstrumentation, SinkType.MONGODB.name(), mongoRequestHandler,
                mongoSinkClient);

        mongoSink.prepare(messages);
        verify(mongoRequestHandler, times(1)).getRequest(messages.get(0));
        verify(mongoRequestHandler, times(1)).getRequest(messages.get(1));
    }

    @Test
    public void shouldGetCorrectRequestsForEachMessageInEsbMessagesList() throws IllegalAccessException, NoSuchFieldException {
        MongoSink mongoSink = new MongoSink(firehoseInstrumentation, SinkType.MONGODB.name(), mongoRequestHandler,
                mongoSinkClient);

        Field requestsField = MongoSink.class.getDeclaredField("requests");
        requestsField.setAccessible(true);
        mongoSink.prepare(messages);
        List<WriteModel<Document>> requestsList = (List<WriteModel<Document>>) requestsField.get(mongoSink);

        assertEquals(this.requests.get(0), requestsList.get(0));
        assertEquals(this.requests.get(1), requestsList.get(1));
    }

    @Test
    public void shouldReturnFailedMessagesWhenBulkRequestFailed() throws NoSuchFieldException, IllegalAccessException {
        BulkWriteError writeError1 = new BulkWriteError(400, "Duplicate Key Error", new BsonDocument(), 0);
        BulkWriteError writeError2 = new BulkWriteError(11000, "Duplicate Key Error", new BsonDocument(), 1);
        List<BulkWriteError> writeErrors = Arrays.asList(writeError1, writeError2);

        MongoSink mongoSink = new MongoSink(firehoseInstrumentation, SinkType.MONGODB.name(), mongoRequestHandler,
                mongoSinkClient);
        Field messagesField = MongoSink.class.getDeclaredField("messages");
        messagesField.setAccessible(true);
        messagesField.set(mongoSink, this.messages);

        when(mongoSinkClient.processRequest(any())).thenReturn(writeErrors);
        List<Message> failedMessages = mongoSink.execute();
        assertEquals(2, failedMessages.size());
        assertEquals(this.messages.get(0), failedMessages.get(0));
        assertEquals(this.messages.get(1), failedMessages.get(1));
    }

    @Test
    public void shouldReturnEmptyListWhenBulRequestSucceeds() throws NoSuchFieldException, IllegalAccessException {
        List<BulkWriteError> writeErrors = new ArrayList<>();
        MongoSink mongoSink = new MongoSink(firehoseInstrumentation, SinkType.MONGODB.name(), mongoRequestHandler,
                mongoSinkClient);

        Field messagesField = MongoSink.class.getDeclaredField("messages");
        messagesField.setAccessible(true);
        messagesField.set(mongoSink, this.messages);

        when(mongoSinkClient.processRequest(any())).thenReturn(writeErrors);
        List<Message> failedMessages = mongoSink.execute();
        assertEquals(0, failedMessages.size());
    }
}
