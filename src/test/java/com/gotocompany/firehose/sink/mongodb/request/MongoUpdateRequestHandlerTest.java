package com.gotocompany.firehose.sink.mongodb.request;

import com.gotocompany.firehose.config.enums.MongoSinkMessageType;
import com.gotocompany.firehose.config.enums.MongoSinkRequestType;
import com.gotocompany.firehose.exception.JsonParseException;
import com.gotocompany.firehose.message.Message;
import com.gotocompany.firehose.serializer.MessageToJson;
import com.gotocompany.stencil.StencilClientFactory;
import com.gotocompany.stencil.client.StencilClient;

import com.google.protobuf.InvalidProtocolBufferException;
import com.mongodb.BasicDBObject;
import com.mongodb.client.model.ReplaceOneModel;
import com.gotocompany.firehose.consumer.TestAggregatedSupplyMessage;
import org.bson.Document;
import org.json.simple.JSONObject;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Base64;
import java.util.stream.Collectors;

import static org.junit.Assert.*;
import static org.mockito.MockitoAnnotations.initMocks;

public class MongoUpdateRequestHandlerTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private final StencilClient stencilClient = StencilClientFactory.getClient();

    private MessageToJson jsonSerializer;
    private Message messageWithJSON;
    private Message messageWithProto;
    private String logMessage;
    private String jsonString;

    @Before
    public void setUp() throws InvalidProtocolBufferException {
        initMocks(this);
        jsonString = "{\"customer_id\":\"544131618\",\"vehicle_type\":\"BIKE\",\"categories\":[{\"category\":\"COFFEE_SHOP\",\"merchant_visits_4_weeks\":1,\"orders_4_weeks\":0,\"orders_24_weeks\":0,\"allocated\":0.0,\"redeemed\":0.0},{\"category\":\"PIZZA_PASTA\",\"merchant_visits_4_weeks\":0,\"orders_4_weeks\":1,\"orders_24_weeks\":1,\"allocated\":0.0,\"redeemed\":0.0},{\"category\":\"ROTI\",\"merchant_visits_4_weeks\":1,\"orders_4_weeks\":0,\"orders_24_weeks\":0,\"allocated\":0.0,\"redeemed\":0.0},{\"category\":\"FASTFOOD\",\"merchant_visits_4_weeks\":0,\"orders_4_weeks\":1,\"orders_24_weeks\":1,\"allocated\":0.0,\"redeemed\":0.0}],\"merchants\":[{\"merchant_id\":\"542629489\",\"merchant_uuid\":\"62598e60-1e5b-497c-b971-5a2bb0efb745\",\"merchant_visits_4_weeks\":1,\"orders_4_weeks\":0,\"orders_24_weeks\":0,\"allocated\":0.0,\"redeemed\":0.0,\"days_since_last_order\":2000},{\"merchant_id\":\"542777412\",\"merchant_uuid\":\"0a84a08b-8a53-47f4-9e62-7b7c2316dd08\",\"merchant_visits_4_weeks\":1,\"orders_4_weeks\":0,\"orders_24_weeks\":0,\"allocated\":0.0,\"redeemed\":0.0,\"days_since_last_order\":2000},{\"merchant_id\":\"542675785\",\"merchant_uuid\":\"daf41597-27d4-4475-b7c7-4f11563adcdb\",\"merchant_visits_4_weeks\":0,\"orders_4_weeks\":1,\"orders_24_weeks\":1,\"allocated\":0.0,\"redeemed\":0.0,\"days_since_last_order\":1},{\"merchant_id\":\"542704646\",\"merchant_uuid\":\"9b522ca0-3ff0-4591-b60b-0e84b48d6d12\",\"merchant_visits_4_weeks\":1,\"orders_4_weeks\":0,\"orders_24_weeks\":0,\"allocated\":0.0,\"redeemed\":0.0,\"days_since_last_order\":2000},{\"merchant_id\":\"542809106\",\"merchant_uuid\":\"b902f7ba-ab5e-4de1-9755-56648f556265\",\"merchant_visits_4_weeks\":0,\"orders_4_weeks\":1,\"orders_24_weeks\":1,\"allocated\":0.0,\"redeemed\":0.0,\"days_since_last_order\":1}],\"brands\":[{\"brand_id\":\"e9f7c4b2-4fa6-489a-ab20-a1bb4638ad29\",\"merchant_visits_4_weeks\":1,\"orders_4_weeks\":0,\"orders_24_weeks\":0,\"allocated\":0.0,\"redeemed\":0.0},{\"brand_id\":\"336eb59c-621a-4704-811c-e1024f970e2e\",\"merchant_visits_4_weeks\":0,\"orders_4_weeks\":1,\"orders_24_weeks\":1,\"allocated\":0.0,\"redeemed\":0.0},{\"brand_id\":\"0f30e2ca-f97f-43ec-895c-0d9d729e4cca\",\"merchant_visits_4_weeks\":0,\"orders_4_weeks\":1,\"orders_24_weeks\":1,\"allocated\":0.0,\"redeemed\":0.0},{\"brand_id\":\"901af18e-f5b7-43c5-9e67-4906d6ccce51\",\"merchant_visits_4_weeks\":1,\"orders_4_weeks\":0,\"orders_24_weeks\":0,\"allocated\":0.0,\"redeemed\":0.0},{\"brand_id\":\"da07057d-7fe1-47de-8713-4c1edcfc9afc\",\"merchant_visits_4_weeks\":1,\"orders_4_weeks\":0,\"orders_24_weeks\":0,\"allocated\":0.0,\"redeemed\":0.0}],\"orders_4_weeks\":2,\"orders_24_weeks\":2,\"merchant_visits_4_weeks\":4,\"app_version_major\":\"3\",\"app_version_minor\":\"30\",\"app_version_patch\":\"2\",\"current_country\":\"ID\",\"os\":\"Android\",\"wallet_id\":\"16230097256391350739\",\"dag_run_time\":\"2019-06-27T07:27:00+00:00\"}";
        messageWithJSON = new Message(null, jsonString.getBytes(), "", 0, 1);
        logMessage = "CgYIyOm+xgUSBgiE6r7GBRgNIICAgIDA9/y0LigCMAM\u003d";
        messageWithProto = new Message(null, Base64.getDecoder().decode(logMessage.getBytes()), "sample-topic", 0, 100);

        String protoClassName = TestAggregatedSupplyMessage.class.getName();
        jsonSerializer = new MessageToJson(stencilClient.getParser(protoClassName), true, false);

    }

    @Test
    public void shouldReturnTrueForUpdateOnlyMode() {
        MongoUpdateRequestHandler mongoUpdateRequestHandler = new MongoUpdateRequestHandler(MongoSinkMessageType.PROTOBUF, jsonSerializer, MongoSinkRequestType.UPDATE_ONLY,
                "customer_id", "message");

        assertTrue(mongoUpdateRequestHandler.canCreate());
    }

    @Test
    public void shouldReturnFalseForInsertOrUpdateMode() {
        MongoUpdateRequestHandler mongoUpdateRequestHandler = new MongoUpdateRequestHandler(MongoSinkMessageType.PROTOBUF, jsonSerializer, MongoSinkRequestType.UPSERT,
                "customer_id", "message");

        assertFalse(mongoUpdateRequestHandler.canCreate());
    }

    @Test
    public void shouldReturnReplaceOneModelForJsonMessageType() {
        MongoUpdateRequestHandler mongoUpdateRequestHandler = new MongoUpdateRequestHandler(MongoSinkMessageType.JSON, jsonSerializer, MongoSinkRequestType.UPDATE_ONLY,
                "customer_id", "message");

        assertEquals(ReplaceOneModel.class, mongoUpdateRequestHandler.getRequest(messageWithJSON).getClass());
    }

    @Test
    public void shouldReturnModelWithCorrectPayloadForJsonMessageType() {
        MongoUpdateRequestHandler mongoUpdateRequestHandler = new MongoUpdateRequestHandler(MongoSinkMessageType.JSON, jsonSerializer, MongoSinkRequestType.UPDATE_ONLY,
                "customer_id", "message");

        ReplaceOneModel<Document> request = mongoUpdateRequestHandler.getRequest(messageWithJSON);
        Document inputMap = new Document("_id", "544131618");
        inputMap.putAll(new BasicDBObject(Document.parse(jsonString)).toMap());
        Document outputMap = request.getReplacement();
        assertEquals(inputMap.keySet().stream().sorted().collect(Collectors.toList()), outputMap.keySet().stream().sorted().collect(Collectors.toList()));
        assertEquals(inputMap.get("wallet_id", "message"), outputMap.get("wallet_id", "message"));
        assertEquals(inputMap.get("dag_run_time"), outputMap.get("dag_run_time"));
    }


    @Test
    public void shouldReturnReplaceOneModelForProtoMessageType() {
        MongoUpdateRequestHandler mongoUpdateRequestHandler = new MongoUpdateRequestHandler(MongoSinkMessageType.PROTOBUF, jsonSerializer, MongoSinkRequestType.UPDATE_ONLY,
                "s2_id_level", "message");

        assertEquals(ReplaceOneModel.class, mongoUpdateRequestHandler.getRequest(messageWithProto).getClass());
    }


    @Test
    public void shouldReturnModelWithCorrectPayloadForProtoMessageType() {
        MongoUpdateRequestHandler mongoUpdateRequestHandler = new MongoUpdateRequestHandler(MongoSinkMessageType.PROTOBUF, jsonSerializer, MongoSinkRequestType.UPDATE_ONLY,
                "s2_id_level", "message");

        ReplaceOneModel<Document> request = mongoUpdateRequestHandler.getRequest(messageWithProto);
        Document outputMap = request.getReplacement();
        System.out.println(messageWithProto);
        assertEquals("BIKE", outputMap.get("vehicle_type"));
        assertEquals("3", outputMap.get("unique_drivers"));
    }

    @Test
    public void shouldThrowJSONParseExceptionForInvalidJson() {
        MongoUpdateRequestHandler mongoUpdateRequestHandler = new MongoUpdateRequestHandler(MongoSinkMessageType.PROTOBUF, jsonSerializer, MongoSinkRequestType.UPDATE_ONLY,
                "s2_id_level", "message");

        thrown.expect(JsonParseException.class);
        mongoUpdateRequestHandler.getJSONObject("");

    }

    @Test
    public void shouldThrowExceptionForInvalidKey() {
        MongoUpdateRequestHandler mongoUpdateRequestHandler = new MongoUpdateRequestHandler(MongoSinkMessageType.PROTOBUF, jsonSerializer, MongoSinkRequestType.UPDATE_ONLY,
                "s2_id_level", "message");
        JSONObject jsonObject = mongoUpdateRequestHandler.getJSONObject(jsonSerializer.serialize(messageWithProto));

        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Key: wrongKey not found in ESB Message");
        mongoUpdateRequestHandler.getFieldFromJSON(jsonObject, "wrongKey");
    }

    @Test
    public void shouldThrowNullPointerExceptionForNullPrimaryKey() {
        MongoUpdateRequestHandler mongoUpdateRequestHandler = new MongoUpdateRequestHandler(MongoSinkMessageType.PROTOBUF, jsonSerializer, MongoSinkRequestType.UPDATE_ONLY,
                "s2_id_level", "message");
        JSONObject jsonObject = mongoUpdateRequestHandler.getJSONObject(jsonSerializer.serialize(messageWithProto));

        thrown.expect(NullPointerException.class);
        thrown.expectMessage("Key cannot be null");
        mongoUpdateRequestHandler.getFieldFromJSON(jsonObject, null);
    }
}
