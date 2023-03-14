package com.gotocompany.firehose.sink.elasticsearch.request;



import com.gotocompany.firehose.config.enums.EsSinkMessageType;
import com.gotocompany.firehose.config.enums.EsSinkRequestType;
import com.gotocompany.firehose.exception.JsonParseException;
import com.gotocompany.firehose.message.Message;
import com.gotocompany.firehose.serializer.MessageToJson;
import com.gotocompany.firehose.consumer.TestAggregatedSupplyMessage;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.google.protobuf.InvalidProtocolBufferException;

import com.gotocompany.stencil.StencilClientFactory;
import com.gotocompany.stencil.client.StencilClient;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.Before;
import org.junit.Test;

import java.util.Base64;
import java.util.HashMap;

import static org.junit.Assert.*;
import static org.mockito.MockitoAnnotations.initMocks;

public class ESUpsertRequestHandlerTest {

    private final StencilClient stencilClient = StencilClientFactory.getClient();

    private MessageToJson jsonSerializer;
    private Message messageWithJSON;
    private Message messageWithProto;
    private String logMessage;
    private String jsonString;

    @Before
    public void setUp() throws InvalidProtocolBufferException {
        initMocks(this);
        jsonString = "{\"customer_id\":\"544131618\",\"vehicle_type\":\"SCOOTER\",\"categories\":[{\"category\":\"COFFEE_SHOP\",\"merchant_visits_4_weeks\":1,\"orders_4_weeks\":0,\"orders_24_weeks\":0,\"allocated\":0.0,\"redeemed\":0.0},{\"category\":\"PIZZA_PASTA\",\"merchant_visits_4_weeks\":0,\"orders_4_weeks\":1,\"orders_24_weeks\":1,\"allocated\":0.0,\"redeemed\":0.0},{\"category\":\"ROTI\",\"merchant_visits_4_weeks\":1,\"orders_4_weeks\":0,\"orders_24_weeks\":0,\"allocated\":0.0,\"redeemed\":0.0},{\"category\":\"FASTFOOD\",\"merchant_visits_4_weeks\":0,\"orders_4_weeks\":1,\"orders_24_weeks\":1,\"allocated\":0.0,\"redeemed\":0.0}],\"merchants\":[{\"merchant_id\":\"542629489\",\"merchant_uuid\":\"62598e60-1e5b-497c-b971-5a2bb0efb745\",\"merchant_visits_4_weeks\":1,\"orders_4_weeks\":0,\"orders_24_weeks\":0,\"allocated\":0.0,\"redeemed\":0.0,\"days_since_last_order\":2000},{\"merchant_id\":\"542777412\",\"merchant_uuid\":\"0a84a08b-8a53-47f4-9e62-7b7c2316dd08\",\"merchant_visits_4_weeks\":1,\"orders_4_weeks\":0,\"orders_24_weeks\":0,\"allocated\":0.0,\"redeemed\":0.0,\"days_since_last_order\":2000},{\"merchant_id\":\"542675785\",\"merchant_uuid\":\"daf41597-27d4-4475-b7c7-4f11563adcdb\",\"merchant_visits_4_weeks\":0,\"orders_4_weeks\":1,\"orders_24_weeks\":1,\"allocated\":0.0,\"redeemed\":0.0,\"days_since_last_order\":1},{\"merchant_id\":\"542704646\",\"merchant_uuid\":\"9b522ca0-3ff0-4591-b60b-0e84b48d6d12\",\"merchant_visits_4_weeks\":1,\"orders_4_weeks\":0,\"orders_24_weeks\":0,\"allocated\":0.0,\"redeemed\":0.0,\"days_since_last_order\":2000},{\"merchant_id\":\"542809106\",\"merchant_uuid\":\"b902f7ba-ab5e-4de1-9755-56648f556265\",\"merchant_visits_4_weeks\":0,\"orders_4_weeks\":1,\"orders_24_weeks\":1,\"allocated\":0.0,\"redeemed\":0.0,\"days_since_last_order\":1}],\"brands\":[{\"brand_id\":\"e9f7c4b2-4fa6-489a-ab20-a1bb4638ad29\",\"merchant_visits_4_weeks\":1,\"orders_4_weeks\":0,\"orders_24_weeks\":0,\"allocated\":0.0,\"redeemed\":0.0},{\"brand_id\":\"336eb59c-621a-4704-811c-e1024f970e2e\",\"merchant_visits_4_weeks\":0,\"orders_4_weeks\":1,\"orders_24_weeks\":1,\"allocated\":0.0,\"redeemed\":0.0},{\"brand_id\":\"0f30e2ca-f97f-43ec-895c-0d9d729e4cca\",\"merchant_visits_4_weeks\":0,\"orders_4_weeks\":1,\"orders_24_weeks\":1,\"allocated\":0.0,\"redeemed\":0.0},{\"brand_id\":\"901af18e-f5b7-43c5-9e67-4906d6ccce51\",\"merchant_visits_4_weeks\":1,\"orders_4_weeks\":0,\"orders_24_weeks\":0,\"allocated\":0.0,\"redeemed\":0.0},{\"brand_id\":\"da07057d-7fe1-47de-8713-4c1edcfc9afc\",\"merchant_visits_4_weeks\":1,\"orders_4_weeks\":0,\"orders_24_weeks\":0,\"allocated\":0.0,\"redeemed\":0.0}],\"orders_4_weeks\":2,\"orders_24_weeks\":2,\"merchant_visits_4_weeks\":4,\"app_version_major\":\"3\",\"app_version_minor\":\"30\",\"app_version_patch\":\"2\",\"current_country\":\"ID\",\"os\":\"Android\",\"wallet_id\":\"16230097256391350739\",\"dag_run_time\":\"2019-06-27T07:27:00+00:00\"}";
        messageWithJSON = new Message(null, jsonString.getBytes(), "", 0, 1);
        logMessage = "CgYIyOm+xgUSBgiE6r7GBRgNIICAgIDA9/y0LigCMAM\u003d";
        messageWithProto = new Message(null, Base64.getDecoder().decode(logMessage.getBytes()), "sample-topic", 0, 100);

        String protoClassName = TestAggregatedSupplyMessage.class.getName();
        jsonSerializer = new MessageToJson(stencilClient.getParser(protoClassName), true, false);
    }

    @Test
    public void shouldReturnTrueForInsertMode() {
        EsUpsertRequestHandler esUpsertRequestHandler = new EsUpsertRequestHandler(
                EsSinkMessageType.JSON, jsonSerializer, "customer", "booking", EsSinkRequestType.INSERT_OR_UPDATE, "customer_id", "vehicle_type");

        assertTrue(esUpsertRequestHandler.canCreate());
    }

    @Test
    public void shouldReturnFalseForUpdateOnlyMode() {
        EsUpsertRequestHandler esUpsertRequestHandler = new EsUpsertRequestHandler(EsSinkMessageType.JSON, jsonSerializer, "customer", "booking", EsSinkRequestType.UPDATE_ONLY, "customer_id", "vehicle_type");

        assertFalse(esUpsertRequestHandler.canCreate());
    }

    @Test
    public void shouldReturnInsertRequestHandlerForJsonMessageType() {
        EsUpsertRequestHandler esUpsertRequestHandler = new EsUpsertRequestHandler(EsSinkMessageType.JSON, jsonSerializer, "customer", "booking", EsSinkRequestType.INSERT_OR_UPDATE, "customer_id", "vehicle_type");

        DocWriteRequest request = esUpsertRequestHandler.getRequest(messageWithJSON);
        assertEquals(IndexRequest.class, request.getClass());
    }

    @Test
    public void shouldReturnRequestWithCorrectIdIndexAndTypeForJsonMessageType() {
        EsUpsertRequestHandler esUpsertRequestHandler = new EsUpsertRequestHandler(EsSinkMessageType.JSON, jsonSerializer, "customer", "booking", EsSinkRequestType.INSERT_OR_UPDATE, "customer_id", "vehicle_type");

        DocWriteRequest request = esUpsertRequestHandler.getRequest(messageWithJSON);
        assertEquals("544131618", request.id());
        assertEquals("booking", request.index());
        assertEquals("customer", request.type());
    }

    @Test
    public void shouldReturnRequestWithCorrectRoutingValueForJsonMessageType() {
        EsUpsertRequestHandler esUpsertRequestHandler = new EsUpsertRequestHandler(EsSinkMessageType.JSON, jsonSerializer, "customer", "booking", EsSinkRequestType.INSERT_OR_UPDATE, "customer_id", "vehicle_type");

        DocWriteRequest request = esUpsertRequestHandler.getRequest(messageWithJSON);
        assertEquals("SCOOTER", request.routing());
    }

    @Test
    public void shouldReturnRequestWithNullRoutingValueWhenNoFieldNameIsProvidedForJsonMessageType() {
        EsUpsertRequestHandler esUpsertRequestHandler = new EsUpsertRequestHandler(EsSinkMessageType.JSON, jsonSerializer, "customer", "booking", EsSinkRequestType.INSERT_OR_UPDATE, "customer_id", "");

        DocWriteRequest request = esUpsertRequestHandler.getRequest(messageWithJSON);
        assertNull(request.routing());
    }

    @Test
    public void shouldReturnRequestWithCorrectPayloadForJsonMessageType() {
        EsUpsertRequestHandler esUpsertRequestHandler = new EsUpsertRequestHandler(EsSinkMessageType.JSON, jsonSerializer, "customer", "booking", EsSinkRequestType.INSERT_OR_UPDATE, "customer_id", "vehicle_type");

        IndexRequest request = (IndexRequest) esUpsertRequestHandler.getRequest(messageWithJSON);
        HashMap<String, Object> inputMap = new Gson().fromJson(
                jsonString, new TypeToken<HashMap<String, Object>>() {
                }.getType()
        );
        HashMap<String, Object> outputMap = (HashMap<String, Object>) request.sourceAsMap();

        assertEquals(inputMap.keySet(), outputMap.keySet());
        assertEquals(inputMap.get("wallet_id"), outputMap.get("wallet_id"));
        assertEquals(inputMap.get("dag_run_time"), outputMap.get("dag_run_time"));
    }

    @Test
    public void shouldReturnRequestWithCorrectContentTypeForJsonMessageType() {
        EsUpsertRequestHandler esUpsertRequestHandler = new EsUpsertRequestHandler(EsSinkMessageType.JSON, jsonSerializer, "customer", "booking", EsSinkRequestType.INSERT_OR_UPDATE, "customer_id", "vehicle_type");

        IndexRequest request = (IndexRequest) esUpsertRequestHandler.getRequest(messageWithJSON);
        assertEquals(XContentType.JSON, request.getContentType());
    }

    @Test
    public void shouldReturnInsertRequestHandlerForProtoMessageType() {
        EsUpsertRequestHandler esUpsertRequestHandler = new EsUpsertRequestHandler(EsSinkMessageType.PROTOBUF, jsonSerializer, "driver", "supply", EsSinkRequestType.INSERT_OR_UPDATE,
                "s2_id_level", "vehicle_type");

        assertEquals(IndexRequest.class, esUpsertRequestHandler.getRequest(messageWithProto).getClass());
    }

    @Test
    public void shouldReturnRequestWithCorrectIdIndexAndTypeForProtoMessageType() {
        EsUpsertRequestHandler esUpsertRequestHandler = new EsUpsertRequestHandler(EsSinkMessageType.PROTOBUF, jsonSerializer, "driver", "supply", EsSinkRequestType.INSERT_OR_UPDATE,
                "s2_id_level", "vehicle_type");

        DocWriteRequest request = esUpsertRequestHandler.getRequest(messageWithProto);
        assertEquals("13", request.id());
        assertEquals("supply", request.index());
        assertEquals("driver", request.type());
    }

    @Test
    public void shouldReturnRequestWithCorrectRoutingValueForProtoMessageType() {
        EsUpsertRequestHandler esUpsertRequestHandler = new EsUpsertRequestHandler(EsSinkMessageType.PROTOBUF, jsonSerializer, "driver", "supply", EsSinkRequestType.INSERT_OR_UPDATE,
                "s2_id_level", "vehicle_type");

        DocWriteRequest request = esUpsertRequestHandler.getRequest(messageWithProto);
        assertEquals("BIKE", request.routing());
    }

    @Test
    public void shouldReturnRequestWithNullRoutingValueForProtoMessageType() {
        EsUpsertRequestHandler esUpsertRequestHandler = new EsUpsertRequestHandler(EsSinkMessageType.PROTOBUF, jsonSerializer, "driver", "supply", EsSinkRequestType.INSERT_OR_UPDATE,
                "s2_id_level", "");

        DocWriteRequest request = esUpsertRequestHandler.getRequest(messageWithProto);
        assertEquals(null, request.routing());
    }

    @Test
    public void shouldReturnRequestWithCorrectContentTypeForProtoMessageType() {
        EsUpsertRequestHandler esUpsertRequestHandler = new EsUpsertRequestHandler(EsSinkMessageType.PROTOBUF, jsonSerializer, "driver", "supply", EsSinkRequestType.INSERT_OR_UPDATE,
                "s2_id_level", "vehicle_type");

        IndexRequest request = (IndexRequest) esUpsertRequestHandler.getRequest(messageWithProto);
        assertEquals(XContentType.JSON, request.getContentType());
    }

    @Test
    public void shouldReturnRequestWithCorrectPayloadForProtoMessageType() {
        EsUpsertRequestHandler esUpsertRequestHandler = new EsUpsertRequestHandler(EsSinkMessageType.PROTOBUF, jsonSerializer, "driver", "supply", EsSinkRequestType.INSERT_OR_UPDATE,
                "s2_id_level", "vehicle_type");

        IndexRequest request = (IndexRequest) esUpsertRequestHandler.getRequest(messageWithProto);
        HashMap<String, Object> outputMap = (HashMap<String, Object>) request.sourceAsMap();

        assertEquals("BIKE", outputMap.get("vehicle_type"));
        assertEquals("3", outputMap.get("unique_drivers"));
    }

    @Test
    public void shouldThrowJSONParseExceptionForInvalidJson() {
        EsUpsertRequestHandler esUpsertRequestHandler = new EsUpsertRequestHandler(EsSinkMessageType.PROTOBUF, jsonSerializer, "driver", "supply", EsSinkRequestType.INSERT_OR_UPDATE,
                "s2_id_level", "vehicle_type");

        try {
            esUpsertRequestHandler.getFieldFromJSON("", "s2_id_level");
        } catch (Exception e) {
            assertEquals(JsonParseException.class, e.getClass());
        }
    }

    @Test
    public void shouldThrowExceptionForInvalidKey() {
        EsUpsertRequestHandler esUpsertRequestHandler = new EsUpsertRequestHandler(EsSinkMessageType.PROTOBUF, jsonSerializer, "driver", "supply", EsSinkRequestType.INSERT_OR_UPDATE,
                "s2_id_level", "vehicle_type");
        try {
            esUpsertRequestHandler.getFieldFromJSON(jsonSerializer.serialize(messageWithProto), "wrongKey");
        } catch (Exception e) {
            assertEquals(IllegalArgumentException.class, e.getClass());
            assertEquals("Key: wrongKey not found in ESB Message", e.getMessage());
        }
    }
}
