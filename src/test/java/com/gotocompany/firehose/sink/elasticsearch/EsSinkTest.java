package com.gotocompany.firehose.sink.elasticsearch;

import com.gotocompany.firehose.config.enums.SinkType;
import com.gotocompany.firehose.exception.NeedToRetry;
import com.gotocompany.firehose.message.Message;
import com.gotocompany.firehose.metrics.FirehoseInstrumentation;
import com.gotocompany.firehose.sink.elasticsearch.request.EsRequestHandler;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.rest.RestStatus;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;

public class EsSinkTest {

    @Mock
    private FirehoseInstrumentation firehoseInstrumentation;
    @Mock
    private RestHighLevelClient client;
    @Mock
    private EsRequestHandler esRequestHandler;
    @Mock
    private IndexRequest indexRequest;
    @Mock
    private UpdateRequest updateRequest;
    @Mock
    private BulkResponse bulkResponse;

    private List<Message> messages;
    private List<String> esRetryStatusCodeBlacklist = new ArrayList<>();

    @Before
    public void setUp() {
        initMocks(this);

        String jsonString = "{\"customer_id\":\"544131618\",\"categories\":[{\"category\":\"COFFEE_SHOP\",\"merchant_visits_4_weeks\":1,\"orders_4_weeks\":0,\"orders_24_weeks\":0,\"allocated\":0.0,\"redeemed\":0.0},{\"category\":\"PIZZA_PASTA\",\"merchant_visits_4_weeks\":0,\"orders_4_weeks\":1,\"orders_24_weeks\":1,\"allocated\":0.0,\"redeemed\":0.0},{\"category\":\"ROTI\",\"merchant_visits_4_weeks\":1,\"orders_4_weeks\":0,\"orders_24_weeks\":0,\"allocated\":0.0,\"redeemed\":0.0},{\"category\":\"FASTFOOD\",\"merchant_visits_4_weeks\":0,\"orders_4_weeks\":1,\"orders_24_weeks\":1,\"allocated\":0.0,\"redeemed\":0.0}],\"merchants\":[{\"merchant_id\":\"542629489\",\"merchant_uuid\":\"62598e60-1e5b-497c-b971-5a2bb0efb745\",\"merchant_visits_4_weeks\":1,\"orders_4_weeks\":0,\"orders_24_weeks\":0,\"allocated\":0.0,\"redeemed\":0.0,\"days_since_last_order\":2000},{\"merchant_id\":\"542777412\",\"merchant_uuid\":\"0a84a08b-8a53-47f4-9e62-7b7c2316dd08\",\"merchant_visits_4_weeks\":1,\"orders_4_weeks\":0,\"orders_24_weeks\":0,\"allocated\":0.0,\"redeemed\":0.0,\"days_since_last_order\":2000},{\"merchant_id\":\"542675785\",\"merchant_uuid\":\"daf41597-27d4-4475-b7c7-4f11563adcdb\",\"merchant_visits_4_weeks\":0,\"orders_4_weeks\":1,\"orders_24_weeks\":1,\"allocated\":0.0,\"redeemed\":0.0,\"days_since_last_order\":1},{\"merchant_id\":\"542704646\",\"merchant_uuid\":\"9b522ca0-3ff0-4591-b60b-0e84b48d6d12\",\"merchant_visits_4_weeks\":1,\"orders_4_weeks\":0,\"orders_24_weeks\":0,\"allocated\":0.0,\"redeemed\":0.0,\"days_since_last_order\":2000},{\"merchant_id\":\"542809106\",\"merchant_uuid\":\"b902f7ba-ab5e-4de1-9755-56648f556265\",\"merchant_visits_4_weeks\":0,\"orders_4_weeks\":1,\"orders_24_weeks\":1,\"allocated\":0.0,\"redeemed\":0.0,\"days_since_last_order\":1}],\"brands\":[{\"brand_id\":\"e9f7c4b2-4fa6-489a-ab20-a1bb4638ad29\",\"merchant_visits_4_weeks\":1,\"orders_4_weeks\":0,\"orders_24_weeks\":0,\"allocated\":0.0,\"redeemed\":0.0},{\"brand_id\":\"336eb59c-621a-4704-811c-e1024f970e2e\",\"merchant_visits_4_weeks\":0,\"orders_4_weeks\":1,\"orders_24_weeks\":1,\"allocated\":0.0,\"redeemed\":0.0},{\"brand_id\":\"0f30e2ca-f97f-43ec-895c-0d9d729e4cca\",\"merchant_visits_4_weeks\":0,\"orders_4_weeks\":1,\"orders_24_weeks\":1,\"allocated\":0.0,\"redeemed\":0.0},{\"brand_id\":\"901af18e-f5b7-43c5-9e67-4906d6ccce51\",\"merchant_visits_4_weeks\":1,\"orders_4_weeks\":0,\"orders_24_weeks\":0,\"allocated\":0.0,\"redeemed\":0.0},{\"brand_id\":\"da07057d-7fe1-47de-8713-4c1edcfc9afc\",\"merchant_visits_4_weeks\":1,\"orders_4_weeks\":0,\"orders_24_weeks\":0,\"allocated\":0.0,\"redeemed\":0.0}],\"orders_4_weeks\":2,\"orders_24_weeks\":2,\"merchant_visits_4_weeks\":4,\"app_version_major\":\"3\",\"app_version_minor\":\"30\",\"app_version_patch\":\"2\",\"current_country\":\"ID\",\"os\":\"Android\",\"wallet_id\":\"16230097256391350739\",\"dag_run_time\":\"2019-06-27T07:27:00+00:00\"}";
        Message messageWithJSON = new Message(null, jsonString.getBytes(), "", 0, 1);

        String logMessage = "CgYIyOm+xgUSBgiE6r7GBRgNIICAgIDA9/y0LigCMAM\u003d";
        Message messageWithProto = new Message(null, Base64.getDecoder().decode(logMessage.getBytes()), "sample-topic", 0, 100);

        messages = new ArrayList<>();
        this.messages.add(messageWithJSON);
        this.messages.add(messageWithProto);

        esRetryStatusCodeBlacklist.add("404");
        esRetryStatusCodeBlacklist.add("502");

        when(esRequestHandler.getRequest(messageWithJSON)).thenReturn(indexRequest);
        when(esRequestHandler.getRequest(messageWithProto)).thenReturn(updateRequest);
    }

    @Test
    public void shouldGetRequestForEachMessageInEsbMessagesList() {
        EsSink esSink = new EsSink(firehoseInstrumentation, SinkType.ELASTICSEARCH.name(), client, esRequestHandler, 5000, 1, esRetryStatusCodeBlacklist);

        esSink.prepare(messages);
        verify(esRequestHandler, times(1)).getRequest(messages.get(0));
        verify(esRequestHandler, times(1)).getRequest(messages.get(1));
    }

    @Test
    public void shouldReturnEmptyArrayListWhenBulkResponseExecutedSuccessfully() throws IOException {
        when(bulkResponse.hasFailures()).thenReturn(false);
        EsSinkMock esSinkMock = new EsSinkMock(firehoseInstrumentation, SinkType.ELASTICSEARCH.name(), client, esRequestHandler,
                5000, 1, esRetryStatusCodeBlacklist);
        esSinkMock.setBulkResponse(bulkResponse);

        List<Message> failedMessages = esSinkMock.pushMessage(this.messages);
        Assert.assertEquals(0, failedMessages.size());
    }

    @Test
    public void shouldThrowNeedToRetryExceptionWhenBulkResponseHasFailuresExceptMentionedInBlacklist() {
        when(bulkResponse.buildFailureMessage()).thenReturn("400");
        BulkResponseItemMock bulkResponseItemMock1 = new BulkResponseItemMock(0, DocWriteRequest.OpType.UPDATE, new UpdateResponse(), 400);
        BulkResponseItemMock bulkResponseItemMock2 = new BulkResponseItemMock(0, DocWriteRequest.OpType.UPDATE, new UpdateResponse(), 400);
        BulkItemResponse[] bulkItemResponses = {bulkResponseItemMock1, bulkResponseItemMock2};
        when(bulkResponse.hasFailures()).thenReturn(true);
        when(bulkResponse.getItems()).thenReturn(bulkItemResponses);
        EsSinkMock esSinkMock = new EsSinkMock(firehoseInstrumentation, SinkType.ELASTICSEARCH.name(), client, esRequestHandler,
                5000, 1, esRetryStatusCodeBlacklist);
        esSinkMock.setBulkResponse(bulkResponse);

        esSinkMock.prepare(messages);
        try {
            esSinkMock.execute();
        } catch (Exception e) {
            Assert.assertEquals(NeedToRetry.class, e.getClass());
            Assert.assertEquals("Status code fall under retry range. StatusCode: 400", e.getMessage());
        }
    }

    @Test
    public void shouldReturnEsbMessagesListWhenBulkResponseHasFailuresAndEmptyBlacklist() throws IOException {
        BulkResponseItemMock bulkResponseItemMock1 = new BulkResponseItemMock(0, DocWriteRequest.OpType.UPDATE, new UpdateResponse(), 400);
        BulkResponseItemMock bulkResponseItemMock2 = new BulkResponseItemMock(0, DocWriteRequest.OpType.UPDATE, new UpdateResponse(), 400);
        BulkItemResponse[] bulkItemResponses = {bulkResponseItemMock1, bulkResponseItemMock2};
        when(bulkResponse.hasFailures()).thenReturn(true);
        when(bulkResponse.getItems()).thenReturn(bulkItemResponses);
        EsSinkMock esSinkMock = new EsSinkMock(firehoseInstrumentation, SinkType.ELASTICSEARCH.name(), client, esRequestHandler,
                5000, 1, new ArrayList<>());
        esSinkMock.setBulkResponse(bulkResponse);

        List<Message> failedMessages = esSinkMock.pushMessage(this.messages);
        Assert.assertEquals(messages.get(0), failedMessages.get(0));
        Assert.assertEquals(messages.get(1), failedMessages.get(1));
    }

    @Test
    public void shouldReturnEsbMessagesListWhenBulkResponseHasFailuresWithStatusOtherThanBlacklist() throws IOException {
        BulkResponseItemMock bulkResponseItemMock1 = new BulkResponseItemMock(0, DocWriteRequest.OpType.UPDATE, new UpdateResponse(), 400);
        BulkResponseItemMock bulkResponseItemMock2 = new BulkResponseItemMock(0, DocWriteRequest.OpType.UPDATE, new UpdateResponse(), 400);
        BulkItemResponse[] bulkItemResponses = {bulkResponseItemMock1, bulkResponseItemMock2};
        when(bulkResponse.hasFailures()).thenReturn(true);
        when(bulkResponse.getItems()).thenReturn(bulkItemResponses);
        EsSinkMock esSinkMock = new EsSinkMock(firehoseInstrumentation, SinkType.ELASTICSEARCH.name(), client, esRequestHandler,
                5000, 1, esRetryStatusCodeBlacklist);
        esSinkMock.setBulkResponse(bulkResponse);

        List<Message> failedMessages = esSinkMock.pushMessage(this.messages);
        Assert.assertEquals(messages.get(0), failedMessages.get(0));
        Assert.assertEquals(messages.get(1), failedMessages.get(1));
    }

    @Test
    public void shouldReturnEmptyMessageListIfAllTheResponsesBelongToBlacklistStatusCode() throws IOException {
        BulkResponseItemMock bulkResponseItemMock1 = new BulkResponseItemMock(0, DocWriteRequest.OpType.UPDATE, new UpdateResponse(), 404);
        BulkResponseItemMock bulkResponseItemMock2 = new BulkResponseItemMock(0, DocWriteRequest.OpType.UPDATE, new UpdateResponse(), 404);
        BulkItemResponse[] bulkItemResponses = {bulkResponseItemMock1, bulkResponseItemMock2};
        when(bulkResponse.hasFailures()).thenReturn(true);
        when(bulkResponse.getItems()).thenReturn(bulkItemResponses);
        EsSinkMock esSinkMock = new EsSinkMock(firehoseInstrumentation, SinkType.ELASTICSEARCH.name(), client, esRequestHandler, 5000,
                1, esRetryStatusCodeBlacklist);
        esSinkMock.setBulkResponse(bulkResponse);

        List<Message> failedMessages = esSinkMock.pushMessage(this.messages);
        Assert.assertEquals(0, failedMessages.size());
    }

    @Test
    public void shouldReportTelemetryIfTheResponsesBelongToBlacklistStatusCode() throws IOException {
        BulkResponseItemMock bulkResponseItemMock1 = new BulkResponseItemMock(0, DocWriteRequest.OpType.UPDATE, new UpdateResponse(), 404);
        BulkResponseItemMock bulkResponseItemMock2 = new BulkResponseItemMock(0, DocWriteRequest.OpType.UPDATE, new UpdateResponse(), 404);
        BulkItemResponse[] bulkItemResponses = {bulkResponseItemMock1, bulkResponseItemMock2};
        when(bulkResponse.hasFailures()).thenReturn(true);
        when(bulkResponse.getItems()).thenReturn(bulkItemResponses);
        EsSinkMock esSinkMock = new EsSinkMock(firehoseInstrumentation, SinkType.ELASTICSEARCH.name(), client, esRequestHandler, 5000, 1, esRetryStatusCodeBlacklist);
        esSinkMock.setBulkResponse(bulkResponse);

        esSinkMock.pushMessage(this.messages);
        verify(firehoseInstrumentation, times(2)).logInfo("Not retrying due to response status: {} is under blacklisted status code", "404");
        verify(firehoseInstrumentation, times(2)).logInfo("Message dropped because of status code: 404");
        verify(firehoseInstrumentation, times(2)).incrementCounter("firehose_sink_messages_drop_total", "cause=NOT_FOUND");
    }

    @Test
    public void shouldThrowNeedToRetryExceptionIfSomeOfTheFailuresDontBelongToBlacklist() throws IOException {
        BulkResponseItemMock bulkResponseItemMock1 = new BulkResponseItemMock(0, DocWriteRequest.OpType.UPDATE, new UpdateResponse(), 404);
        BulkResponseItemMock bulkResponseItemMock2 = new BulkResponseItemMock(0, DocWriteRequest.OpType.UPDATE, new UpdateResponse(), 502);
        BulkResponseItemMock bulkResponseItemMock3 = new BulkResponseItemMock(0, DocWriteRequest.OpType.UPDATE, new UpdateResponse(), 400);
        BulkItemResponse[] bulkItemResponses = {bulkResponseItemMock1, bulkResponseItemMock2, bulkResponseItemMock3};
        when(bulkResponse.hasFailures()).thenReturn(true);
        when(bulkResponse.getItems()).thenReturn(bulkItemResponses);
        EsSinkMock esSinkMock = new EsSinkMock(firehoseInstrumentation, SinkType.ELASTICSEARCH.name(), client, esRequestHandler,
                5000, 1, esRetryStatusCodeBlacklist);
        esSinkMock.setBulkResponse(bulkResponse);
        String logMessage = "CgYIyOm+xgUSBgiE6r7GBRgNIICAgIDA9/y0LigCMAM\u003d";
        Message messageWithProto = new Message(null, Base64.getDecoder().decode(logMessage.getBytes()), "sample-topic", 0, 100);
        messages.add(messageWithProto);
        List<Message> failedMessages = esSinkMock.pushMessage(this.messages);
        verify(firehoseInstrumentation, times(2)).incrementCounter(any(String.class), any(String.class));
        Assert.assertEquals(3, failedMessages.size());
    }

    @Test
    public void shouldLogBulkRequestFailedWhenBulkResponsesHasFailures() {
        BulkResponseItemMock bulkResponseItemMock1 = new BulkResponseItemMock(0, DocWriteRequest.OpType.UPDATE, new UpdateResponse(), 404);
        BulkResponseItemMock bulkResponseItemMock2 = new BulkResponseItemMock(0, DocWriteRequest.OpType.UPDATE, new UpdateResponse(), 404);
        BulkItemResponse[] bulkItemResponses = {bulkResponseItemMock1, bulkResponseItemMock2};
        when(bulkResponse.hasFailures()).thenReturn(true);
        when(bulkResponse.getItems()).thenReturn(bulkItemResponses);
        EsSinkMock esSinkMock = new EsSinkMock(firehoseInstrumentation, SinkType.ELASTICSEARCH.name(), client, esRequestHandler,
                5000, 1, esRetryStatusCodeBlacklist);
        esSinkMock.setBulkResponse(bulkResponse);

        esSinkMock.pushMessage(this.messages);
        verify(firehoseInstrumentation, times(1)).logWarn("Bulk request failed");
        verify(firehoseInstrumentation, times(1)).logWarn("Bulk request failed count: {}", 2);
    }

    @Test
    public void shouldNotLogBulkRequestFailedWhenBulkResponsesHasNotFailures() {
        BulkResponseItemMock bulkResponseItemMock1 = new BulkResponseItemMock(0, DocWriteRequest.OpType.UPDATE, new UpdateResponse(), 404);
        BulkResponseItemMock bulkResponseItemMock2 = new BulkResponseItemMock(0, DocWriteRequest.OpType.UPDATE, new UpdateResponse(), 404);
        BulkItemResponse[] bulkItemResponses = {bulkResponseItemMock1, bulkResponseItemMock2};
        when(bulkResponse.hasFailures()).thenReturn(false);
        when(bulkResponse.getItems()).thenReturn(bulkItemResponses);
        EsSinkMock esSinkMock = new EsSinkMock(firehoseInstrumentation, SinkType.ELASTICSEARCH.name(), client, esRequestHandler,
                5000, 1, esRetryStatusCodeBlacklist);
        esSinkMock.setBulkResponse(bulkResponse);

        esSinkMock.pushMessage(this.messages);
        verify(firehoseInstrumentation, times(0)).logWarn("Bulk request failed");
        verify(firehoseInstrumentation, times(0)).logWarn("Bulk request failed count: {}", 2);
    }

    public static class EsSinkMock extends EsSink {

        private BulkResponse bulkResponse;

        public EsSinkMock(FirehoseInstrumentation firehoseInstrumentation, String sinkType, RestHighLevelClient client, EsRequestHandler esRequestHandler,
                          long esRequestTimeoutInMs, Integer esWaitForActiveShardsCount, List<String> esRetryStatusCodeBlacklist) {
            super(firehoseInstrumentation, sinkType, client, esRequestHandler, esRequestTimeoutInMs, esWaitForActiveShardsCount, esRetryStatusCodeBlacklist);
        }

        public void setBulkResponse(BulkResponse bulkResponse) {
            this.bulkResponse = bulkResponse;
        }

        BulkResponse getBulkResponse() {
            return bulkResponse;
        }
    }

    private static class BulkResponseItemMock extends BulkItemResponse {
        private int status;

        BulkResponseItemMock(int id, DocWriteRequest.OpType opType, DocWriteResponse response, int status) {
            super(id, opType, response);
            this.status = status;
        }

        @Override
        public boolean isFailed() {
            return true;
        }

        @Override
        public RestStatus status() {
            return RestStatus.fromCode(status);
        }
    }
}
