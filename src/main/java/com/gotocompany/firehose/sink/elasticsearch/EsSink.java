package com.gotocompany.firehose.sink.elasticsearch;

import com.gotocompany.firehose.exception.NeedToRetry;
import com.gotocompany.firehose.message.Message;
import com.gotocompany.firehose.metrics.FirehoseInstrumentation;
import com.gotocompany.firehose.metrics.Metrics;
import com.gotocompany.firehose.sink.elasticsearch.request.EsRequestHandler;
import com.gotocompany.firehose.sink.AbstractSink;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Elastic search sink.
 */
public class EsSink extends AbstractSink {
    private RestHighLevelClient client;
    private EsRequestHandler esRequestHandler;
    private BulkRequest bulkRequest;
    private long esRequestTimeoutInMs;
    private Integer esWaitForActiveShardsCount;
    private List<String> esRetryStatusCodeBlacklist;

    /**
     * Instantiates a new Es sink.
     *
     * @param firehoseInstrumentation            the instrumentation
     * @param sinkType                   the sink type
     * @param client                     the client
     * @param esRequestHandler           the es request handler
     * @param esRequestTimeoutInMs       the es request timeout in ms
     * @param esWaitForActiveShardsCount the es wait for active shards count
     * @param esRetryStatusCodeBlacklist the es retry status code blacklist
     */
    public EsSink(FirehoseInstrumentation firehoseInstrumentation, String sinkType, RestHighLevelClient client, EsRequestHandler esRequestHandler,
                  long esRequestTimeoutInMs, Integer esWaitForActiveShardsCount, List<String> esRetryStatusCodeBlacklist) {
        super(firehoseInstrumentation, sinkType);
        this.client = client;
        this.esRequestHandler = esRequestHandler;
        this.esRequestTimeoutInMs = esRequestTimeoutInMs;
        this.esWaitForActiveShardsCount = esWaitForActiveShardsCount;
        this.esRetryStatusCodeBlacklist = esRetryStatusCodeBlacklist;
    }

    @Override
    protected void prepare(List<Message> messages) {
        bulkRequest = new BulkRequest();
        bulkRequest.timeout(TimeValue.timeValueMillis(esRequestTimeoutInMs));
        bulkRequest.waitForActiveShards(esWaitForActiveShardsCount);
        messages.forEach(message -> bulkRequest.add(esRequestHandler.getRequest(message)));
    }

    @Override
    protected List<Message> execute() throws Exception {
        BulkResponse bulkResponse = getBulkResponse();
        if (bulkResponse.hasFailures()) {
            getFirehoseInstrumentation().logWarn("Bulk request failed");
            handleResponse(bulkResponse);
        }
        return new ArrayList<>();
    }

    @Override
    public void close() throws IOException {
        getFirehoseInstrumentation().logInfo("Elastic Search connection closing");
        this.client.close();
    }

    BulkResponse getBulkResponse() throws IOException {
        return client.bulk(bulkRequest);
    }

    private void handleResponse(BulkResponse bulkResponse) throws NeedToRetry {
        int failedResponseCount = 0;
        for (BulkItemResponse response : bulkResponse.getItems()) {
            if (response.isFailed()) {
                failedResponseCount++;
                String responseStatus = String.valueOf(response.status().getStatus());
                if (esRetryStatusCodeBlacklist.contains(responseStatus)) {
                    getFirehoseInstrumentation().logInfo("Not retrying due to response status: {} is under blacklisted status code", responseStatus);
                    getFirehoseInstrumentation().incrementCounter(Metrics.SINK_MESSAGES_DROP_TOTAL, "cause=" + response.status().name());
                    getFirehoseInstrumentation().logInfo("Message dropped because of status code: " + responseStatus);
                } else {
                    throw new NeedToRetry(bulkResponse.buildFailureMessage());
                }
            }
        }
        getFirehoseInstrumentation().logWarn("Bulk request failed count: {}", failedResponseCount);
    }
}
