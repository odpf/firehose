package com.gotocompany.firehose.sink.prometheus;


import com.gotocompany.firehose.exception.DeserializerException;
import com.gotocompany.firehose.message.Message;
import com.gotocompany.firehose.metrics.FirehoseInstrumentation;
import com.gotocompany.firehose.metrics.Metrics;
import com.gotocompany.firehose.sink.prometheus.request.PromRequest;
import com.gotocompany.firehose.sink.common.AbstractHttpSink;
import com.google.protobuf.DynamicMessage;
import cortexpb.Cortex;
import com.gotocompany.stencil.client.StencilClient;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.xerial.snappy.Snappy;

import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * the Prometheus Sink. this sink use prometheus remote write api to send data into Cortex.
 */
public class PromSink extends AbstractHttpSink {

    private final PromRequest request;

    /**
     * Instantiates a new Prometheus sink.
     *
     * @param firehoseInstrumentation            the instrumentation
     * @param request                    the request
     * @param httpClient                 the http client
     * @param stencilClient              the stencil client
     * @param retryStatusCodeRanges      the retry status code ranges
     * @param requestLogStatusCodeRanges the request log status code ranges
     */
    public PromSink(FirehoseInstrumentation firehoseInstrumentation, PromRequest request, HttpClient httpClient, StencilClient stencilClient, Map<Integer, Boolean> retryStatusCodeRanges, Map<Integer, Boolean> requestLogStatusCodeRanges) {
        super(firehoseInstrumentation, "prometheus", httpClient, stencilClient, retryStatusCodeRanges, requestLogStatusCodeRanges);
        this.request = request;
    }

    /**
     * process messages before sending to cortex.
     *
     * @param messages the consumer messages
     * @throws DeserializerException the exception on deserialization
     * @throws IOException           the io exception
     */
    @Override
    protected void prepare(List<Message> messages) throws DeserializerException, IOException, SQLException {
        try {
            super.prepare(messages);
            setHttpRequests(request.build(messages));
        } catch (URISyntaxException e) {
            throw new IOException(e);
        }
    }

    protected void captureMessageDropCount(HttpResponse response, List<String> contentStringList) {
        getFirehoseInstrumentation().captureCount(Metrics.SINK_MESSAGES_DROP_TOTAL, (long) contentStringList.size(), "cause= " + statusCode(response));
        getFirehoseInstrumentation().logInfo("Message dropped because of status code: " + statusCode(response));
    }

    /**
     * read compressed request body.
     *
     * @param httpRequest http request object
     * @return list of request body string
     * @throws IOException the io exception
     */
    protected List<String> readContent(HttpEntityEnclosingRequestBase httpRequest) throws IOException {
        try (InputStream inputStream = httpRequest.getEntity().getContent()) {
            byte[] byteArrayIs = IOUtils.toByteArray(inputStream);
            byte[] uncompressedSnappy = Snappy.uncompress(byteArrayIs);
            String requestBody = DynamicMessage.parseFrom(Cortex.WriteRequest.getDescriptor(), uncompressedSnappy).toString();
            return Arrays.asList(requestBody.split("\\s(?=timeseries)"));
        }
    }
}
