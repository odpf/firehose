package io.odpf.firehose.sink.prometheus;


import io.odpf.firehose.sink.common.AbstractHttpSink;
import io.odpf.firehose.sink.prometheus.request.PromRequest;
import com.google.protobuf.DynamicMessage;
import cortexpb.Cortex;
import io.odpf.firehose.consumer.Message;
import io.odpf.firehose.exception.DeserializerException;
import io.odpf.firehose.metrics.Instrumentation;
import io.odpf.stencil.client.StencilClient;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.xerial.snappy.Snappy;

import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static io.odpf.firehose.metrics.Metrics.SINK_MESSAGES_DROP_TOTAL;

/**
 * the Prometheus Sink. this sink use prometheus remote write api to send data into Cortex.
 */
public class PromSink extends AbstractHttpSink {

    private final PromRequest request;

    /**
     * Instantiates a new Prometheus sink.
     *
     * @param instrumentation            the instrumentation
     * @param request                    the request
     * @param httpClient                 the http client
     * @param stencilClient              the stencil client
     * @param retryStatusCodeRanges      the retry status code ranges
     * @param requestLogStatusCodeRanges the request log status code ranges
     */
    public PromSink(Instrumentation instrumentation, PromRequest request, HttpClient httpClient, StencilClient stencilClient, Map<Integer, Boolean> retryStatusCodeRanges, Map<Integer, Boolean> requestLogStatusCodeRanges) {
        super(instrumentation, "prometheus", httpClient, stencilClient, retryStatusCodeRanges, requestLogStatusCodeRanges);
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
    protected void prepare(List<Message> messages) throws DeserializerException, IOException {
        try {
            setHttpRequests(request.build(messages));
        } catch (URISyntaxException e) {
            throw new IOException(e);
        }
    }

    protected void captureMessageDropCount(HttpResponse response, HttpEntityEnclosingRequestBase httpRequest) throws IOException {
        InputStream inputStream = httpRequest.getEntity().getContent();
        List<String> result = readContent(inputStream);

        getInstrumentation().captureCountWithTags(SINK_MESSAGES_DROP_TOTAL, result.size(), "cause= " + statusCode(response));
        getInstrumentation().logInfo("Message dropped because of status code: " + statusCode(response));
    }

    /**
     * read compressed request body.
     *
     * @param inputStream the inputstream
     * @return list of request body string
     * @throws IOException the io exception
     */
    protected List<String> readContent(InputStream inputStream) throws IOException {
        byte[] byteArrayIs = IOUtils.toByteArray(inputStream);
        byte[] uncompressedSnappy = Snappy.uncompress(byteArrayIs);
        String requestBody = DynamicMessage.parseFrom(Cortex.WriteRequest.getDescriptor(), uncompressedSnappy).toString();
        return Arrays.asList(requestBody.split("\\s(?=timeseries)"));
    }
}
