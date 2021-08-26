package io.odpf.firehose.sink.http;


import io.odpf.firehose.consumer.Message;
import io.odpf.firehose.exception.DeserializerException;
import io.odpf.firehose.metrics.Instrumentation;
import io.odpf.firehose.sink.common.AbstractHttpSink;
import io.odpf.firehose.sink.http.request.types.Request;
import io.odpf.stencil.client.StencilClient;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.odpf.firehose.metrics.Metrics.SINK_MESSAGES_DROP_TOTAL;


/**
 * HttpSink implement {@link AbstractHttpSink } lifecycle for HTTP.
 */
public class HttpSink extends AbstractHttpSink {

    private final Request request;

    /**
     * Instantiates a new Http sink.
     *
     * @param instrumentation            the instrumentation
     * @param request                    the request
     * @param httpClient                 the http client
     * @param stencilClient              the stencil client
     * @param retryStatusCodeRanges      the retry status code ranges
     * @param requestLogStatusCodeRanges the request log status code ranges
     */
    public HttpSink(Instrumentation instrumentation, Request request, HttpClient httpClient, StencilClient stencilClient, Map<Integer, Boolean> retryStatusCodeRanges, Map<Integer, Boolean> requestLogStatusCodeRanges) {
        super(instrumentation, "http", httpClient, stencilClient, retryStatusCodeRanges, requestLogStatusCodeRanges);
        this.request = request;
    }

    @Override
    protected void prepare(List<Message> messages) throws DeserializerException, IOException {
        try {
            setHttpRequests(request.build(messages));
        } catch (URISyntaxException e) {
            throw new IOException(e);
        }
    }

    @Override
    protected List<String> readContent(InputStream inputStream) {
        return new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8)).lines().collect(Collectors.toList());
    }

    protected void captureMessageDropCount(HttpResponse response, HttpEntityEnclosingRequestBase httpRequest) throws IOException {
        InputStream inputStream = httpRequest.getEntity().getContent();
        String requestBody = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8)).lines().collect(Collectors.joining("\n"));

        List<String> result = Arrays.asList(requestBody.replaceAll("^\\[|]$", "").split("},\\s*\\{"));

        getInstrumentation().captureCountWithTags(SINK_MESSAGES_DROP_TOTAL, result.size(), "cause= " + statusCode(response));
        getInstrumentation().logInfo("Message dropped because of status code: " + statusCode(response));
    }
}
