package io.odpf.firehose.sink.common;


import com.newrelic.api.agent.NewRelic;
import com.newrelic.api.agent.Trace;
import io.odpf.firehose.consumer.Message;
import io.odpf.firehose.exception.NeedToRetry;
import io.odpf.firehose.metrics.Instrumentation;
import io.odpf.firehose.sink.AbstractSink;
import io.odpf.stencil.client.StencilClient;
import joptsimple.internal.Strings;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static io.odpf.firehose.metrics.Metrics.SINK_HTTP_RESPONSE_CODE_TOTAL;

public abstract class AbstractHttpSink extends AbstractSink {

    private final List<HttpEntityEnclosingRequestBase> httpRequests = new ArrayList<>();
    private final HttpClient httpClient;
    private final StencilClient stencilClient;
    private final Map<Integer, Boolean> retryStatusCodeRanges;
    private final Map<Integer, Boolean> requestLogStatusCodeRanges;
    protected static final String SUCCESS_CODE_PATTERN = "^2.*";

    public AbstractHttpSink(Instrumentation instrumentation, String sinkType, HttpClient httpClient, StencilClient stencilClient, Map<Integer, Boolean> retryStatusCodeRanges, Map<Integer, Boolean> requestLogStatusCodeRanges) {
        super(instrumentation, sinkType);
        this.httpClient = httpClient;
        this.stencilClient = stencilClient;
        this.retryStatusCodeRanges = retryStatusCodeRanges;
        this.requestLogStatusCodeRanges = requestLogStatusCodeRanges;
    }

    @Override
    @Trace(dispatcher = true)
    public List<Message> execute() throws Exception {
        HttpResponse response = null;
        for (HttpEntityEnclosingRequestBase httpRequest : httpRequests) {
            try {
                response = httpClient.execute(httpRequest);
                getInstrumentation().logInfo("Response Status: {}", statusCode(response));
                if (shouldLogRequest(response)) {
                    printRequest(httpRequest);
                }
                if (shouldRetry(response)) {
                    throw new NeedToRetry(statusCode(response));
                } else if (!Pattern.compile(SUCCESS_CODE_PATTERN).matcher(String.valueOf(response.getStatusLine().getStatusCode())).matches()) {
                    captureMessageDropCount(response, httpRequest);
                }
            } catch (IOException e) {
                NewRelic.noticeError(e);
                throw e;
            } finally {
                consumeResponse(response);
                captureHttpStatusCount(httpRequest, response);
            }
        }
        return new ArrayList<>();
    }

    @Override
    public void close() throws IOException {
        getInstrumentation().logInfo("HTTP connection closing");
        getHttpRequests().clear();
        getStencilClient().close();
    }


    private void consumeResponse(HttpResponse response) {
        if (response != null) {
            EntityUtils.consumeQuietly(response.getEntity());
        }
    }

    private boolean shouldLogRequest(HttpResponse response) {
        return response == null || getRequestLogStatusCodeRanges().containsKey(response.getStatusLine().getStatusCode());
    }

    private boolean shouldRetry(HttpResponse response) {
        return response == null || getRetryStatusCodeRanges().containsKey(response.getStatusLine().getStatusCode());
    }

    protected String statusCode(HttpResponse response) {
        if (response != null) {
            return Integer.toString(response.getStatusLine().getStatusCode());
        } else {
            return "null";
        }
    }

    private void captureHttpStatusCount(HttpEntityEnclosingRequestBase httpRequestMethod, HttpResponse response) {
        String urlTag = "url=" + httpRequestMethod.getURI().getPath();
        String httpCodeTag = "status_code=";
        if (response != null) {
            httpCodeTag = "status_code=" + response.getStatusLine().getStatusCode();
        }
        getInstrumentation().captureCountWithTags(SINK_HTTP_RESPONSE_CODE_TOTAL, 1, httpCodeTag, urlTag);
    }


    private void printRequest(HttpEntityEnclosingRequestBase httpRequest) throws IOException {
        InputStream inputStream = httpRequest.getEntity().getContent();
        String entireRequest = String.format("\nRequest Method: %s\nRequest Url: %s\nRequest Headers: %s\nRequest Body: %s",
                httpRequest.getMethod(),
                httpRequest.getURI(),
                Arrays.asList(httpRequest.getAllHeaders()),
                Strings.join(readContent(inputStream), "\n"));
        getInstrumentation().logInfo(entireRequest);
        inputStream.reset();
    }

    protected abstract List<String> readContent(InputStream inputStream) throws IOException;

    protected abstract void captureMessageDropCount(HttpResponse response, HttpEntityEnclosingRequestBase httpRequest) throws IOException;

    public void setHttpRequests(List<HttpEntityEnclosingRequestBase> httpRequests) {
        this.httpRequests.clear();
        this.httpRequests.addAll(httpRequests);
    }

    public List<HttpEntityEnclosingRequestBase> getHttpRequests() {
        return httpRequests;
    }

    public StencilClient getStencilClient() {
        return stencilClient;
    }

    public Map<Integer, Boolean> getRetryStatusCodeRanges() {
        return retryStatusCodeRanges;
    }

    public Map<Integer, Boolean> getRequestLogStatusCodeRanges() {
        return requestLogStatusCodeRanges;
    }
}
