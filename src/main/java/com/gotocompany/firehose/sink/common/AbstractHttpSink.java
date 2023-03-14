package com.gotocompany.firehose.sink.common;


import com.gotocompany.firehose.exception.NeedToRetry;
import com.gotocompany.firehose.message.Message;
import com.gotocompany.firehose.metrics.FirehoseInstrumentation;
import com.gotocompany.firehose.metrics.Metrics;
import com.gotocompany.firehose.sink.AbstractSink;
import com.gotocompany.stencil.client.StencilClient;
import joptsimple.internal.Strings;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.util.EntityUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public abstract class AbstractHttpSink extends AbstractSink {

    private final List<HttpEntityEnclosingRequestBase> httpRequests = new ArrayList<>();
    private final HttpClient httpClient;
    private final StencilClient stencilClient;
    private final Map<Integer, Boolean> retryStatusCodeRanges;
    private final Map<Integer, Boolean> requestLogStatusCodeRanges;
    protected static final String SUCCESS_CODE_PATTERN = "^2.*";

    public AbstractHttpSink(FirehoseInstrumentation firehoseInstrumentation, String sinkType, HttpClient httpClient, StencilClient stencilClient, Map<Integer, Boolean> retryStatusCodeRanges, Map<Integer, Boolean> requestLogStatusCodeRanges) {
        super(firehoseInstrumentation, sinkType);
        this.httpClient = httpClient;
        this.stencilClient = stencilClient;
        this.retryStatusCodeRanges = retryStatusCodeRanges;
        this.requestLogStatusCodeRanges = requestLogStatusCodeRanges;
    }

    @Override
    public List<Message> execute() throws Exception {
        HttpResponse response = null;
        for (HttpEntityEnclosingRequestBase httpRequest : httpRequests) {
            try {
                response = httpClient.execute(httpRequest);
                List<String> contentStringList = null;
                getFirehoseInstrumentation().logInfo("Response Status: {}", statusCode(response));
                if (shouldLogResponse(response)) {
                    printResponse(response);
                }
                if (shouldLogRequest(response)) {
                    contentStringList = readContent(httpRequest);
                    printRequest(httpRequest, contentStringList);
                }
                if (shouldRetry(response)) {
                    throw new NeedToRetry(statusCode(response));
                } else if (!Pattern.compile(SUCCESS_CODE_PATTERN).matcher(String.valueOf(response.getStatusLine().getStatusCode())).matches()) {
                    contentStringList = contentStringList == null ? readContent(httpRequest) : contentStringList;
                    captureMessageDropCount(response, contentStringList);
                }
            } finally {
                consumeResponse(response);
                captureHttpStatusCount(response);
            }
        }
        return new ArrayList<>();
    }

    @Override
    public void close() throws IOException {
        getFirehoseInstrumentation().logInfo("HTTP connection closing");
        getHttpRequests().clear();
        getStencilClient().close();
    }


    private void consumeResponse(HttpResponse response) {
        if (response != null) {
            EntityUtils.consumeQuietly(response.getEntity());
        }
    }

    private boolean shouldLogRequest(HttpResponse response) {
        String statusCode = statusCode(response);
        return statusCode.equals("null") || getRequestLogStatusCodeRanges().containsKey(Integer.parseInt(statusCode));
    }

    private boolean shouldLogResponse(HttpResponse response) {
        return getFirehoseInstrumentation().isDebugEnabled() && response != null && response.getEntity() != null;
    }

    private boolean shouldRetry(HttpResponse response) {
        String statusCode = statusCode(response);
        return statusCode.equals("null") || Integer.parseInt(statusCode) == 0 || getRetryStatusCodeRanges().containsKey(Integer.parseInt(statusCode));
    }

    protected String statusCode(HttpResponse response) {
        if (response != null && response.getStatusLine() != null) {
            return Integer.toString(response.getStatusLine().getStatusCode());
        } else {
            return "null";
        }
    }

    private void captureHttpStatusCount(HttpResponse response) {
        String statusCode = statusCode(response);
        String httpCodeTag = statusCode.equals("null") ? "status_code=" : "status_code=" + statusCode;
        getFirehoseInstrumentation().captureCount(Metrics.SINK_HTTP_RESPONSE_CODE_TOTAL, 1L, httpCodeTag);
    }

    private void printRequest(HttpEntityEnclosingRequestBase httpRequest, List<String> contentStringList) throws IOException {
        String entireRequest = String.format("\nRequest Method: %s\nRequest Url: %s\nRequest Headers: %s\nRequest Body: %s",
                httpRequest.getMethod(),
                httpRequest.getURI(),
                Arrays.asList(httpRequest.getAllHeaders()),
                Strings.join(contentStringList, "\n"));
        getFirehoseInstrumentation().logInfo(entireRequest);
    }

    private void printResponse(HttpResponse httpResponse) throws IOException {
        try (InputStream inputStream = httpResponse.getEntity().getContent()) {
            String responseBody = String.format("Response Body: %s",
                    Strings.join(new BufferedReader(new InputStreamReader(
                            inputStream,
                            StandardCharsets.UTF_8)).lines().collect(Collectors.toList()), "\n"));
            getFirehoseInstrumentation().logDebug(responseBody);
        }
    }

    protected abstract List<String> readContent(HttpEntityEnclosingRequestBase httpRequest) throws IOException;

    protected abstract void captureMessageDropCount(HttpResponse response, List<String> contentString) throws IOException;

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
