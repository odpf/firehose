package io.odpf.firehose.sink.http;


import io.odpf.firehose.config.converter.RangeToHashMapConverter;
import io.odpf.firehose.consumer.Message;
import io.odpf.firehose.exception.DeserializerException;
import io.odpf.firehose.exception.NeedToRetry;
import io.odpf.firehose.metrics.Instrumentation;
import io.odpf.firehose.sink.http.request.types.Request;
import io.odpf.stencil.client.StencilClient;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.message.BasicHeader;
import org.apache.tools.ant.filters.StringInputStream;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;

import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;

@RunWith(MockitoJUnitRunner.class)
public class HttpSinkTest {
    @Mock
    private Instrumentation instrumentation;
    @Mock
    private Request request;
    @Mock
    private HttpClient httpClient;
    @Mock
    private StencilClient stencilClient;
    @Mock
    private HttpPut httpPut;
    @Mock
    private HttpPost httpPost;
    @Mock
    private HttpResponse response;
    @Mock
    private HttpEntity httpEntity;
    @Mock
    private StatusLine statusLine;
    @Mock
    private Map<Integer, Boolean> retryStatusCodeRange;
    @Mock
    private Map<Integer, Boolean> requestLogStatusCodeRanges;

    private List<Message> messages;

    @Before
    public void setup() {
        initMocks(this);

        messages = new ArrayList<>();

        String jsonString = "{\"customer_id\":\"544131618\",\"categories\":[{\"category\":\"COFFEE_SHOP\",\"merchant_visits_4_weeks\":1,\"orders_4_weeks\":0,\"orders_24_weeks\":0,\"allocated\":0.0,\"redeemed\":0.0},{\"category\":\"PIZZA_PASTA\",\"merchant_visits_4_weeks\":0,\"orders_4_weeks\":1,\"orders_24_weeks\":1,\"allocated\":0.0,\"redeemed\":0.0},{\"category\":\"ROTI\",\"merchant_visits_4_weeks\":1,\"orders_4_weeks\":0,\"orders_24_weeks\":0,\"allocated\":0.0,\"redeemed\":0.0},{\"category\":\"FASTFOOD\",\"merchant_visits_4_weeks\":0,\"orders_4_weeks\":1,\"orders_24_weeks\":1,\"allocated\":0.0,\"redeemed\":0.0}],\"merchants\":[{\"merchant_id\":\"542629489\",\"merchant_uuid\":\"62598e60-1e5b-497c-b971-5a2bb0efb745\",\"merchant_visits_4_weeks\":1,\"orders_4_weeks\":0,\"orders_24_weeks\":0,\"allocated\":0.0,\"redeemed\":0.0,\"days_since_last_order\":2000},{\"merchant_id\":\"542777412\",\"merchant_uuid\":\"0a84a08b-8a53-47f4-9e62-7b7c2316dd08\",\"merchant_visits_4_weeks\":1,\"orders_4_weeks\":0,\"orders_24_weeks\":0,\"allocated\":0.0,\"redeemed\":0.0,\"days_since_last_order\":2000},{\"merchant_id\":\"542675785\",\"merchant_uuid\":\"daf41597-27d4-4475-b7c7-4f11563adcdb\",\"merchant_visits_4_weeks\":0,\"orders_4_weeks\":1,\"orders_24_weeks\":1,\"allocated\":0.0,\"redeemed\":0.0,\"days_since_last_order\":1},{\"merchant_id\":\"542704646\",\"merchant_uuid\":\"9b522ca0-3ff0-4591-b60b-0e84b48d6d12\",\"merchant_visits_4_weeks\":1,\"orders_4_weeks\":0,\"orders_24_weeks\":0,\"allocated\":0.0,\"redeemed\":0.0,\"days_since_last_order\":2000},{\"merchant_id\":\"542809106\",\"merchant_uuid\":\"b902f7ba-ab5e-4de1-9755-56648f556265\",\"merchant_visits_4_weeks\":0,\"orders_4_weeks\":1,\"orders_24_weeks\":1,\"allocated\":0.0,\"redeemed\":0.0,\"days_since_last_order\":1}],\"brands\":[{\"brand_id\":\"e9f7c4b2-4fa6-489a-ab20-a1bb4638ad29\",\"merchant_visits_4_weeks\":1,\"orders_4_weeks\":0,\"orders_24_weeks\":0,\"allocated\":0.0,\"redeemed\":0.0},{\"brand_id\":\"336eb59c-621a-4704-811c-e1024f970e2e\",\"merchant_visits_4_weeks\":0,\"orders_4_weeks\":1,\"orders_24_weeks\":1,\"allocated\":0.0,\"redeemed\":0.0},{\"brand_id\":\"0f30e2ca-f97f-43ec-895c-0d9d729e4cca\",\"merchant_visits_4_weeks\":0,\"orders_4_weeks\":1,\"orders_24_weeks\":1,\"allocated\":0.0,\"redeemed\":0.0},{\"brand_id\":\"901af18e-f5b7-43c5-9e67-4906d6ccce51\",\"merchant_visits_4_weeks\":1,\"orders_4_weeks\":0,\"orders_24_weeks\":0,\"allocated\":0.0,\"redeemed\":0.0},{\"brand_id\":\"da07057d-7fe1-47de-8713-4c1edcfc9afc\",\"merchant_visits_4_weeks\":1,\"orders_4_weeks\":0,\"orders_24_weeks\":0,\"allocated\":0.0,\"redeemed\":0.0}],\"orders_4_weeks\":2,\"orders_24_weeks\":2,\"merchant_visits_4_weeks\":4,\"app_version_major\":\"3\",\"app_version_minor\":\"30\",\"app_version_patch\":\"2\",\"current_country\":\"ID\",\"os\":\"Android\",\"wallet_id\":\"16230097256391350739\",\"dag_run_time\":\"2019-06-27T07:27:00+00:00\"}";
        Message message = new Message(null, jsonString.getBytes(), "", 0, 1);

        messages.add(message);
        messages.add(message);
    }

    @Test
    public void shouldCallHttpClientWithProperRequest() throws Exception {
        when(httpPut.getURI()).thenReturn(new URI("http://dummy.com"));
        when(httpPost.getURI()).thenReturn(new URI("http://dummy.com"));
        when(response.getStatusLine()).thenReturn(statusLine, statusLine);
        when(statusLine.getStatusCode()).thenReturn(200, 200);

        List<HttpEntityEnclosingRequestBase> httpRequests = Arrays.asList(httpPut, httpPost);
        when(request.build(messages)).thenReturn(httpRequests);
        when(httpClient.execute(httpPut)).thenReturn(response, response);
        when(httpClient.execute(httpPost)).thenReturn(response, response);
        when(response.getAllHeaders()).thenReturn(
                new Header[]{new BasicHeader("Accept", "text/plain")},
                new Header[]{new BasicHeader("Accept", "text/plain")});
        when(response.getEntity()).thenReturn(httpEntity, httpEntity);
        when(httpEntity.getContent()).thenReturn(new StringInputStream("[{\"key\":\"value1\"}, {\"key\":\"value2\"}]"));

        HttpSink httpSink = new HttpSink(instrumentation, request, httpClient, stencilClient, retryStatusCodeRange, requestLogStatusCodeRanges);
        httpSink.prepare(messages);
        httpSink.execute();

        verify(request, times(1)).build(messages);
        verify(httpClient, times(1)).execute(httpPut);
        verify(httpClient, times(1)).execute(httpPost);
    }

    @Test(expected = NeedToRetry.class)
    public void shouldThrowNeedToRetryExceptionWhenResponseCodeIsGivenRange() throws Exception {
        when(response.getStatusLine()).thenReturn(statusLine);
        when(statusLine.getStatusCode()).thenReturn(500);

        List<HttpEntityEnclosingRequestBase> httpRequests = Arrays.asList(httpPut);

        when(httpPut.getURI()).thenReturn(new URI("http://dummy.com"));
        when(request.build(messages)).thenReturn(httpRequests);
        when(httpClient.execute(httpPut)).thenReturn(response);
        when(response.getAllHeaders()).thenReturn(new Header[]{new BasicHeader("Accept", "text/plain")});
        when(response.getEntity()).thenReturn(httpEntity);
        when(httpEntity.getContent()).thenReturn(new StringInputStream("{\"key\":\"value\"}"));

        HttpSink httpSink = new HttpSink(instrumentation, request, httpClient, stencilClient,
                new RangeToHashMapConverter().convert(null, "400-505"), requestLogStatusCodeRanges);
        httpSink.prepare(messages);
        httpSink.execute();
    }

    @Test(expected = NeedToRetry.class)
    public void shouldThrowNeedToRetryExceptionWhenResponseCodeIsNull() throws Exception {

        List<HttpEntityEnclosingRequestBase> httpRequests = Arrays.asList(httpPut);

        when(httpPut.getURI()).thenReturn(new URI("http://dummy.com"));
        when(httpPut.getAllHeaders()).thenReturn(new Header[]{});
        when(httpPut.getEntity()).thenReturn(httpEntity);
        when(httpEntity.getContent()).thenReturn(new StringInputStream(""));
        when(request.build(messages)).thenReturn(httpRequests);
        when(httpClient.execute(httpPut)).thenReturn(null);

        HttpSink httpSink = new HttpSink(instrumentation, request, httpClient, stencilClient, retryStatusCodeRange, requestLogStatusCodeRanges);
        httpSink.prepare(messages);
        httpSink.execute();
    }

    @Test(expected = IOException.class)
    public void shouldCatchURISyntaxExceptionAndThrowIOException() throws URISyntaxException, DeserializerException, IOException {
        when(request.build(messages)).thenThrow(new URISyntaxException("", ""));

        HttpSink httpSink = new HttpSink(instrumentation, request, httpClient, stencilClient, retryStatusCodeRange, requestLogStatusCodeRanges);
        httpSink.prepare(messages);
    }

    @Test
    public void shouldCatchIOExceptionInAbstractSinkAndCaptureFailedExecutionTelemetry() throws Exception {
        when(httpPut.getURI()).thenReturn(new URI("http://dummy.com"));
        List<HttpEntityEnclosingRequestBase> httpRequests = Arrays.asList(httpPut, httpPost);

        when(request.build(messages)).thenReturn(httpRequests);
        when(httpClient.execute(any(HttpPut.class))).thenThrow(IOException.class);

        HttpSink httpSink = new HttpSink(instrumentation, request, httpClient, stencilClient, retryStatusCodeRange, requestLogStatusCodeRanges);
        httpSink.pushMessage(messages);

        verify(instrumentation, times(1)).captureFailedExecutionTelemetry(any(IOException.class), anyInt());
    }

    @Test
    public void shouldCloseStencilClient() throws IOException {
        HttpSink httpSink = new HttpSink(instrumentation, request, httpClient, stencilClient, retryStatusCodeRange, requestLogStatusCodeRanges);

        httpSink.close();
        verify(stencilClient, times(1)).close();
    }

    @Test
    public void shouldLogConnectionClosing() throws IOException {
        HttpSink httpSink = new HttpSink(instrumentation, request, httpClient, stencilClient, retryStatusCodeRange, requestLogStatusCodeRanges);

        httpSink.close();
        verify(instrumentation, times(1)).logInfo("HTTP connection closing");
    }

    @Test
    public void shouldLogEntireRequestIfInStatusCodeRangeWithBatchRequestAndCaptureDroppedMessages() throws Exception {
        when(response.getStatusLine()).thenReturn(statusLine);
        when(statusLine.getStatusCode()).thenReturn(500);

        List<HttpEntityEnclosingRequestBase> httpRequests = Collections.singletonList(httpPut);

        when(httpPut.getMethod()).thenReturn("PUT");
        when(httpPut.getURI()).thenReturn(new URI("http://dummy.com"));
        when(httpPut.getAllHeaders()).thenReturn(new Header[]{new BasicHeader("Accept", "text/plain")});
        when(httpPut.getEntity()).thenReturn(httpEntity);
        when(httpEntity.getContent()).thenReturn(new StringInputStream("[{\"key\":\"value1\"},{\"key\":\"value2\"}]"));
        when(request.build(messages)).thenReturn(httpRequests);
        when(httpClient.execute(httpPut)).thenReturn(response);
        when(response.getAllHeaders()).thenReturn(new Header[]{new BasicHeader("Accept", "text/plain")});
        when(response.getEntity()).thenReturn(httpEntity);
        when(httpEntity.getContent()).thenReturn(new StringInputStream("[{\"key\":\"value1\"},{\"key\":\"value2\"}]"));

        HttpSink httpSink = new HttpSink(instrumentation, request, httpClient, stencilClient,
                retryStatusCodeRange, new RangeToHashMapConverter().convert(null, "400-505"));
        httpSink.prepare(messages);
        httpSink.execute();
        verify(instrumentation, times(1)).logInfo(
                    "\nRequest Method: PUT"
                    + "\nRequest Url: http://dummy.com"
                    + "\nRequest Headers: [Accept: text/plain]"
                    + "\nRequest Body: [{\"key\":\"value1\"},{\"key\":\"value2\"}]");
        verify(instrumentation, times(1)).logInfo("Message dropped because of status code: 500");
        verify(instrumentation, times(1)).captureCountWithTags("firehose_sink_messages_drop_total", 2, "cause= 500");
    }

    @Test
    public void shouldLogEntireRequestIfInStatusCodeRangeWithIndividualRequestAndCaptureDroppedMessages() throws Exception {
        when(response.getStatusLine()).thenReturn(statusLine);
        when(statusLine.getStatusCode()).thenReturn(500);

        List<HttpEntityEnclosingRequestBase> httpRequests = Collections.singletonList(httpPut);

        when(httpPut.getMethod()).thenReturn("PUT");
        when(httpPut.getURI()).thenReturn(new URI("http://dummy.com"));
        when(httpPut.getAllHeaders()).thenReturn(new Header[]{new BasicHeader("Accept", "text/plain")});
        when(httpPut.getEntity()).thenReturn(httpEntity);
        when(httpEntity.getContent()).thenReturn(new StringInputStream("{\"key\":\"value\"}"));
        when(request.build(messages)).thenReturn(httpRequests);
        when(httpClient.execute(httpPut)).thenReturn(response);
        when(response.getAllHeaders()).thenReturn(new Header[]{new BasicHeader("Accept", "text/plain")});
        when(response.getEntity()).thenReturn(httpEntity);
        when(httpEntity.getContent()).thenReturn(new StringInputStream("[{\"key\":\"value\"}]"));

        HttpSink httpSink = new HttpSink(instrumentation, request, httpClient, stencilClient,
                retryStatusCodeRange, new RangeToHashMapConverter().convert(null, "400-505"));
        httpSink.prepare(messages);
        httpSink.execute();
        verify(instrumentation, times(1)).logInfo(
                        "\nRequest Method: PUT"
                        + "\nRequest Url: http://dummy.com"
                        + "\nRequest Headers: [Accept: text/plain]"
                        + "\nRequest Body: [{\"key\":\"value\"}]");
        verify(instrumentation, times(1)).logInfo("Message dropped because of status code: 500");
        verify(instrumentation, times(1)).captureCountWithTags("firehose_sink_messages_drop_total", 1, "cause= 500");
    }

    @Test
    public void shouldLogEntireRequestIfInStatusCodeRangeWithSingleListRequestBodyAndCaptureDroppedMessages() throws Exception {
        when(response.getStatusLine()).thenReturn(statusLine);
        when(statusLine.getStatusCode()).thenReturn(500);

        List<HttpEntityEnclosingRequestBase> httpRequests = Collections.singletonList(httpPut);

        when(httpPut.getMethod()).thenReturn("PUT");
        when(httpPut.getURI()).thenReturn(new URI("http://dummy.com"));
        when(httpPut.getAllHeaders()).thenReturn(new Header[]{new BasicHeader("Accept", "text/plain")});
        when(httpPut.getEntity()).thenReturn(httpEntity);
        when(httpEntity.getContent()).thenReturn(new StringInputStream("[{\"key\":\"value\"}]"));
        when(request.build(messages)).thenReturn(httpRequests);
        when(httpClient.execute(httpPut)).thenReturn(response);
        when(response.getAllHeaders()).thenReturn(new Header[]{new BasicHeader("Accept", "text/plain")});
        when(response.getEntity()).thenReturn(httpEntity);
        when(httpEntity.getContent()).thenReturn(new StringInputStream("[{\"key\":\"value\"}]"));

        HttpSink httpSink = new HttpSink(instrumentation, request, httpClient, stencilClient,
                retryStatusCodeRange, new RangeToHashMapConverter().convert(null, "400-505"));
        httpSink.prepare(messages);
        httpSink.execute();
        verify(instrumentation, times(1)).logInfo(
                        "\nRequest Method: PUT"
                        + "\nRequest Url: http://dummy.com"
                        + "\nRequest Headers: [Accept: text/plain]"
                        + "\nRequest Body: [{\"key\":\"value\"}]");
        verify(instrumentation, times(1)).logInfo("Message dropped because of status code: 500");
        verify(instrumentation, times(1)).captureCountWithTags("firehose_sink_messages_drop_total", 1, "cause= 500");
    }

    @Test
    public void shouldNotLogEntireRequestIfNotInStatusCodeRange() throws Exception {
        when(response.getStatusLine()).thenReturn(statusLine);
        when(statusLine.getStatusCode()).thenReturn(500);

        List<HttpEntityEnclosingRequestBase> httpRequests = Collections.singletonList(httpPut);

        when(httpPut.getURI()).thenReturn(new URI("http://dummy.com"));
        when(httpPut.getEntity()).thenReturn(httpEntity);
        when(httpEntity.getContent()).thenReturn(new StringInputStream("[{\"key\":\"value1\"},{\"key\":\"value2\"}]"));
        when(request.build(messages)).thenReturn(httpRequests);
        when(httpClient.execute(httpPut)).thenReturn(response);
        when(response.getAllHeaders()).thenReturn(new Header[]{new BasicHeader("Accept", "text/plain")});
        when(response.getEntity()).thenReturn(httpEntity);
        when(httpEntity.getContent()).thenReturn(new StringInputStream("[{\"key\":\"value1\"},{\"key\":\"value2\"}]"));

        HttpSink httpSink = new HttpSink(instrumentation, request, httpClient, stencilClient,
                retryStatusCodeRange, new RangeToHashMapConverter().convert(null, "400-499"));
        httpSink.prepare(messages);
        httpSink.execute();
        verify(instrumentation, times(0)).logInfo(
                        "\nRequest Method: PUT"
                        + "\nRequest Url: http://dummy.com"
                        + "\nRequest Headers: [Accept: text/plain]"
                        + "\nRequest Body: [{\"key\":\"value1\"},{\"key\":\"value2\"}]");
    }

    @Test
    public void shouldCaptureDroppedMessagesMetricsIfNotInStatusCodeRange() throws Exception {
        when(response.getStatusLine()).thenReturn(statusLine);
        when(statusLine.getStatusCode()).thenReturn(500);

        List<HttpEntityEnclosingRequestBase> httpRequests = Collections.singletonList(httpPut);

        when(httpPut.getURI()).thenReturn(new URI("http://dummy.com"));
        when(httpPut.getEntity()).thenReturn(httpEntity);
        when(httpEntity.getContent()).thenReturn(new StringInputStream("[{\"key\":\"value1\"},{\"key\":\"value2\"}]"));
        when(request.build(messages)).thenReturn(httpRequests);
        when(httpClient.execute(httpPut)).thenReturn(response);
        when(response.getAllHeaders()).thenReturn(new Header[]{new BasicHeader("Accept", "text/plain")});
        when(response.getEntity()).thenReturn(httpEntity);
        when(httpEntity.getContent()).thenReturn(new StringInputStream("[{\"key\":\"value1\"},{\"key\":\"value2\"}]"));

        HttpSink httpSink = new HttpSink(instrumentation, request, httpClient, stencilClient,
                new RangeToHashMapConverter().convert(null, "400-499"), requestLogStatusCodeRanges);
        httpSink.prepare(messages);
        httpSink.execute();
        verify(instrumentation, times(1)).logInfo("Message dropped because of status code: 500");
        verify(instrumentation, times(1)).captureCountWithTags("firehose_sink_messages_drop_total", 2, "cause= 500");
    }

    @Test(expected = NeedToRetry.class)
    public void shouldNotCaptureDroppedMessagesMetricsIfInStatusCodeRange() throws Exception {
        when(response.getStatusLine()).thenReturn(statusLine);
        when(statusLine.getStatusCode()).thenReturn(500);

        List<HttpEntityEnclosingRequestBase> httpRequests = Collections.singletonList(httpPut);

        when(httpPut.getURI()).thenReturn(new URI("http://dummy.com"));
        when(request.build(messages)).thenReturn(httpRequests);
        when(httpClient.execute(httpPut)).thenReturn(response);
        when(response.getAllHeaders()).thenReturn(new Header[]{new BasicHeader("Accept", "text/plain")});
        when(response.getEntity()).thenReturn(httpEntity);
        when(httpEntity.getContent()).thenReturn(new StringInputStream("{\"key\":\"value\"}"));

        HttpSink httpSink = new HttpSink(instrumentation, request, httpClient, stencilClient,
                new RangeToHashMapConverter().convert(null, "400-600"), requestLogStatusCodeRanges);
        httpSink.prepare(messages);
        try {
            httpSink.execute();
        } finally {
            verify(instrumentation, times(0)).logInfo("Message dropped because of status code: 500");
            verify(instrumentation, times(0)).captureCountWithTags("messages.dropped.count", 1, "500");
        }
    }

    @Test
    public void shouldNotCaptureDroppedMessagesMetricsIfStatusCodeIs200() throws Exception {
        when(response.getStatusLine()).thenReturn(statusLine);
        when(statusLine.getStatusCode()).thenReturn(200);

        List<HttpEntityEnclosingRequestBase> httpRequests = Collections.singletonList(httpPut);

        when(httpPut.getURI()).thenReturn(new URI("http://dummy.com"));
        when(request.build(messages)).thenReturn(httpRequests);
        when(httpClient.execute(httpPut)).thenReturn(response);
        when(response.getAllHeaders()).thenReturn(new Header[]{new BasicHeader("Accept", "text/plain")});
        when(response.getEntity()).thenReturn(httpEntity);
        when(httpEntity.getContent()).thenReturn(new StringInputStream("{\"key\":\"value\"}"));

        HttpSink httpSink = new HttpSink(instrumentation, request, httpClient, stencilClient,
                retryStatusCodeRange, requestLogStatusCodeRanges);
        httpSink.prepare(messages);
        httpSink.execute();
        verify(instrumentation, times(0)).logInfo("Message dropped because of status code: 500");
        verify(instrumentation, times(0)).captureCountWithTags("messages.dropped.count", 1, "200");
    }

    @Test
    public void shouldNotCaptureDroppedMessagesMetricsIfStatusCodeIs201() throws Exception {
        when(response.getStatusLine()).thenReturn(statusLine);
        when(statusLine.getStatusCode()).thenReturn(201);

        List<HttpEntityEnclosingRequestBase> httpRequests = Collections.singletonList(httpPut);

        when(httpPut.getURI()).thenReturn(new URI("http://dummy.com"));
        when(request.build(messages)).thenReturn(httpRequests);
        when(httpClient.execute(httpPut)).thenReturn(response);
        when(response.getAllHeaders()).thenReturn(new Header[]{new BasicHeader("Accept", "text/plain")});
        when(response.getEntity()).thenReturn(httpEntity);
        when(httpEntity.getContent()).thenReturn(new StringInputStream("{\"key\":\"value\"}"));

        HttpSink httpSink = new HttpSink(instrumentation, request, httpClient, stencilClient,
                retryStatusCodeRange, requestLogStatusCodeRanges);
        httpSink.prepare(messages);
        httpSink.execute();
        verify(instrumentation, times(0)).logInfo("Message dropped because of status code: 500");
        verify(instrumentation, times(0)).captureCountWithTags("messages.dropped.count", 1, "201");
    }

    @Test
    public void shouldCaptureResponseStatusCount() throws Exception {
        when(response.getStatusLine()).thenReturn(statusLine);
        when(statusLine.getStatusCode()).thenReturn(200);

        List<HttpEntityEnclosingRequestBase> httpRequests = Collections.singletonList(httpPut);

        URI uri = new URI("http://dummy.com");
        when(httpPut.getURI()).thenReturn(uri);
        when(request.build(messages)).thenReturn(httpRequests);
        when(httpClient.execute(httpPut)).thenReturn(response);
        when(response.getAllHeaders()).thenReturn(new Header[]{new BasicHeader("Accept", "text/plain")});
        when(response.getEntity()).thenReturn(httpEntity);
        when(httpEntity.getContent()).thenReturn(new StringInputStream("{\"key\":\"value\"}"));

        HttpSink httpSink = new HttpSink(instrumentation, request, httpClient, stencilClient,
                retryStatusCodeRange, requestLogStatusCodeRanges);
        httpSink.prepare(messages);
        httpSink.execute();

        verify(instrumentation, times(1)).captureCountWithTags("firehose_sink_http_response_code_total", 1, "status_code=" + statusLine.getStatusCode(), "url=" + uri.getPath());
    }
}
