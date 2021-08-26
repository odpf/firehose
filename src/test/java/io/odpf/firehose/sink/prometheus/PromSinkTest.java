package io.odpf.firehose.sink.prometheus;


import cortexpb.Cortex;
import io.odpf.firehose.config.converter.RangeToHashMapConverter;
import io.odpf.firehose.consumer.Message;
import io.odpf.firehose.exception.DeserializerException;
import io.odpf.firehose.exception.NeedToRetry;
import io.odpf.firehose.metrics.Instrumentation;
import io.odpf.firehose.sink.prometheus.request.PromRequest;
import io.odpf.stencil.client.StencilClient;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.message.BasicHeader;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.xerial.snappy.Snappy;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static io.odpf.firehose.sink.prometheus.PromSinkConstants.PROMETHEUS_LABEL_FOR_METRIC_NAME;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;

@RunWith(MockitoJUnitRunner.class)
public class PromSinkTest {
    @Mock
    private Instrumentation instrumentation;
    @Mock
    private PromRequest request;
    @Mock
    private HttpClient httpClient;
    @Mock
    private StencilClient stencilClient;
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
    private Cortex.WriteRequest writeRequest;
    private List<HttpEntityEnclosingRequestBase> httpPostList;

    @Before
    public void setup() {
        initMocks(this);
        httpPostList = Collections.singletonList(httpPost);

        Cortex.Sample sample = Cortex.Sample.newBuilder().setValue(10).setTimestampMs(1000000).build();
        Cortex.LabelPair labelPair = Cortex.LabelPair.newBuilder().setName(PROMETHEUS_LABEL_FOR_METRIC_NAME).setValue("test_metric").build();
        Cortex.TimeSeries timeSeries = Cortex.TimeSeries.newBuilder()
                .addSamples(sample).addLabels(labelPair)
                .build();
        writeRequest = Cortex.WriteRequest.newBuilder().addTimeseries(timeSeries).build();

        messages = new ArrayList<>();
        Message message = new Message(null, writeRequest.toByteArray(), "", 0, 1);

        messages.add(message);
    }

    @Test
    public void shouldPrepareRequestDuringPreparationAndCallItDuringExecution() throws Exception {
        when(httpPost.getURI()).thenReturn(new URI("http://dummy.com"));
        when(response.getStatusLine()).thenReturn(statusLine);
        when(statusLine.getStatusCode()).thenReturn(200);
        when(request.build(messages)).thenReturn(httpPostList);
        when(httpClient.execute(httpPost)).thenReturn(response);

        PromSink promSink = new PromSink(instrumentation, request, httpClient, stencilClient, retryStatusCodeRange, requestLogStatusCodeRanges);
        promSink.prepare(messages);
        promSink.execute();

        verify(request, times(1)).build(messages);
        verify(httpClient, times(1)).execute(httpPost);
    }

    @Test(expected = NeedToRetry.class)
    public void shouldThrowNeedToRetryExceptionWhenResponseCodeIsGivenRange() throws Exception {
        when(response.getStatusLine()).thenReturn(statusLine);
        when(statusLine.getStatusCode()).thenReturn(500);
        when(httpPost.getURI()).thenReturn(new URI("http://dummy.com"));
        when(request.build(messages)).thenReturn(httpPostList);
        when(httpClient.execute(httpPost)).thenReturn(response);

        PromSink promSink = new PromSink(instrumentation, request, httpClient, stencilClient,
                new RangeToHashMapConverter().convert(null, "400-505"), requestLogStatusCodeRanges);
        promSink.prepare(messages);
        promSink.execute();
    }

    @Test(expected = NeedToRetry.class)
    public void shouldThrowNeedToRetryExceptionWhenResponseCodeIsNull() throws Exception {
        when(httpPost.getURI()).thenReturn(new URI("http://dummy.com"));
        when(httpPost.getAllHeaders()).thenReturn(new Header[]{});
        when(httpPost.getEntity()).thenReturn(httpEntity);
        when(httpEntity.getContent()).thenReturn(new ByteArrayInputStream(Snappy.compress(writeRequest.toByteArray())));
        when(request.build(messages)).thenReturn(httpPostList);
        when(httpClient.execute(httpPost)).thenReturn(null);

        PromSink promSink = new PromSink(instrumentation, request, httpClient, stencilClient, retryStatusCodeRange, requestLogStatusCodeRanges);
        promSink.prepare(messages);
        promSink.execute();
    }

    @Test(expected = IOException.class)
    public void shouldCatchURISyntaxExceptionAndThrowIOException() throws URISyntaxException, DeserializerException, IOException {
        when(request.build(messages)).thenThrow(new URISyntaxException("", ""));

        PromSink promSink = new PromSink(instrumentation, request, httpClient, stencilClient, retryStatusCodeRange, requestLogStatusCodeRanges);
        promSink.prepare(messages);
    }

    @Test
    public void shouldCatchIOExceptionInAbstractSinkAndCaptureFailedExecutionTelemetry() throws Exception {
        when(httpPost.getURI()).thenReturn(new URI("http://dummy.com"));
        when(request.build(messages)).thenReturn(httpPostList);
        when(httpClient.execute(any(HttpPost.class))).thenThrow(IOException.class);

        PromSink promSink = new PromSink(instrumentation, request, httpClient, stencilClient, retryStatusCodeRange, requestLogStatusCodeRanges);
        promSink.pushMessage(messages);

        verify(instrumentation, times(1)).captureFailedExecutionTelemetry(any(IOException.class), anyInt());
    }

    @Test
    public void shouldCloseStencilClient() throws IOException {
        PromSink promSink = new PromSink(instrumentation, request, httpClient, stencilClient, retryStatusCodeRange, requestLogStatusCodeRanges);

        promSink.close();
        verify(stencilClient, times(1)).close();
    }

    @Test
    public void shouldLogConnectionClosing() throws IOException {
        PromSink promSink = new PromSink(instrumentation, request, httpClient, stencilClient, retryStatusCodeRange, requestLogStatusCodeRanges);

        promSink.close();
        verify(instrumentation, times(1)).logInfo("HTTP connection closing");
    }

    @Test
    public void shouldLogEntireRequestIfInStatusCodeRangeAndCaptureDroppedMessages() throws Exception {
        String body = "timeseries {\n  labels {\n    name: \"__name__\"\n    value: \"test_metric\"\n  }\n  samples {\n    value: 10.0\n    timestamp_ms: 1000000\n  }\n}\n";
        when(response.getStatusLine()).thenReturn(statusLine);
        when(statusLine.getStatusCode()).thenReturn(500);
        when(httpPost.getMethod()).thenReturn("POST");
        when(httpPost.getURI()).thenReturn(new URI("http://dummy.com"));
        when(httpPost.getAllHeaders()).thenReturn(new Header[]{new BasicHeader("Accept", "text/plain")});
        when(httpPost.getEntity()).thenReturn(httpEntity);
        when(httpEntity.getContent()).thenReturn(new ByteArrayInputStream(Snappy.compress(writeRequest.toByteArray())));
        when(request.build(messages)).thenReturn(httpPostList);
        when(httpClient.execute(httpPost)).thenReturn(response);

        PromSink promSink = new PromSink(instrumentation, request, httpClient, stencilClient,
                retryStatusCodeRange, new RangeToHashMapConverter().convert(null, "400-505"));
        promSink.prepare(messages);
        promSink.execute();
        verify(instrumentation, times(1)).logInfo(
                "\nRequest Method: POST"
                        + "\nRequest Url: http://dummy.com"
                        + "\nRequest Headers: [Accept: text/plain]"
                        + "\nRequest Body: " + body);
        verify(instrumentation, times(1)).logInfo("Message dropped because of status code: 500");
        verify(instrumentation, times(1)).captureCountWithTags("firehose_sink_messages_drop_total", 1, "cause= 500");
    }

    @Test
    public void shouldNotLogEntireRequestIfNotInStatusCodeRange() throws Exception {
        String body = "[timeseries {\n  labels {\n    name: \"__name__\"\n    value: \"test_metric\"\n  }\n  samples {\n    value: 10.0\n    timestamp_ms: 1000000\n  }\n}\n]";

        when(response.getStatusLine()).thenReturn(statusLine);
        when(statusLine.getStatusCode()).thenReturn(500);
        when(httpPost.getURI()).thenReturn(new URI("http://dummy.com"));
        when(httpPost.getEntity()).thenReturn(httpEntity);
        when(httpEntity.getContent()).thenReturn(new ByteArrayInputStream(Snappy.compress(writeRequest.toByteArray())));
        when(request.build(messages)).thenReturn(httpPostList);
        when(httpClient.execute(httpPost)).thenReturn(response);

        PromSink promSink = new PromSink(instrumentation, request, httpClient, stencilClient,
                retryStatusCodeRange, new RangeToHashMapConverter().convert(null, "400-499"));
        promSink.prepare(messages);
        promSink.execute();
        verify(instrumentation, times(0)).logInfo(
                "\nRequest Method: POST"
                        + "\nRequest Url: http://dummy.com"
                        + "\nRequest Headers: [Accept: text/plain]"
                        + "\nRequest Body: " + body);
    }

    @Test
    public void shouldCaptureDroppedMessagesMetricsIfNotInStatusCodeRange() throws Exception {
        when(response.getStatusLine()).thenReturn(statusLine);
        when(statusLine.getStatusCode()).thenReturn(500);
        when(httpPost.getURI()).thenReturn(new URI("http://dummy.com"));
        when(httpPost.getEntity()).thenReturn(httpEntity);
        when(httpEntity.getContent()).thenReturn(new ByteArrayInputStream(Snappy.compress(writeRequest.toByteArray())));
        when(request.build(messages)).thenReturn(httpPostList);
        when(httpClient.execute(httpPost)).thenReturn(response);

        PromSink promSink = new PromSink(instrumentation, request, httpClient, stencilClient,
                new RangeToHashMapConverter().convert(null, "400-499"), requestLogStatusCodeRanges);
        promSink.prepare(messages);
        promSink.execute();
        verify(instrumentation, times(1)).logInfo("Message dropped because of status code: 500");
        verify(instrumentation, times(1)).captureCountWithTags("firehose_sink_messages_drop_total", 1, "cause= 500");
    }

    @Test(expected = NeedToRetry.class)
    public void shouldNotCaptureDroppedMessagesMetricsIfInStatusCodeRange() throws Exception {
        when(response.getStatusLine()).thenReturn(statusLine);
        when(statusLine.getStatusCode()).thenReturn(500);
        when(httpPost.getURI()).thenReturn(new URI("http://dummy.com"));
        when(request.build(messages)).thenReturn(httpPostList);
        when(httpClient.execute(httpPost)).thenReturn(response);

        PromSink promSink = new PromSink(instrumentation, request, httpClient, stencilClient,
                new RangeToHashMapConverter().convert(null, "400-600"), requestLogStatusCodeRanges);
        promSink.prepare(messages);
        promSink.execute();
        verify(instrumentation, times(0)).logInfo("Message dropped because of status code: 500");
    }

    @Test
    public void shouldNotCaptureDroppedMessagesMetricsIfStatusCodeIs200() throws Exception {
        when(response.getStatusLine()).thenReturn(statusLine);
        when(statusLine.getStatusCode()).thenReturn(200);
        when(httpPost.getURI()).thenReturn(new URI("http://dummy.com"));
        when(request.build(messages)).thenReturn(httpPostList);
        when(httpClient.execute(httpPost)).thenReturn(response);

        PromSink promSink = new PromSink(instrumentation, request, httpClient, stencilClient,
                retryStatusCodeRange, requestLogStatusCodeRanges);
        promSink.prepare(messages);
        promSink.execute();
        verify(instrumentation, times(0)).logInfo("Message dropped because of status code: 200");
        verify(instrumentation, times(0)).captureCountWithTags("firehose_sink_messages_drop_total", 1, "200");
    }

    @Test
    public void shouldNotCaptureDroppedMessagesMetricsIfStatusCodeIs201() throws Exception {
        when(response.getStatusLine()).thenReturn(statusLine);
        when(statusLine.getStatusCode()).thenReturn(201);
        when(httpPost.getURI()).thenReturn(new URI("http://dummy.com"));
        when(request.build(messages)).thenReturn(httpPostList);
        when(httpClient.execute(httpPost)).thenReturn(response);

        PromSink promSink = new PromSink(instrumentation, request, httpClient, stencilClient,
                retryStatusCodeRange, requestLogStatusCodeRanges);
        promSink.prepare(messages);
        promSink.execute();
        verify(instrumentation, times(0)).logInfo("Message dropped because of status code: 201");
        verify(instrumentation, times(0)).captureCountWithTags("firehose_sink_messages_drop_total", 1, "201");
    }

    @Test
    public void shouldCaptureResponseStatusCount() throws Exception {
        when(response.getStatusLine()).thenReturn(statusLine);
        when(statusLine.getStatusCode()).thenReturn(200);
        URI uri = new URI("http://dummy.com");
        when(httpPost.getURI()).thenReturn(uri);
        when(request.build(messages)).thenReturn(httpPostList);
        when(httpClient.execute(httpPost)).thenReturn(response);

        PromSink promSink = new PromSink(instrumentation, request, httpClient, stencilClient,
                retryStatusCodeRange, requestLogStatusCodeRanges);
        promSink.prepare(messages);
        promSink.execute();

        verify(instrumentation, times(1)).captureCountWithTags("firehose_sink_http_response_code_total", 1, "status_code=" + statusLine.getStatusCode(), "url=" + uri.getPath());
    }

    @Test
    public void shouldReadSnappyCompressedContent() throws Exception {
        String body = "[timeseries {\n  labels {\n    name: \"__name__\"\n    value: \"test_metric\"\n  }\n  samples {\n    value: 10.0\n    timestamp_ms: 1000000\n  }\n}\n]";
        InputStream inputStream = new ByteArrayInputStream(Snappy.compress(writeRequest.toByteArray()));
        when(httpPost.getEntity()).thenReturn(httpEntity);
        when(httpEntity.getContent()).thenReturn(inputStream);

        PromSink promSink = new PromSink(instrumentation, request, httpClient, stencilClient,
                retryStatusCodeRange, requestLogStatusCodeRanges);

        List<String> requestBody = promSink.readContent(inputStream);
        assertEquals(body, requestBody.toString());
    }
}
