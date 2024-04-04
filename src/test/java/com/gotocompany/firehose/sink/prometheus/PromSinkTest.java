package com.gotocompany.firehose.sink.prometheus;


import com.gotocompany.firehose.config.converter.RangeToHashMapConverter;
import com.gotocompany.firehose.exception.DeserializerException;
import com.gotocompany.firehose.message.Message;
import com.gotocompany.firehose.metrics.FirehoseInstrumentation;
import com.gotocompany.firehose.sink.prometheus.request.PromRequest;
import cortexpb.Cortex;
import com.gotocompany.stencil.client.StencilClient;
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
import org.mockito.junit.MockitoJUnitRunner;
import org.xerial.snappy.Snappy;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;

@RunWith(MockitoJUnitRunner.class)
public class PromSinkTest {
    @Mock
    private FirehoseInstrumentation firehoseInstrumentation;
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
        Cortex.LabelPair labelPair = Cortex.LabelPair.newBuilder().setName(PromSinkConstants.PROMETHEUS_LABEL_FOR_METRIC_NAME).setValue("test_metric").build();
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

        PromSink promSink = new PromSink(firehoseInstrumentation, request, httpClient, stencilClient, retryStatusCodeRange, requestLogStatusCodeRanges);
        promSink.prepare(messages);
        promSink.execute();

        verify(request, times(1)).build(messages);
        verify(httpClient, times(1)).execute(httpPost);
    }

    @Test
    public void shouldReturnBackFailedMessagesWhenResponseCodeIsGivenRange() throws Exception {
        when(response.getStatusLine()).thenReturn(statusLine);
        when(statusLine.getStatusCode()).thenReturn(500);
        when(httpPost.getURI()).thenReturn(new URI("http://dummy.com"));
        when(request.build(messages)).thenReturn(httpPostList);
        when(httpClient.execute(httpPost)).thenReturn(response);

        PromSink promSink = new PromSink(firehoseInstrumentation, request, httpClient, stencilClient,
                new RangeToHashMapConverter().convert(null, "400-505"), requestLogStatusCodeRanges);
        promSink.prepare(messages);
        List<Message> failedMessages = promSink.execute();

        assertFalse(failedMessages.isEmpty());
        assertEquals(1, failedMessages.size());
    }

    @Test
    public void shouldReturnBackFailedMessagesWhenResponseCodeIsNull() throws Exception {
        when(httpPost.getURI()).thenReturn(new URI("http://dummy.com"));
        when(httpPost.getAllHeaders()).thenReturn(new Header[]{});
        when(httpPost.getEntity()).thenReturn(httpEntity);
        when(httpEntity.getContent()).thenReturn(new ByteArrayInputStream(Snappy.compress(writeRequest.toByteArray())));
        when(request.build(messages)).thenReturn(httpPostList);
        when(httpClient.execute(httpPost)).thenReturn(null);

        PromSink promSink = new PromSink(firehoseInstrumentation, request, httpClient, stencilClient, retryStatusCodeRange, requestLogStatusCodeRanges);
        promSink.prepare(messages);
        List<Message> failedMessages = promSink.execute();

        assertFalse(failedMessages.isEmpty());
        assertEquals(1, failedMessages.size());
    }

    @Test(expected = IOException.class)
    public void shouldCatchURISyntaxExceptionAndThrowIOException() throws URISyntaxException, DeserializerException, IOException, SQLException {
        when(request.build(messages)).thenThrow(new URISyntaxException("", ""));

        PromSink promSink = new PromSink(firehoseInstrumentation, request, httpClient, stencilClient, retryStatusCodeRange, requestLogStatusCodeRanges);
        promSink.prepare(messages);
    }

    @Test
    public void shouldCloseStencilClient() throws IOException {
        PromSink promSink = new PromSink(firehoseInstrumentation, request, httpClient, stencilClient, retryStatusCodeRange, requestLogStatusCodeRanges);

        promSink.close();
        verify(stencilClient, times(1)).close();
    }

    @Test
    public void shouldLogConnectionClosing() throws IOException {
        PromSink promSink = new PromSink(firehoseInstrumentation, request, httpClient, stencilClient, retryStatusCodeRange, requestLogStatusCodeRanges);

        promSink.close();
        verify(firehoseInstrumentation, times(1)).logInfo("HTTP connection closing");
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

        PromSink promSink = new PromSink(firehoseInstrumentation, request, httpClient, stencilClient,
                retryStatusCodeRange, new RangeToHashMapConverter().convert(null, "400-505"));
        promSink.prepare(messages);
        promSink.execute();
        verify(firehoseInstrumentation, times(1)).logInfo(
                "\nRequest Method: POST"
                        + "\nRequest Url: http://dummy.com"
                        + "\nRequest Headers: [Accept: text/plain]"
                        + "\nRequest Body: " + body);
        verify(firehoseInstrumentation, times(1)).logInfo("Message dropped because of status code: 500");
        verify(firehoseInstrumentation, times(1)).captureCount("firehose_sink_messages_drop_total", 1L, "cause= 500");
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

        PromSink promSink = new PromSink(firehoseInstrumentation, request, httpClient, stencilClient,
                retryStatusCodeRange, new RangeToHashMapConverter().convert(null, "400-499"));
        promSink.prepare(messages);
        promSink.execute();
        verify(firehoseInstrumentation, times(0)).logInfo(
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

        PromSink promSink = new PromSink(firehoseInstrumentation, request, httpClient, stencilClient,
                new RangeToHashMapConverter().convert(null, "400-499"), requestLogStatusCodeRanges);
        promSink.prepare(messages);
        promSink.execute();
        verify(firehoseInstrumentation, times(1)).logInfo("Message dropped because of status code: 500");
        verify(firehoseInstrumentation, times(1)).captureCount("firehose_sink_messages_drop_total", 1L, "cause= 500");
    }

    @Test
    public void shouldNotCaptureDroppedMessagesMetricsIfInStatusCodeRange() throws Exception {
        when(response.getStatusLine()).thenReturn(statusLine);
        when(statusLine.getStatusCode()).thenReturn(500);
        when(httpPost.getURI()).thenReturn(new URI("http://dummy.com"));
        when(request.build(messages)).thenReturn(httpPostList);
        when(httpClient.execute(httpPost)).thenReturn(response);

        PromSink promSink = new PromSink(firehoseInstrumentation, request, httpClient, stencilClient,
                new RangeToHashMapConverter().convert(null, "400-600"), requestLogStatusCodeRanges);
        promSink.prepare(messages);
        promSink.execute();
        verify(firehoseInstrumentation, times(0)).logInfo("Message dropped because of status code: 500");
    }

    @Test
    public void shouldNotCaptureDroppedMessagesMetricsIfStatusCodeIs200() throws Exception {
        when(response.getStatusLine()).thenReturn(statusLine);
        when(statusLine.getStatusCode()).thenReturn(200);
        when(httpPost.getURI()).thenReturn(new URI("http://dummy.com"));
        when(request.build(messages)).thenReturn(httpPostList);
        when(httpClient.execute(httpPost)).thenReturn(response);

        PromSink promSink = new PromSink(firehoseInstrumentation, request, httpClient, stencilClient,
                retryStatusCodeRange, requestLogStatusCodeRanges);
        promSink.prepare(messages);
        promSink.execute();
        verify(firehoseInstrumentation, times(0)).logInfo("Message dropped because of status code: 200");
        verify(firehoseInstrumentation, times(0)).captureCount("firehose_sink_messages_drop_total", 1L, "200");
    }

    @Test
    public void shouldNotCaptureDroppedMessagesMetricsIfStatusCodeIs201() throws Exception {
        when(response.getStatusLine()).thenReturn(statusLine);
        when(statusLine.getStatusCode()).thenReturn(201);
        when(httpPost.getURI()).thenReturn(new URI("http://dummy.com"));
        when(request.build(messages)).thenReturn(httpPostList);
        when(httpClient.execute(httpPost)).thenReturn(response);

        PromSink promSink = new PromSink(firehoseInstrumentation, request, httpClient, stencilClient,
                retryStatusCodeRange, requestLogStatusCodeRanges);
        promSink.prepare(messages);
        promSink.execute();
        verify(firehoseInstrumentation, times(0)).logInfo("Message dropped because of status code: 201");
        verify(firehoseInstrumentation, times(0)).captureCount("firehose_sink_messages_drop_total", 1L, "201");
    }

    @Test
    public void shouldCaptureResponseStatusCount() throws Exception {
        when(response.getStatusLine()).thenReturn(statusLine);
        when(statusLine.getStatusCode()).thenReturn(200);
        URI uri = new URI("http://dummy.com");
        when(httpPost.getURI()).thenReturn(uri);
        when(request.build(messages)).thenReturn(httpPostList);
        when(httpClient.execute(httpPost)).thenReturn(response);

        PromSink promSink = new PromSink(firehoseInstrumentation, request, httpClient, stencilClient,
                retryStatusCodeRange, requestLogStatusCodeRanges);
        promSink.prepare(messages);
        promSink.execute();

        verify(firehoseInstrumentation, times(1)).captureCount("firehose_sink_http_response_code_total", 1L, "status_code=" + statusLine.getStatusCode());
    }

    @Test
    public void shouldReadSnappyCompressedContent() throws Exception {
        String body = "[timeseries {\n  labels {\n    name: \"__name__\"\n    value: \"test_metric\"\n  }\n  samples {\n    value: 10.0\n    timestamp_ms: 1000000\n  }\n}\n]";
        InputStream inputStream = new ByteArrayInputStream(Snappy.compress(writeRequest.toByteArray()));
        when(httpPost.getEntity()).thenReturn(httpEntity);
        when(httpEntity.getContent()).thenReturn(inputStream);

        PromSink promSink = new PromSink(firehoseInstrumentation, request, httpClient, stencilClient,
                retryStatusCodeRange, requestLogStatusCodeRanges);

        List<String> requestBody = promSink.readContent(httpPost);
        assertEquals(body, requestBody.toString());
    }
}
