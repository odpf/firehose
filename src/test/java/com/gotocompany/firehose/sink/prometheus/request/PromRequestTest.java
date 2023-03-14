package com.gotocompany.firehose.sink.prometheus.request;

import com.google.protobuf.DynamicMessage;
import com.gotocompany.firehose.message.Message;
import com.gotocompany.firehose.metrics.FirehoseInstrumentation;
import com.gotocompany.firehose.sink.prometheus.PromSinkConstants;
import com.gotocompany.firehose.sink.prometheus.builder.HeaderBuilder;
import com.gotocompany.firehose.sink.prometheus.builder.RequestEntityBuilder;
import com.gotocompany.firehose.sink.prometheus.builder.WriteRequestBuilder;
import cortexpb.Cortex;
import org.apache.commons.io.IOUtils;
import org.apache.http.Header;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.message.BasicHeader;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.internal.matchers.apachecommons.ReflectionEquals;
import org.xerial.snappy.Snappy;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;


public class PromRequestTest {

    @Mock
    private FirehoseInstrumentation firehoseInstrumentation;

    @Mock
    private HeaderBuilder headerBuilder;

    @Mock
    private RequestEntityBuilder requestEntityBuilder;

    @Mock
    private WriteRequestBuilder writeRequestBuilder;

    private List<Message> messages;

    @Before
    public void setUp() {
        initMocks(this);
        Message message = new Message("".getBytes(), "".getBytes(), "topic", 0, 100);
        messages = Arrays.asList(message, message, message);
    }

    @Test
    public void shouldProperlyBuildMessages() throws URISyntaxException, IOException {
        Cortex.LabelPair labelPair1 = Cortex.LabelPair.newBuilder().setName(PromSinkConstants.PROMETHEUS_LABEL_FOR_METRIC_NAME).setValue("tip_amount").build();
        Cortex.LabelPair labelPair2 = Cortex.LabelPair.newBuilder().setName(PromSinkConstants.KAFKA_PARTITION).setValue("0").build();
        Cortex.Sample sample = Cortex.Sample.newBuilder().setTimestampMs(10000).setValue(100).build();

        Cortex.TimeSeries timeSeries = Cortex.TimeSeries.newBuilder()
                .addLabels(labelPair1).addSamples(sample)
                .addLabels(labelPair2)
                .build();

        Cortex.WriteRequest writeRequestBody = Cortex.WriteRequest.newBuilder()
                .addTimeseries(timeSeries)
                .build();

        HashMap<String, String> headerMap = new HashMap<>();
        headerMap.put(PromSinkConstants.CONTENT_ENCODING, PromSinkConstants.CONTENT_ENCODING_DEFAULT);
        headerMap.put(PromSinkConstants.PROMETHEUS_REMOTE_WRITE_VERSION, PromSinkConstants.PROMETHEUS_REMOTE_WRITE_VERSION_DEFAULT);

        String url = "dummyEndpoint";
        URI uri = new URI(url);
        byte[] compressedBody = Snappy.compress(writeRequestBody.toByteArray());
        when(headerBuilder.build()).thenReturn(headerMap);
        when(writeRequestBuilder.buildWriteRequest(messages)).thenReturn(writeRequestBody);
        when(requestEntityBuilder.buildHttpEntity(writeRequestBody)).thenReturn(new ByteArrayEntity(compressedBody));

        PromRequest promRequest = new PromRequest(firehoseInstrumentation, headerBuilder, url, requestEntityBuilder, writeRequestBuilder);
        HttpEntityEnclosingRequestBase request = promRequest.build(messages).get(0);

        BasicHeader header1 = new BasicHeader(PromSinkConstants.CONTENT_ENCODING, PromSinkConstants.CONTENT_ENCODING_DEFAULT);
        BasicHeader header2 = new BasicHeader(PromSinkConstants.PROMETHEUS_REMOTE_WRITE_VERSION, PromSinkConstants.PROMETHEUS_REMOTE_WRITE_VERSION_DEFAULT);
        Header[] headers = new Header[2];
        headers[0] = header1;
        headers[1] = header2;

        verify(headerBuilder, times(1)).build();
        verify(writeRequestBuilder, times(1)).buildWriteRequest(messages);
        verify(requestEntityBuilder, times(1)).buildHttpEntity(writeRequestBody);

        byte[] byteArrayIs = IOUtils.toByteArray(request.getEntity().getContent());
        byte[] uncompressedSnappy = Snappy.uncompress(byteArrayIs);
        String requestBody = DynamicMessage.parseFrom(Cortex.WriteRequest.getDescriptor(), uncompressedSnappy).toString();

        assertEquals(requestBody, writeRequestBody.toString());
        assertTrue(new ReflectionEquals(request.getAllHeaders()).matches(headers));
        assertEquals(uri, request.getURI());
    }
}
