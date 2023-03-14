package com.gotocompany.firehose.sink.prometheus.builder;

import com.google.protobuf.DynamicMessage;
import com.gotocompany.firehose.sink.prometheus.PromSinkConstants;
import cortexpb.Cortex;
import org.apache.commons.io.IOUtils;
import org.apache.http.entity.ByteArrayEntity;
import org.junit.Assert;
import org.junit.Test;
import org.xerial.snappy.Snappy;

import java.io.IOException;

public class RequestEntityBuilderTest {

    @Test
    public void shouldCreateSnappyCompressedByteArrayEntity() throws IOException {
        Cortex.Sample sample = Cortex.Sample.newBuilder().setValue(10).setTimestampMs(System.currentTimeMillis()).build();
        Cortex.LabelPair labelPair = Cortex.LabelPair.newBuilder().setName(PromSinkConstants.PROMETHEUS_LABEL_FOR_METRIC_NAME).setValue("test_metric").build();
        Cortex.TimeSeries timeSeries = Cortex.TimeSeries.newBuilder()
                .addSamples(sample).addLabels(labelPair)
                .build();
        Cortex.WriteRequest writeRequest = Cortex.WriteRequest.newBuilder().addTimeseries(timeSeries).build();

        RequestEntityBuilder requestEntityBuilder = new RequestEntityBuilder();
        ByteArrayEntity byteArrayEntity = requestEntityBuilder.buildHttpEntity(writeRequest);

        byte[] uncompressedSnappy = Snappy.uncompress(IOUtils.toByteArray(byteArrayEntity.getContent()));
        DynamicMessage actual = DynamicMessage.parseFrom(Cortex.WriteRequest.getDescriptor(), uncompressedSnappy);
        DynamicMessage expected = DynamicMessage.parseFrom(Cortex.WriteRequest.getDescriptor(), writeRequest.toByteArray());

        Assert.assertEquals(expected, actual);
    }
}
