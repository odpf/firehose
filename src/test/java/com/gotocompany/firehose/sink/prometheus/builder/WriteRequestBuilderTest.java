package com.gotocompany.firehose.sink.prometheus.builder;


import com.gotocompany.firehose.message.Message;
import com.gotocompany.firehose.sink.prometheus.PromSinkConstants;
import com.gotocompany.firehose.consumer.TestFeedbackLogMessage;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Timestamp;
import cortexpb.Cortex;
import com.gotocompany.stencil.Parser;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.*;
import static org.mockito.MockitoAnnotations.initMocks;

public class WriteRequestBuilderTest {

    @Mock
    private TimeSeriesBuilder timeSeriesBuilder;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Mock
    private Parser protoParser;

    private Message message1;
    private Message message2;
    private Cortex.LabelPair labelPair1;
    private Cortex.LabelPair labelPair2;
    private Cortex.LabelPair labelPair3;
    private Cortex.Sample sample1;
    private Cortex.Sample sample2;
    private DynamicMessage dynamicMessage1;
    private DynamicMessage dynamicMessage2;
    private List<Message> messages;

    @Before
    public void setUp() throws InvalidProtocolBufferException {
        initMocks(this);

        message1 = new Message("key".getBytes(), "msg1".getBytes(), "topic1", 0, 100);
        message2 = new Message("key".getBytes(), "msg2".getBytes(), "topic1", 1, 100);
        messages = new ArrayList<>();
        messages.add(message1);
        messages.add(message2);

        TestFeedbackLogMessage feedbackLogMessage1 = TestFeedbackLogMessage.newBuilder()
                .setTipAmount(100)
                .setEventTimestamp(Timestamp.newBuilder().setSeconds(10000)).build();

        TestFeedbackLogMessage feedbackLogMessage2 = TestFeedbackLogMessage.newBuilder()
                .setTipAmount(200)
                .setEventTimestamp(Timestamp.newBuilder().setSeconds(8000)).build();

        dynamicMessage1 = DynamicMessage.parseFrom(TestFeedbackLogMessage.getDescriptor(), feedbackLogMessage1.toByteArray());
        dynamicMessage2 = DynamicMessage.parseFrom(TestFeedbackLogMessage.getDescriptor(), feedbackLogMessage2.toByteArray());
        labelPair1 = Cortex.LabelPair.newBuilder().setName(PromSinkConstants.PROMETHEUS_LABEL_FOR_METRIC_NAME).setValue("tip_amount").build();
        labelPair2 = Cortex.LabelPair.newBuilder().setName(PromSinkConstants.KAFKA_PARTITION).setValue("0").build();
        labelPair3 = Cortex.LabelPair.newBuilder().setName(PromSinkConstants.KAFKA_PARTITION).setValue("1").build();
        sample1 = Cortex.Sample.newBuilder().setTimestampMs(10000).setValue(100).build();
        sample2 = Cortex.Sample.newBuilder().setTimestampMs(8000).setValue(200).build();
    }

    @Test
    public void shouldReturnWriteRequest() throws InvalidProtocolBufferException {
        Cortex.TimeSeries timeSeries = Cortex.TimeSeries.newBuilder()
                .addLabels(labelPair1).addSamples(sample1)
                .addLabels(labelPair2)
                .build();

        Mockito.when(timeSeriesBuilder.buildTimeSeries(dynamicMessage1, message1.getPartition())).thenReturn(Collections.singletonList(timeSeries));
        Mockito.when(protoParser.parse(message1.getLogMessage())).thenReturn(dynamicMessage1);

        Cortex.WriteRequest writeRequest = new WriteRequestBuilder(timeSeriesBuilder, protoParser)
                .buildWriteRequest(Collections.singletonList(message1));

        String expected = "timeseries {\n  labels {\n    name: \"__name__\"\n    value: \"tip_amount\"\n  }\n  labels {\n    name: \"kafka_partition\"\n    value: \"0\"\n  }\n  samples {\n    value: 100.0\n    timestamp_ms: 10000\n  }\n}\n";
        assertEquals(expected, writeRequest.toString());
    }

    @Test
    public void shouldReturnWriteRequestWithSortedTimeSeries() throws InvalidProtocolBufferException {
        Cortex.TimeSeries timeSeries1 = Cortex.TimeSeries.newBuilder()
                .addLabels(labelPair1).addSamples(sample1)
                .addLabels(labelPair2)
                .build();
        Cortex.TimeSeries timeSeries2 = Cortex.TimeSeries.newBuilder()
                .addLabels(labelPair1).addSamples(sample2)
                .addLabels(labelPair3)
                .build();

        Mockito.when(timeSeriesBuilder.buildTimeSeries(dynamicMessage1, message1.getPartition())).thenReturn(Collections.singletonList(timeSeries1));
        Mockito.when(protoParser.parse(message1.getLogMessage())).thenReturn(dynamicMessage1);
        Mockito.when(timeSeriesBuilder.buildTimeSeries(dynamicMessage2, message2.getPartition())).thenReturn(Collections.singletonList(timeSeries2));
        Mockito.when(protoParser.parse(message2.getLogMessage())).thenReturn(dynamicMessage2);

        Cortex.WriteRequest writeRequest = new WriteRequestBuilder(timeSeriesBuilder, protoParser)
                .buildWriteRequest(messages);

        String expected = "timeseries {\n  labels {\n    name: \"__name__\"\n    value: \"tip_amount\"\n  }\n  labels {\n    name: \"kafka_partition\"\n    value: \"1\"\n  }\n  samples {\n    value: 200.0\n    timestamp_ms: 8000\n  }\n}\n"
                + "timeseries {\n  labels {\n    name: \"__name__\"\n    value: \"tip_amount\"\n  }\n  labels {\n    name: \"kafka_partition\"\n    value: \"0\"\n  }\n  samples {\n    value: 100.0\n    timestamp_ms: 10000\n  }\n}\n";
        assertEquals(expected, writeRequest.toString());
    }

    @Test
    public void shouldReturnEmptyRequest() throws InvalidProtocolBufferException {
        Cortex.WriteRequest writeRequest = new WriteRequestBuilder(timeSeriesBuilder, protoParser)
                .buildWriteRequest(new ArrayList<>());
        assertTrue(writeRequest.toString().isEmpty());
    }

    @Test
    public void shouldThrowException() throws InvalidProtocolBufferException {
        expectedException.expect(InvalidProtocolBufferException.class);
        expectedException.expectMessage("Invalid");
        Mockito.when(protoParser.parse(message1.getLogMessage())).thenThrow(new InvalidProtocolBufferException("Invalid"));
        new WriteRequestBuilder(timeSeriesBuilder, protoParser).buildWriteRequest(messages);
    }
}
