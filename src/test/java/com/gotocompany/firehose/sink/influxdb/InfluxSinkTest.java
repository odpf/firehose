package com.gotocompany.firehose.sink.influxdb;




import com.gotocompany.firehose.config.InfluxSinkConfig;
import com.gotocompany.firehose.exception.DeserializerException;
import com.gotocompany.firehose.message.Message;
import com.gotocompany.firehose.metrics.FirehoseInstrumentation;
import com.gotocompany.firehose.sink.Sink;
import com.gotocompany.firehose.consumer.TestBookingLogMessage;
import com.gotocompany.firehose.consumer.TestFeedbackLogKey;
import com.gotocompany.firehose.consumer.TestFeedbackLogMessage;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Timestamp;
import com.gotocompany.stencil.StencilClientFactory;
import com.gotocompany.stencil.client.StencilClient;
import com.gotocompany.stencil.Parser;
import org.aeonbits.owner.ConfigFactory;
import org.influxdb.InfluxDB;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class InfluxSinkTest {
    private static final int TIMESTAMP_IN_EPOCH_SECONDS = 1498600;
    private final String measurementName = "FeedbackMeasure";
    private final String databaseName = "test";
    private final String emptyFieldNameIndex = "{}";
    private final String emptyTagNameIndexMapping = "{}";
    private final String orderNumber = "lol";
    private final String feedbackComment = "good";
    private final String driverId = "3";
    private final float tipAmount = 3.14f;
    private final int feedbackOrderNumberIndex = 1;
    private final int feedbackCommentIndex = 6;
    private final int tipAmountIndex = 7;
    private final int driverIdIndex = 3;
    private Sink sink;
    private Message message;
    private Point expectedPoint;
    private InfluxSinkConfig config;
    private Properties props = new Properties();
    private Point.Builder pointBuilder;
    private StencilClient stencilClient;
    private List<Message> messages;

    @Mock
    private InfluxDB client;

    @Mock
    private Parser protoParser;

    @Mock
    private FirehoseInstrumentation firehoseInstrumentation;

    @Mock
    private StencilClient mockStencilClient;

    @Before
    public void setUp() {
        props.setProperty("SINK_INFLUX_MEASUREMENT_NAME", measurementName);
        props.setProperty("SINK_INFLUX_PROTO_EVENT_TIMESTAMP_INDEX", "2");
        props.setProperty("SINK_INFLUX_DB_NAME", databaseName);
        props.setProperty("INPUT_SCHEMA_PROTO_CLASS", TestFeedbackLogMessage.class.getName());

        TestFeedbackLogMessage feedbackLogMessage = TestFeedbackLogMessage.newBuilder().setDriverId(driverId).setOrderNumber(orderNumber)
                .setFeedbackComment(feedbackComment).setTipAmount(tipAmount).setEventTimestamp(
                        Timestamp.newBuilder().setSeconds(TIMESTAMP_IN_EPOCH_SECONDS).build()).build();
        TestFeedbackLogKey feedbackLogKey = TestFeedbackLogKey.newBuilder().setOrderNumber(orderNumber).build();

        message = new Message(feedbackLogKey.toByteArray(), feedbackLogMessage.toByteArray(), databaseName, 1, 1);
        messages = Collections.singletonList(message);

        pointBuilder = Point.measurement(measurementName).addField("feedbackOrderNumber", orderNumber)
                .addField("comment", feedbackComment).addField("tipAmount", tipAmount).time(TIMESTAMP_IN_EPOCH_SECONDS, TimeUnit.SECONDS);
        stencilClient = StencilClientFactory.getClient();
    }

    @Test
    public void shouldPrepareBatchPoints() throws IOException, DeserializerException {
        setupFieldNameIndexMappingProperties();
        setupTagNameIndexMappingProperties();
        props.setProperty("SINK_INFLUX_PROTO_EVENT_TIMESTAMP_INDEX", "5");
        config = ConfigFactory.create(InfluxSinkConfig.class, props);

        DynamicMessage dynamicMessage = DynamicMessage.newBuilder(TestBookingLogMessage.getDescriptor()).build();
        when(protoParser.parse(any())).thenReturn(dynamicMessage);
        InfluxSinkStub influx = new InfluxSinkStub(firehoseInstrumentation, "influx", config, protoParser, client, stencilClient);

        influx.prepare(messages);
        verify(protoParser, times(1)).parse(message.getLogMessage());
    }

    @Test
    public void shouldPushTagsAsStringValues() throws DeserializerException, IOException {
        expectedPoint = pointBuilder.tag("driver_id", driverId).build();
        setupFieldNameIndexMappingProperties();
        setupTagNameIndexMappingProperties();
        config = ConfigFactory.create(InfluxSinkConfig.class, props);

        sink = new InfluxSink(firehoseInstrumentation, "influx", config, stencilClient.getParser(config.getInputSchemaProtoClass()), client, stencilClient);

        ArgumentCaptor<BatchPoints> batchPointsArgumentCaptor = ArgumentCaptor.forClass(BatchPoints.class);

        sink.pushMessage(messages);
        verify(client, times(1)).write(batchPointsArgumentCaptor.capture());
        List<BatchPoints> batchPointsList = batchPointsArgumentCaptor.getAllValues();

        assertEquals(expectedPoint.lineProtocol(), batchPointsList.get(0).getPoints().get(0).lineProtocol());
    }

    @Test
    public void shouldThrowExceptionOnEmptyFieldNameIndexMapping() throws IOException, DeserializerException {
        props.setProperty("SINK_INFLUX_FIELD_NAME_PROTO_INDEX_MAPPING", emptyFieldNameIndex);
        props.setProperty("SINK_INFLUX_TAG_NAME_PROTO_INDEX_MAPPING", emptyTagNameIndexMapping);
        config = ConfigFactory.create(InfluxSinkConfig.class, props);
        sink = new InfluxSink(firehoseInstrumentation, "influx", config, stencilClient.getParser(config.getInputSchemaProtoClass()), client, stencilClient);

        try {
            sink.pushMessage(messages);
        } catch (Exception e) {
            assertEquals(InfluxSink.FIELD_NAME_MAPPING_ERROR_MESSAGE, e.getMessage());
        }
    }

    @Test
    public void shouldPushMessagesWithType() throws DeserializerException, IOException {
        expectedPoint = pointBuilder.build();
        setupFieldNameIndexMappingProperties();
        props.setProperty("SINK_INFLUX_TAG_NAME_PROTO_INDEX_MAPPING", emptyTagNameIndexMapping);
        config = ConfigFactory.create(InfluxSinkConfig.class, props);
        sink = new InfluxSink(firehoseInstrumentation, "influx", config, stencilClient.getParser(config.getInputSchemaProtoClass()), client, stencilClient);
        ArgumentCaptor<BatchPoints> batchPointsArgumentCaptor = ArgumentCaptor.forClass(BatchPoints.class);

        sink.pushMessage(messages);
        verify(firehoseInstrumentation, times(1)).capturePreExecutionLatencies(messages);
        verify(firehoseInstrumentation, times(1)).startExecution();
        verify(firehoseInstrumentation, times(1)).logInfo("Preparing {} messages", messages.size());
        verify(client, times(1)).write(batchPointsArgumentCaptor.capture());
        List<BatchPoints> batchPointsList = batchPointsArgumentCaptor.getAllValues();

        assertEquals(expectedPoint.lineProtocol(), batchPointsList.get(0).getPoints().get(0).lineProtocol());
    }

    @Test
    public void shouldCloseStencilClient() throws IOException {
        config = ConfigFactory.create(InfluxSinkConfig.class, props);

        sink = new InfluxSink(firehoseInstrumentation, "influx", config, mockStencilClient.getParser(config.getInputSchemaProtoClass()), client, mockStencilClient);
        sink.close();

        verify(mockStencilClient, times(1)).close();
    }

    @Test
    public void shouldLogWhenClosingConnection() throws IOException {
        config = ConfigFactory.create(InfluxSinkConfig.class, props);

        sink = new InfluxSink(firehoseInstrumentation, "influx", config, mockStencilClient.getParser(config.getInputSchemaProtoClass()), client, mockStencilClient);
        sink.close();

        verify(firehoseInstrumentation, times(1)).logInfo("InfluxDB connection closing");
    }

    @Test
    public void shouldLogDataPointAndBatchPoints() throws IOException, DeserializerException {
        setupFieldNameIndexMappingProperties();
        setupTagNameIndexMappingProperties();
        config = ConfigFactory.create(InfluxSinkConfig.class, props);

        sink = new InfluxSink(firehoseInstrumentation, "influx", config, stencilClient.getParser(config.getInputSchemaProtoClass()), client, stencilClient);
        ArgumentCaptor<BatchPoints> batchPointsArgumentCaptor = ArgumentCaptor.forClass(BatchPoints.class);

        sink.pushMessage(messages);
        verify(client, times(1)).write(batchPointsArgumentCaptor.capture());
        List<BatchPoints> batchPointsList = batchPointsArgumentCaptor.getAllValues();

        verify(firehoseInstrumentation, times(1)).logInfo("Preparing {} messages", messages.size());
        verify(firehoseInstrumentation, times(1)).logDebug("Data point: {}", batchPointsList.get(0).getPoints().get(0).toString());
        verify(firehoseInstrumentation, times(1)).logDebug("Batch points: {}", batchPointsList.get(0).toString());
    }

    private void setupFieldNameIndexMappingProperties() {
        props.setProperty("SINK_INFLUX_FIELD_NAME_PROTO_INDEX_MAPPING", String.format("{\"%d\":\"feedbackOrderNumber\", \"%d\":\"comment\" , \"%d\":\"tipAmount\"}", feedbackOrderNumberIndex, feedbackCommentIndex, tipAmountIndex));
    }

    private void setupTagNameIndexMappingProperties() {
        props.setProperty("SINK_INFLUX_TAG_NAME_PROTO_INDEX_MAPPING", String.format("{\"%d\":\"driver_id\"}", driverIdIndex));
    }

    public class InfluxSinkStub extends InfluxSink {
        public InfluxSinkStub(FirehoseInstrumentation firehoseInstrumentation, String sinkType, InfluxSinkConfig config, Parser protoParser, InfluxDB client, StencilClient stencilClient) {
            super(firehoseInstrumentation, sinkType, config, protoParser, client, stencilClient);
        }

        public void prepare(List<Message> messageList) throws IOException {
            super.prepare(messageList);
        }
    }

}
