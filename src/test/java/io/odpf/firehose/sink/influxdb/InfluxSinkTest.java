package io.odpf.firehose.sink.influxdb;




import io.odpf.firehose.config.InfluxSinkConfig;
import io.odpf.firehose.consumer.Message;
import io.odpf.firehose.consumer.TestBookingLogMessage;
import io.odpf.firehose.consumer.TestFeedbackLogKey;
import io.odpf.firehose.consumer.TestFeedbackLogMessage;
import io.odpf.firehose.exception.DeserializerException;
import io.odpf.firehose.metrics.Instrumentation;
import io.odpf.firehose.sink.Sink;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Timestamp;
import io.odpf.stencil.StencilClientFactory;
import io.odpf.stencil.client.StencilClient;
import io.odpf.stencil.parser.ProtoParser;
import org.aeonbits.owner.ConfigFactory;
import org.influxdb.InfluxDB;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

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
    private ProtoParser protoParser;

    @Mock
    private Instrumentation instrumentation;

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
        InfluxSinkStub influx = new InfluxSinkStub(instrumentation, "influx", config, protoParser, client, stencilClient);

        influx.prepare(messages);
        verify(protoParser, times(1)).parse(message.getLogMessage());
    }

    @Test
    public void shouldPushTagsAsStringValues() throws DeserializerException, IOException {
        expectedPoint = pointBuilder.tag("driver_id", driverId).build();
        setupFieldNameIndexMappingProperties();
        setupTagNameIndexMappingProperties();
        config = ConfigFactory.create(InfluxSinkConfig.class, props);

        sink = new InfluxSink(instrumentation, "influx", config, new ProtoParser(stencilClient, config.getInputSchemaProtoClass()), client, stencilClient);

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
        sink = new InfluxSink(instrumentation, "influx", config, new ProtoParser(stencilClient, config.getInputSchemaProtoClass()), client, stencilClient);

        try {
            sink.pushMessage(messages);
        } catch (Exception e) {
            assertEquals(InfluxSink.FIELD_NAME_MAPPING_ERROR_MESSAGE, e.getMessage());
        }
    }

    @Test
    public void shouldCaptureFailedExecutionTelemetryIncaseOfExceptions() throws DeserializerException, IOException {
        expectedPoint = pointBuilder.tag("driver_id", driverId).build();
        setupFieldNameIndexMappingProperties();
        setupTagNameIndexMappingProperties();
        config = ConfigFactory.create(InfluxSinkConfig.class, props);
        sink = new InfluxSink(instrumentation, "influx", config, new ProtoParser(stencilClient, config.getInputSchemaProtoClass()), client, stencilClient);

        RuntimeException runtimeException = new RuntimeException();
        doThrow(runtimeException).when(instrumentation).startExecution();

        sink.pushMessage(messages);

        verify(instrumentation, times(1)).captureFailedExecutionTelemetry(runtimeException, messages.size());
    }


    @Test
    public void shouldPushMessagesWithType() throws DeserializerException, IOException {
        expectedPoint = pointBuilder.build();
        setupFieldNameIndexMappingProperties();
        props.setProperty("SINK_INFLUX_TAG_NAME_PROTO_INDEX_MAPPING", emptyTagNameIndexMapping);
        config = ConfigFactory.create(InfluxSinkConfig.class, props);
        sink = new InfluxSink(instrumentation, "influx", config, new ProtoParser(stencilClient, config.getInputSchemaProtoClass()), client, stencilClient);
        ArgumentCaptor<BatchPoints> batchPointsArgumentCaptor = ArgumentCaptor.forClass(BatchPoints.class);

        sink.pushMessage(messages);
        verify(instrumentation, times(1)).capturePreExecutionLatencies(messages);
        verify(instrumentation, times(1)).startExecution();
        verify(instrumentation, times(1)).logDebug("Preparing {} messages", messages.size());
        verify(client, times(1)).write(batchPointsArgumentCaptor.capture());
        List<BatchPoints> batchPointsList = batchPointsArgumentCaptor.getAllValues();

        assertEquals(expectedPoint.lineProtocol(), batchPointsList.get(0).getPoints().get(0).lineProtocol());
    }

    @Test
    public void shouldCloseStencilClient() throws IOException {
        config = ConfigFactory.create(InfluxSinkConfig.class, props);

        sink = new InfluxSink(instrumentation, "influx", config, new ProtoParser(mockStencilClient, config.getInputSchemaProtoClass()), client, mockStencilClient);
        sink.close();

        verify(mockStencilClient, times(1)).close();
    }

    @Test
    public void shouldLogWhenClosingConnection() throws IOException {
        config = ConfigFactory.create(InfluxSinkConfig.class, props);

        sink = new InfluxSink(instrumentation, "influx", config, new ProtoParser(mockStencilClient, config.getInputSchemaProtoClass()), client, mockStencilClient);
        sink.close();

        verify(instrumentation, times(1)).logInfo("InfluxDB connection closing");
    }

    @Test
    public void shouldLogDataPointAndBatchPoints() throws IOException, DeserializerException {
        setupFieldNameIndexMappingProperties();
        setupTagNameIndexMappingProperties();
        config = ConfigFactory.create(InfluxSinkConfig.class, props);

        sink = new InfluxSink(instrumentation, "influx", config, new ProtoParser(stencilClient, config.getInputSchemaProtoClass()), client, stencilClient);
        ArgumentCaptor<BatchPoints> batchPointsArgumentCaptor = ArgumentCaptor.forClass(BatchPoints.class);

        sink.pushMessage(messages);
        verify(client, times(1)).write(batchPointsArgumentCaptor.capture());
        List<BatchPoints> batchPointsList = batchPointsArgumentCaptor.getAllValues();

        verify(instrumentation, times(1)).logDebug("Preparing {} messages", messages.size());
        verify(instrumentation, times(1)).logDebug("Data point: {}", batchPointsList.get(0).getPoints().get(0).toString());
        verify(instrumentation, times(1)).logDebug("Batch points: {}", batchPointsList.get(0).toString());
    }

    private void setupFieldNameIndexMappingProperties() {
        props.setProperty("SINK_INFLUX_FIELD_NAME_PROTO_INDEX_MAPPING", String.format("{\"%d\":\"feedbackOrderNumber\", \"%d\":\"comment\" , \"%d\":\"tipAmount\"}", feedbackOrderNumberIndex, feedbackCommentIndex, tipAmountIndex));
    }

    private void setupTagNameIndexMappingProperties() {
        props.setProperty("SINK_INFLUX_TAG_NAME_PROTO_INDEX_MAPPING", String.format("{\"%d\":\"driver_id\"}", driverIdIndex));
    }

    public class InfluxSinkStub extends InfluxSink {
        public InfluxSinkStub(Instrumentation instrumentation, String sinkType, InfluxSinkConfig config, ProtoParser protoParser, InfluxDB client, StencilClient stencilClient) {
            super(instrumentation, sinkType, config, protoParser, client, stencilClient);
        }

        public void prepare(List<Message> messageList) throws IOException {
            super.prepare(messageList);
        }
    }

}
