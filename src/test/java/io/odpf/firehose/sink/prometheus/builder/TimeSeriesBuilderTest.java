package io.odpf.firehose.sink.prometheus.builder;

import io.odpf.firehose.config.PromSinkConfig;
import io.odpf.firehose.consumer.TestBookingLogMessage;
import io.odpf.firehose.consumer.TestDurationMessage;
import io.odpf.firehose.consumer.TestFeedbackLogMessage;
import io.odpf.firehose.consumer.TestLocation;
import io.odpf.firehose.exception.EglcConfigurationException;
import com.google.protobuf.Duration;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Timestamp;
import cortexpb.Cortex;
import org.aeonbits.owner.ConfigFactory;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.List;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class TimeSeriesBuilderTest {

    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    @Test
    public void testSingleMetricWithMultipleLables() throws InvalidProtocolBufferException {
        Properties promConfigProps = new Properties();

        promConfigProps.setProperty("SINK_PROM_PROTO_EVENT_TIMESTAMP_INDEX", "2");
        promConfigProps.setProperty("INPUT_SCHEMA_PROTO_CLASS", TestFeedbackLogMessage.class.getName());
        promConfigProps.setProperty("SINK_PROM_WITH_EVENT_TIMESTAMP", "true");
        promConfigProps.setProperty("SINK_PROM_METRIC_NAME_PROTO_INDEX_MAPPING", "{\"7\": \"tip_amount\"}");
        promConfigProps.setProperty("SINK_PROM_LABEL_NAME_PROTO_INDEX_MAPPING", "{ \"4\": \"customer_id\", \"13\": \"support_ticket_created\" }");

        TestFeedbackLogMessage feedbackLogMessage = TestFeedbackLogMessage.newBuilder()
                .setCustomerId("CUSTOMER")
                .setTipAmount(10000)
                .setSupportTicketCreated(true)
                .setEventTimestamp(Timestamp.newBuilder().setSeconds(1000000)).build();

        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(TestFeedbackLogMessage.getDescriptor(), feedbackLogMessage.toByteArray());

        PromSinkConfig promSinkConfig = ConfigFactory.create(PromSinkConfig.class, promConfigProps);

        List<Cortex.TimeSeries> timeSeries = new TimeSeriesBuilder(promSinkConfig)
                .buildTimeSeries(dynamicMessage, 2);

        String expectedResult = "[labels {\n  name: \"__name__\"\n  value: \"tip_amount\"\n}\nlabels {\n  name: \"kafka_partition\"\n  value: \"2\"\n}\nlabels {\n  name: \"support_ticket_created\"\n  value: \"true\"\n}\nlabels {\n  name: \"customer_id\"\n  value: \"CUSTOMER\"\n}\nsamples {\n  value: 10000.0\n  timestamp_ms: 1000000000\n}\n]";

        assertEquals(expectedResult, timeSeries.toString());
    }

    @Test
    public void testMultipleMetricWithMultipleLabels() throws InvalidProtocolBufferException {
        Properties promConfigProps = new Properties();
        promConfigProps.setProperty("SINK_PROM_PROTO_EVENT_TIMESTAMP_INDEX", "2");
        promConfigProps.setProperty("INPUT_SCHEMA_PROTO_CLASS", TestFeedbackLogMessage.class.getName());
        promConfigProps.setProperty("SINK_PROM_WITH_EVENT_TIMESTAMP", "true");
        promConfigProps.setProperty("SINK_PROM_METRIC_NAME_PROTO_INDEX_MAPPING", " {\"7\": \"tip_amount\", \"5\": \"feedback_ratings\" }");
        promConfigProps.setProperty("SINK_PROM_LABEL_NAME_PROTO_INDEX_MAPPING", " {\"3\": \"driver_id\" , \"13\": \"support_ticket_created\"}");

        TestFeedbackLogMessage feedbackLogMessage = TestFeedbackLogMessage.newBuilder()
                .setDriverId("DRIVER")
                .setTipAmount(10000)
                .setFeedbackRating(5)
                .setSupportTicketCreated(true)
                .setEventTimestamp(Timestamp.newBuilder().setSeconds(1000000)).build();

        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(TestFeedbackLogMessage.getDescriptor(), feedbackLogMessage.toByteArray());

        PromSinkConfig promSinkConfig = ConfigFactory.create(PromSinkConfig.class, promConfigProps);

        List<Cortex.TimeSeries> timeSeries = new TimeSeriesBuilder(promSinkConfig)
                .buildTimeSeries(dynamicMessage, 2);

        String expectedResult = "[labels {\n  name: \"__name__\"\n  value: \"feedback_ratings\"\n}\nlabels {\n  name: \"driver_id\"\n  value: \"DRIVER\"\n}\nlabels {\n  name: \"kafka_partition\"\n  value: \"2\"\n}\nlabels {\n  name: \"support_ticket_created\"\n  value: \"true\"\n}\nsamples {\n  value: 5.0\n  timestamp_ms: 1000000000\n}\n, labels {\n  name: \"__name__\"\n  value: \"tip_amount\"\n}\nlabels {\n  name: \"driver_id\"\n  value: \"DRIVER\"\n}\nlabels {\n  name: \"kafka_partition\"\n  value: \"2\"\n}\nlabels {\n  name: \"support_ticket_created\"\n  value: \"true\"\n}\nsamples {\n  value: 10000.0\n  timestamp_ms: 1000000000\n}\n]";
        assertEquals(expectedResult, timeSeries.toString());
    }

    @Test
    public void testMessageWithTimestampIsBuiltIntoMillis() throws InvalidProtocolBufferException {
        Properties promConfigProps = new Properties();
        promConfigProps.setProperty("SINK_PROM_PROTO_EVENT_TIMESTAMP_INDEX", "2");
        promConfigProps.setProperty("INPUT_SCHEMA_PROTO_CLASS", TestFeedbackLogMessage.class.getName());
        promConfigProps.setProperty("SINK_PROM_WITH_EVENT_TIMESTAMP", "true");
        promConfigProps.setProperty("SINK_PROM_METRIC_NAME_PROTO_INDEX_MAPPING", "{ \"7\": \"tip_amount\" }");
        promConfigProps.setProperty("SINK_PROM_LABEL_NAME_PROTO_INDEX_MAPPING", "{ \"2\": \"event_timestamp\", \"3\": \"driver_id\" }");

        TestFeedbackLogMessage feedbackLogMessage = TestFeedbackLogMessage.newBuilder()
                .setDriverId("DRIVER")
                .setTipAmount(10000)
                .setEventTimestamp(Timestamp.newBuilder().setSeconds(1000000)).build();

        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(TestFeedbackLogMessage.getDescriptor(), feedbackLogMessage.toByteArray());

        PromSinkConfig promSinkConfig = ConfigFactory.create(PromSinkConfig.class, promConfigProps);

        List<Cortex.TimeSeries> timeSeries = new TimeSeriesBuilder(promSinkConfig)
                .buildTimeSeries(dynamicMessage, 2);

        String expectedResult = "[labels {\n  name: \"__name__\"\n  value: \"tip_amount\"\n}\nlabels {\n  name: \"driver_id\"\n  value: \"DRIVER\"\n}\nlabels {\n  name: \"kafka_partition\"\n  value: \"2\"\n}\nlabels {\n  name: \"event_timestamp\"\n  value: \"1000000000\"\n}\nsamples {\n  value: 10000.0\n  timestamp_ms: 1000000000\n}\n]";

        assertEquals(expectedResult, timeSeries.toString());

    }

    @Test
    public void testMessageWithDurationIsBuiltIntoMillis() throws InvalidProtocolBufferException {
        Properties promConfigProps = new Properties();
        promConfigProps.setProperty("SINK_PROM_PROTO_EVENT_TIMESTAMP_INDEX", "4");
        promConfigProps.setProperty("INPUT_SCHEMA_PROTO_CLASS", TestFeedbackLogMessage.class.getName());
        promConfigProps.setProperty("SINK_PROM_WITH_EVENT_TIMESTAMP", "true");
        promConfigProps.setProperty("SINK_PROM_METRIC_NAME_PROTO_INDEX_MAPPING", "{ \"1\": \"order_number\" }");

        TestDurationMessage testDurationMessage = TestDurationMessage.newBuilder()
                .setOrderNumber("100")
                .setDuration(Duration.newBuilder().setSeconds(1000))
                .build();
        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(TestDurationMessage.getDescriptor(), testDurationMessage.toByteArray());

        PromSinkConfig promSinkConfig = ConfigFactory.create(PromSinkConfig.class, promConfigProps);

        List<Cortex.TimeSeries> timeSeries = new TimeSeriesBuilder(promSinkConfig)
                .buildTimeSeries(dynamicMessage, 2);

        String expectedResult = "[labels {\n  name: \"__name__\"\n  value: \"order_number\"\n}\nlabels {\n  name: \"kafka_partition\"\n  value: \"2\"\n}\nsamples {\n  value: 100.0\n  timestamp_ms: 1000000\n}\n]";

        assertEquals(expectedResult, timeSeries.toString());
    }

    @Test
    public void testMessageWithEnum() throws InvalidProtocolBufferException {
        Properties promConfigProps = new Properties();
        promConfigProps.setProperty("SINK_PROM_PROTO_EVENT_TIMESTAMP_INDEX", "2");
        promConfigProps.setProperty("INPUT_SCHEMA_PROTO_CLASS", TestFeedbackLogMessage.class.getName());
        promConfigProps.setProperty("SINK_PROM_WITH_EVENT_TIMESTAMP", "true");
        promConfigProps.setProperty("SINK_PROM_METRIC_NAME_PROTO_INDEX_MAPPING", "{ \"7\": \"tip_amount\" }");
        promConfigProps.setProperty("SINK_PROM_LABEL_NAME_PROTO_INDEX_MAPPING", "{ \"10\": \"feedback_source\" }");

        TestFeedbackLogMessage feedbackLogMessage = TestFeedbackLogMessage.newBuilder()
                .setTipAmount(10000)
                .setFeedbackSource(io.odpf.firehose.consumer.TestFeedbackSource.Enum.DRIVER)
                .setEventTimestamp(Timestamp.newBuilder().setSeconds(1000000))
                .build();
        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(TestFeedbackLogMessage.getDescriptor(), feedbackLogMessage.toByteArray());

        PromSinkConfig promSinkConfig = ConfigFactory.create(PromSinkConfig.class, promConfigProps);

        List<Cortex.TimeSeries> timeSeries = new TimeSeriesBuilder(promSinkConfig)
                .buildTimeSeries(dynamicMessage, 2);

        String expectedResult = "[labels {\n  name: \"__name__\"\n  value: \"tip_amount\"\n}\nlabels {\n  name: \"kafka_partition\"\n  value: \"2\"\n}\nlabels {\n  name: \"feedback_source\"\n  value: \"DRIVER\"\n}\nsamples {\n  value: 10000.0\n  timestamp_ms: 1000000000\n}\n]";

        assertEquals(expectedResult, timeSeries.toString());
    }

    @Test
    public void testMessageWithNestedSchemaForMetrics() throws InvalidProtocolBufferException {
        Properties promConfigProps = new Properties();
        promConfigProps.setProperty("SINK_PROM_PROTO_EVENT_TIMESTAMP_INDEX", "2");
        promConfigProps.setProperty("INPUT_SCHEMA_PROTO_CLASS", TestFeedbackLogMessage.class.getName());
        promConfigProps.setProperty("SINK_PROM_WITH_EVENT_TIMESTAMP", "true");
        promConfigProps.setProperty("SINK_PROM_METRIC_NAME_PROTO_INDEX_MAPPING", "{ \"15\": { \"1\": \"order_completion_time_seconds\" }, \"7\": \"tip_amount\"}");
        promConfigProps.setProperty("SINK_PROM_LABEL_NAME_PROTO_INDEX_MAPPING", "{ \"3\": \"driver_id\" }");

        TestFeedbackLogMessage feedbackLogMessage = TestFeedbackLogMessage.newBuilder()
                .setTipAmount(100)
                .setDriverId("DRIVER")
                .setEventTimestamp(Timestamp.newBuilder().setSeconds(1000000))
                .setOrderCompletionTime(Timestamp.newBuilder().setSeconds(12345))
                .build();

        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(TestFeedbackLogMessage.getDescriptor(), feedbackLogMessage.toByteArray());

        PromSinkConfig promSinkConfig = ConfigFactory.create(PromSinkConfig.class, promConfigProps);

        List<Cortex.TimeSeries> timeSeries = new TimeSeriesBuilder(promSinkConfig)
                .buildTimeSeries(dynamicMessage, 2);

        String expectedResult = "[labels {\n  name: \"__name__\"\n  value: \"order_completion_time_seconds\"\n}\nlabels {\n  name: \"driver_id\"\n  value: \"DRIVER\"\n}\nlabels {\n  name: \"kafka_partition\"\n  value: \"2\"\n}\nsamples {\n  value: 12345.0\n  timestamp_ms: 1000000000\n}\n, labels {\n  name: \"__name__\"\n  value: \"tip_amount\"\n}\nlabels {\n  name: \"driver_id\"\n  value: \"DRIVER\"\n}\nlabels {\n  name: \"kafka_partition\"\n  value: \"2\"\n}\nsamples {\n  value: 100.0\n  timestamp_ms: 1000000000\n}\n]";
        assertEquals(expectedResult, timeSeries.toString());
    }

    @Test
    public void testMessageWithNestedSchemaForLabels() throws InvalidProtocolBufferException {
        Properties promConfigProps = new Properties();
        promConfigProps.setProperty("SINK_PROM_PROTO_EVENT_TIMESTAMP_INDEX", "2");
        promConfigProps.setProperty("INPUT_SCHEMA_PROTO_CLASS", TestFeedbackLogMessage.class.getName());
        promConfigProps.setProperty("SINK_PROM_WITH_EVENT_TIMESTAMP", "true");
        promConfigProps.setProperty("SINK_PROM_METRIC_NAME_PROTO_INDEX_MAPPING", "{ \"7\": \"tip_amount\" }");
        promConfigProps.setProperty("SINK_PROM_LABEL_NAME_PROTO_INDEX_MAPPING", "{ \"15\": { \"1\": \"order_completion_time_seconds\" } }");

        TestFeedbackLogMessage feedbackLogMessage = TestFeedbackLogMessage.newBuilder()
                .setTipAmount(10000)
                .setEventTimestamp(Timestamp.newBuilder().setSeconds(1000000))
                .setOrderCompletionTime(Timestamp.newBuilder().setSeconds(12345))
                .build();

        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(TestFeedbackLogMessage.getDescriptor(), feedbackLogMessage.toByteArray());

        PromSinkConfig promSinkConfig = ConfigFactory.create(PromSinkConfig.class, promConfigProps);

        List<Cortex.TimeSeries> timeSeries = new TimeSeriesBuilder(promSinkConfig)
                .buildTimeSeries(dynamicMessage, 2);

        String expectedResult = "[labels {\n  name: \"__name__\"\n  value: \"tip_amount\"\n}\nlabels {\n  name: \"kafka_partition\"\n  value: \"2\"\n}\nlabels {\n  name: \"order_completion_time_seconds\"\n  value: \"12345\"\n}\nsamples {\n  value: 10000.0\n  timestamp_ms: 1000000000\n}\n]";

        assertEquals(expectedResult, timeSeries.toString());
    }

    @Test
    public void testMessageWithMetricTimestampUsingIngestionTimestamp() throws InvalidProtocolBufferException {
        Properties promConfigProps = new Properties();
        promConfigProps.setProperty("SINK_PROM_PROTO_EVENT_TIMESTAMP_INDEX", "2");
        promConfigProps.setProperty("INPUT_SCHEMA_PROTO_CLASS", TestFeedbackLogMessage.class.getName());
        promConfigProps.setProperty("SINK_PROM_WITH_EVENT_TIMESTAMP", "false");
        promConfigProps.setProperty("SINK_PROM_METRIC_NAME_PROTO_INDEX_MAPPING", "{ \"1\": \"order_number\" }");
        promConfigProps.setProperty("SINK_PROM_LABEL_NAME_PROTO_INDEX_MAPPING", "{ \"3\": \"driver_id\" }");

        TestFeedbackLogMessage testFeedbackLogMessage = TestFeedbackLogMessage.newBuilder()
                .setOrderNumber("100")
                .setDriverId("DRIVER")
                .setEventTimestamp(Timestamp.newBuilder().setSeconds(1000))
                .build();
        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(TestFeedbackLogMessage.getDescriptor(), testFeedbackLogMessage.toByteArray());

        PromSinkConfig promSinkConfig = ConfigFactory.create(PromSinkConfig.class, promConfigProps);

        List<Cortex.TimeSeries> timeSeries = new TimeSeriesBuilder(promSinkConfig)
                .buildTimeSeries(dynamicMessage, 2);

        String eventTimestampResult = "[labels {\n  name: \"__name__\"\n  value: \"order_number\"\n}\n\nlabels {\n  name: \"driver_id\"\n  value: \"DRIVER\"\n}labels {\n  name: \"kafka_partition\"\n  value: \"2\"\n}\nsamples {\n  value: 100.0\n  timestamp_ms: 1000000\n}\n]";

        assertNotEquals(eventTimestampResult, timeSeries.toString());
    }

    @Test
    public void testMessageWithEmptyMetricProtoMappingConfig() throws InvalidProtocolBufferException {
        expectedEx.expect(EglcConfigurationException.class);
        expectedEx.expectMessage("field index mapping cannot be empty; at least one field value is required");

        Properties promConfigProps = new Properties();
        promConfigProps.setProperty("SINK_PROM_PROTO_EVENT_TIMESTAMP_INDEX", "2");
        promConfigProps.setProperty("INPUT_SCHEMA_PROTO_CLASS", TestFeedbackLogMessage.class.getName());
        promConfigProps.setProperty("SINK_PROM_WITH_EVENT_TIMESTAMP", "false");
        promConfigProps.setProperty("SINK_PROM_LABEL_NAME_PROTO_INDEX_MAPPING", "{ \"3\": \"driver_id\" }");

        TestFeedbackLogMessage testFeedbackLogMessage = TestFeedbackLogMessage.newBuilder()
                .setOrderNumber("100")
                .setDriverId("DRIVER")
                .setEventTimestamp(Timestamp.newBuilder().setSeconds(1000))
                .build();
        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(TestFeedbackLogMessage.getDescriptor(), testFeedbackLogMessage.toByteArray());

        PromSinkConfig promSinkConfig = ConfigFactory.create(PromSinkConfig.class, promConfigProps);

        TimeSeriesBuilder timeSeries = new TimeSeriesBuilder(promSinkConfig);
        timeSeries.buildTimeSeries(dynamicMessage, 2);
    }

    @Test
    public void testMessageWithEmptyLabelProtoMappingConfig() throws InvalidProtocolBufferException {
        Properties promConfigProps = new Properties();
        promConfigProps.setProperty("SINK_PROM_PROTO_EVENT_TIMESTAMP_INDEX", "2");
        promConfigProps.setProperty("INPUT_SCHEMA_PROTO_CLASS", TestFeedbackLogMessage.class.getName());
        promConfigProps.setProperty("SINK_PROM_WITH_EVENT_TIMESTAMP", "true");
        promConfigProps.setProperty("SINK_PROM_METRIC_NAME_PROTO_INDEX_MAPPING", "{ \"7\": \"tip_amount\" }");

        TestFeedbackLogMessage testFeedbackLogMessage = TestFeedbackLogMessage.newBuilder()
                .setTipAmount(12345)
                .setEventTimestamp(Timestamp.newBuilder().setSeconds(1000))
                .build();
        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(TestFeedbackLogMessage.getDescriptor(), testFeedbackLogMessage.toByteArray());

        PromSinkConfig promSinkConfig = ConfigFactory.create(PromSinkConfig.class, promConfigProps);

        List<Cortex.TimeSeries> timeSeries = new TimeSeriesBuilder(promSinkConfig)
                .buildTimeSeries(dynamicMessage, 2);

        String expectedResult = "[labels {\n  name: \"__name__\"\n  value: \"tip_amount\"\n}\nlabels {\n  name: \"kafka_partition\"\n  value: \"2\"\n}\nsamples {\n  value: 12345.0\n  timestamp_ms: 1000000\n}\n]";

        assertEquals(expectedResult, timeSeries.toString());

    }

    @Test
    public void testMessageWithEmptyFieldIndex() throws InvalidProtocolBufferException {
        expectedEx.expect(EglcConfigurationException.class);
        expectedEx.expectMessage("field index mapping cannot be empty; at least one field value is required");

        Properties promConfigProps = new Properties();
        promConfigProps.setProperty("SINK_PROM_PROTO_EVENT_TIMESTAMP_INDEX", "2");
        promConfigProps.setProperty("INPUT_SCHEMA_PROTO_CLASS", TestFeedbackLogMessage.class.getName());
        promConfigProps.setProperty("SINK_PROM_WITH_EVENT_TIMESTAMP", "false");
        promConfigProps.setProperty("SINK_PROM_METRIC_NAME_PROTO_INDEX_MAPPING", "{}");

        TestFeedbackLogMessage testFeedbackLogMessage = TestFeedbackLogMessage.newBuilder()
                .setDriverId("DRIVER")
                .setEventTimestamp(Timestamp.newBuilder().setSeconds(1000))
                .build();
        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(TestFeedbackLogMessage.getDescriptor(), testFeedbackLogMessage.toByteArray());

        PromSinkConfig promSinkConfig = ConfigFactory.create(PromSinkConfig.class, promConfigProps);

        TimeSeriesBuilder timeSeries = new TimeSeriesBuilder(promSinkConfig);
        timeSeries.buildTimeSeries(dynamicMessage, 2);
    }

    @Test
    public void testMessageWithNestedMetricsWithNestedLabels() throws InvalidProtocolBufferException {
        Properties promConfigProps = new Properties();
        promConfigProps.setProperty("SINK_PROM_PROTO_EVENT_TIMESTAMP_INDEX", "5");
        promConfigProps.setProperty("INPUT_SCHEMA_PROTO_CLASS", TestFeedbackLogMessage.class.getName());
        promConfigProps.setProperty("SINK_PROM_WITH_EVENT_TIMESTAMP", "true");
        promConfigProps.setProperty("SINK_PROM_METRIC_NAME_PROTO_INDEX_MAPPING", "{ \"41\": { \"1\": \"booking_creation_time_seconds\" }, \"16\": \"amount_paid_by_cash\"}");
        promConfigProps.setProperty("SINK_PROM_LABEL_NAME_PROTO_INDEX_MAPPING", "{ \"29\" : \"customer_name\",\"26\": { \"1\": \"driver_pickup_location_name\", \"5\":\"driver_pickup_location_type\" } }");


        TestBookingLogMessage message = TestBookingLogMessage.newBuilder()
                .setDriverPickupLocation(TestLocation.newBuilder().setName("local1").setType("type1"))
                .setAmountPaidByCash(10)
                .setEventTimestamp(Timestamp.newBuilder().setSeconds(12345))
                .setBookingCreationTime(Timestamp.newBuilder().setSeconds(54321))
                .setCustomerName("testing")
                .build();

        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(TestBookingLogMessage.getDescriptor(), message.toByteArray());

        PromSinkConfig promSinkConfig = ConfigFactory.create(PromSinkConfig.class, promConfigProps);

        List<Cortex.TimeSeries> timeSeries = new TimeSeriesBuilder(promSinkConfig)
                .buildTimeSeries(dynamicMessage, 2);

        String expectedResult = "[labels {\n  name: \"__name__\"\n  value: \"amount_paid_by_cash\"\n}\nlabels {\n  name: \"driver_pickup_location_name\"\n  value: \"local1\"\n}\nlabels {\n  name: \"customer_name\"\n  value: \"testing\"\n}\nlabels {\n  name: \"kafka_partition\"\n  value: \"2\"\n}\nlabels {\n  name: \"driver_pickup_location_type\"\n  value: \"type1\"\n}\nsamples {\n  value: 10.0\n  timestamp_ms: 12345000\n}\n, labels {\n  name: \"__name__\"\n  value: \"booking_creation_time_seconds\"\n}\nlabels {\n  name: \"driver_pickup_location_name\"\n  value: \"local1\"\n}\nlabels {\n  name: \"customer_name\"\n  value: \"testing\"\n}\nlabels {\n  name: \"kafka_partition\"\n  value: \"2\"\n}\nlabels {\n  name: \"driver_pickup_location_type\"\n  value: \"type1\"\n}\nsamples {\n  value: 54321.0\n  timestamp_ms: 12345000\n}\n]";

        assertEquals(expectedResult, timeSeries.toString());
    }
}
