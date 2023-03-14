package com.gotocompany.firehose.sink.prometheus.builder;

import com.gotocompany.firehose.config.PromSinkConfig;
import com.gotocompany.firehose.exception.ConfigurationException;
import com.gotocompany.firehose.consumer.TestBookingLogMessage;
import com.gotocompany.firehose.consumer.TestDurationMessage;
import com.gotocompany.firehose.consumer.TestFeedbackLogMessage;
import com.gotocompany.firehose.consumer.TestLocation;
import com.google.protobuf.Duration;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Timestamp;
import cortexpb.Cortex;
import org.aeonbits.owner.ConfigFactory;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.*;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

public class TimeSeriesBuilderTest {

    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    private void sortAndAssertTimeSeries(List<Cortex.TimeSeries> expectedTimeSeries, List<Cortex.TimeSeries> timeSeries) {
        timeSeries.sort((o1, o2) -> o1.getLabelsList().stream().filter(x -> x.getName().equals("__name__")).findFirst().get().getValue().compareTo(
                o2.getLabelsList().stream().filter(x -> x.getName().equals("__name__")).findFirst().get().getValue()));
        expectedTimeSeries.sort((o1, o2) -> o1.getLabelsList().stream().filter(x -> x.getName().equals("__name__")).findFirst().get().getValue().compareTo(
                o2.getLabelsList().stream().filter(x -> x.getName().equals("__name__")).findFirst().get().getValue()));
        timeSeries = timeSeries.stream().map(x -> {
            Cortex.TimeSeries.Builder builder = x.toBuilder();
            List<Cortex.LabelPair> labels = new ArrayList<>(builder.getLabelsList());
            labels.sort(Comparator.comparing(Cortex.LabelPairOrBuilder::getName));
            builder.clearLabels();
            builder.addAllLabels(labels);
            return builder.build();
        }).collect(Collectors.toList());
        expectedTimeSeries = expectedTimeSeries.stream().map(x -> {
            Cortex.TimeSeries.Builder builder = x.toBuilder();
            List<Cortex.LabelPair> labels = new ArrayList<>(builder.getLabelsList());
            labels.sort(Comparator.comparing(Cortex.LabelPairOrBuilder::getName));
            builder.clearLabels();
            builder.addAllLabels(labels);
            return builder.build();
        }).collect(Collectors.toList());

        assertEquals(expectedTimeSeries, timeSeries);
    }

    @Test
    public void testSingleMetricWithMultipleLabels() throws InvalidProtocolBufferException {
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

        List<Cortex.TimeSeries> timeSeries = new TimeSeriesBuilder(promSinkConfig).buildTimeSeries(dynamicMessage, 2);

        List<Cortex.TimeSeries> expectedTimeSeries = new ArrayList<>();
        expectedTimeSeries.add(Cortex.TimeSeries.newBuilder().
                addLabels(Cortex.LabelPair.newBuilder().
                        setName("support_ticket_created").
                        setValue("true")).
                addLabels(Cortex.LabelPair.newBuilder().
                        setName("kafka_partition").
                        setValue("2")).
                addLabels(Cortex.LabelPair.newBuilder().
                        setName("__name__").
                        setValue("tip_amount")).
                addLabels(Cortex.LabelPair.newBuilder().
                        setName("customer_id").
                        setValue("CUSTOMER")).
                addSamples(Cortex.Sample.newBuilder().
                        setValue(10000).
                        setTimestampMs(1000000000)).build());
        sortAndAssertTimeSeries(expectedTimeSeries, timeSeries);
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

        List<Cortex.TimeSeries> timeSeries = new TimeSeriesBuilder(promSinkConfig).buildTimeSeries(dynamicMessage, 2);

        List<Cortex.TimeSeries> expectedTimeSeries = new ArrayList<>();
        expectedTimeSeries.add(Cortex.TimeSeries.newBuilder().
                addLabels(Cortex.LabelPair.newBuilder().
                        setName("__name__").
                        setValue("tip_amount")).
                addLabels(Cortex.LabelPair.newBuilder().
                        setName("driver_id").
                        setValue("DRIVER")).
                addLabels(Cortex.LabelPair.newBuilder().
                        setName("support_ticket_created").
                        setValue("true")).
                addLabels(Cortex.LabelPair.newBuilder().
                        setName("kafka_partition").
                        setValue("2")).
                addSamples(Cortex.Sample.newBuilder().
                        setValue(10000).
                        setTimestampMs(1000000000)).build());
        expectedTimeSeries.add(Cortex.TimeSeries.newBuilder().
                addLabels(Cortex.LabelPair.newBuilder().
                        setName("__name__").
                        setValue("feedback_ratings")).
                addLabels(Cortex.LabelPair.newBuilder().
                        setName("support_ticket_created").
                        setValue("true")).
                addLabels(Cortex.LabelPair.newBuilder().
                        setName("kafka_partition").
                        setValue("2")).
                addLabels(Cortex.LabelPair.newBuilder().
                        setName("driver_id").
                        setValue("DRIVER")).
                addSamples(Cortex.Sample.newBuilder().
                        setValue(5).
                        setTimestampMs(1000000000)).build());

        sortAndAssertTimeSeries(expectedTimeSeries, timeSeries);
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

        List<Cortex.TimeSeries> timeSeries = new TimeSeriesBuilder(promSinkConfig).buildTimeSeries(dynamicMessage, 2);


        List<Cortex.TimeSeries> expectedTimeSeries = new ArrayList<>();
        expectedTimeSeries.add(Cortex.TimeSeries.newBuilder().
                addLabels(Cortex.LabelPair.newBuilder().
                        setName("__name__").
                        setValue("tip_amount")).
                addLabels(Cortex.LabelPair.newBuilder().
                        setName("driver_id").
                        setValue("DRIVER")).
                addLabels(Cortex.LabelPair.newBuilder().
                        setName("event_timestamp").
                        setValue("1000000000")).
                addLabels(Cortex.LabelPair.newBuilder().
                        setName("kafka_partition").
                        setValue("2")).
                addSamples(Cortex.Sample.newBuilder().
                        setValue(10000).
                        setTimestampMs(1000000000)).build());

        sortAndAssertTimeSeries(expectedTimeSeries, timeSeries);
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

        List<Cortex.TimeSeries> timeSeries = new ArrayList(new TimeSeriesBuilder(promSinkConfig)
                .buildTimeSeries(dynamicMessage, 2));

        List<Cortex.TimeSeries> expectedTimeSeries = new ArrayList<>();
        expectedTimeSeries.add(Cortex.TimeSeries.newBuilder().
                addLabels(Cortex.LabelPair.newBuilder().
                        setName("__name__").
                        setValue("order_number")).
                addLabels(Cortex.LabelPair.newBuilder().
                        setName("kafka_partition").
                        setValue("2")).
                addSamples(Cortex.Sample.newBuilder().
                        setValue(100).
                        setTimestampMs(1000000)).build());
        sortAndAssertTimeSeries(expectedTimeSeries, timeSeries);
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
                .setFeedbackSource(com.gotocompany.firehose.consumer.TestFeedbackSource.Enum.DRIVER)
                .setEventTimestamp(Timestamp.newBuilder().setSeconds(1000000))
                .build();
        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(TestFeedbackLogMessage.getDescriptor(), feedbackLogMessage.toByteArray());

        PromSinkConfig promSinkConfig = ConfigFactory.create(PromSinkConfig.class, promConfigProps);

        List<Cortex.TimeSeries> timeSeries = new TimeSeriesBuilder(promSinkConfig).buildTimeSeries(dynamicMessage, 2);

        List<Cortex.TimeSeries> expectedTimeSeries = new ArrayList<>();
        expectedTimeSeries.add(Cortex.TimeSeries.newBuilder().
                addLabels(Cortex.LabelPair.newBuilder().
                        setName("__name__").
                        setValue("tip_amount")).
                addLabels(Cortex.LabelPair.newBuilder().
                        setName("feedback_source").
                        setValue("DRIVER")).
                addLabels(Cortex.LabelPair.newBuilder().
                        setName("kafka_partition").
                        setValue("2")).
                addSamples(Cortex.Sample.newBuilder().
                        setValue(10000).
                        setTimestampMs(1000000000)).build());

        sortAndAssertTimeSeries(expectedTimeSeries, timeSeries);
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

        List<Cortex.TimeSeries> timeSeries = new TimeSeriesBuilder(promSinkConfig).buildTimeSeries(dynamicMessage, 2);

        List<Cortex.TimeSeries> expectedTimeSeries = new ArrayList<>();
        expectedTimeSeries.add(Cortex.TimeSeries.newBuilder().
                addLabels(Cortex.LabelPair.newBuilder().
                        setName("__name__").
                        setValue("tip_amount")).
                addLabels(Cortex.LabelPair.newBuilder().
                        setName("driver_id").
                        setValue("DRIVER")).
                addLabels(Cortex.LabelPair.newBuilder().
                        setName("kafka_partition").
                        setValue("2")).
                addSamples(Cortex.Sample.newBuilder().
                        setValue(100).
                        setTimestampMs(1000000000)).build());

        expectedTimeSeries.add(Cortex.TimeSeries.newBuilder().
                addLabels(Cortex.LabelPair.newBuilder().
                        setName("__name__").
                        setValue("order_completion_time_seconds")).
                addLabels(Cortex.LabelPair.newBuilder().
                        setName("driver_id").
                        setValue("DRIVER")).
                addLabels(Cortex.LabelPair.newBuilder().
                        setName("kafka_partition").
                        setValue("2")).
                addSamples(Cortex.Sample.newBuilder().
                        setValue(12345).
                        setTimestampMs(1000000000)).build());
        sortAndAssertTimeSeries(expectedTimeSeries, timeSeries);
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

        List<Cortex.TimeSeries> timeSeries = new TimeSeriesBuilder(promSinkConfig).buildTimeSeries(dynamicMessage, 2);
        List<Cortex.TimeSeries> expectedTimeSeries = new ArrayList<>();
        expectedTimeSeries.add(Cortex.TimeSeries.newBuilder().
                addLabels(Cortex.LabelPair.newBuilder().
                        setName("__name__").
                        setValue("tip_amount")).
                addLabels(Cortex.LabelPair.newBuilder().
                        setName("order_completion_time_seconds").
                        setValue("12345")).
                addLabels(Cortex.LabelPair.newBuilder().
                        setName("kafka_partition").
                        setValue("2")).
                addSamples(Cortex.Sample.newBuilder().
                        setValue(10000).
                        setTimestampMs(1000000000)).build());
        sortAndAssertTimeSeries(expectedTimeSeries, timeSeries);

    }

    @Test
    public void testMessageWithMetricTimestampUsingIngestionTimestamp() throws InvalidProtocolBufferException, InterruptedException {
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

        long beforeTime = System.currentTimeMillis();
        Thread.sleep(2);
        List<Cortex.TimeSeries> timeSeries = new TimeSeriesBuilder(promSinkConfig).buildTimeSeries(dynamicMessage, 2);
        long afterTime = System.currentTimeMillis();
        long seriesTime = timeSeries.get(0).getSamples(0).getTimestampMs();
        assertTrue(seriesTime > beforeTime && seriesTime <= afterTime);
    }

    @Test
    public void testMessageWithEmptyMetricProtoMappingConfig() throws InvalidProtocolBufferException {
        expectedEx.expect(ConfigurationException.class);
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

        List<Cortex.TimeSeries> timeSeries = new TimeSeriesBuilder(promSinkConfig).buildTimeSeries(dynamicMessage, 2);

        List<Cortex.TimeSeries> expectedTimeSeries = new ArrayList<>();
        expectedTimeSeries.add(Cortex.TimeSeries.newBuilder().
                addLabels(Cortex.LabelPair.newBuilder().
                        setName("__name__").
                        setValue("tip_amount")).
                addLabels(Cortex.LabelPair.newBuilder().
                        setName("kafka_partition").
                        setValue("2")).
                addSamples(Cortex.Sample.newBuilder().
                        setValue(12345).
                        setTimestampMs(1000000)).build());
        sortAndAssertTimeSeries(expectedTimeSeries, timeSeries);
    }

    @Test
    public void testMessageWithEmptyFieldIndex() throws InvalidProtocolBufferException {
        expectedEx.expect(ConfigurationException.class);
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

        List<Cortex.TimeSeries> timeSeries = new TimeSeriesBuilder(promSinkConfig).buildTimeSeries(dynamicMessage, 2);
        List<Cortex.TimeSeries> expectedTimeSeries = new ArrayList<>();
        expectedTimeSeries.add(Cortex.TimeSeries.newBuilder().
                addLabels(Cortex.LabelPair.newBuilder().
                        setName("__name__").
                        setValue("amount_paid_by_cash")).
                addLabels(Cortex.LabelPair.newBuilder().
                        setName("customer_name").
                        setValue("testing")).
                addLabels(Cortex.LabelPair.newBuilder().
                        setName("driver_pickup_location_name").
                        setValue("local1")).
                addLabels(Cortex.LabelPair.newBuilder().
                        setName("driver_pickup_location_type").
                        setValue("type1")).
                addLabels(Cortex.LabelPair.newBuilder().
                        setName("kafka_partition").
                        setValue("2")).
                addSamples(Cortex.Sample.newBuilder().
                        setValue(10).
                        setTimestampMs(12345000)).build());
        expectedTimeSeries.add(Cortex.TimeSeries.newBuilder().
                addLabels(Cortex.LabelPair.newBuilder().
                        setName("__name__").
                        setValue("booking_creation_time_seconds")).
                addLabels(Cortex.LabelPair.newBuilder().
                        setName("customer_name").
                        setValue("testing")).
                addLabels(Cortex.LabelPair.newBuilder().
                        setName("driver_pickup_location_name").
                        setValue("local1")).
                addLabels(Cortex.LabelPair.newBuilder().
                        setName("driver_pickup_location_type").
                        setValue("type1")).
                addLabels(Cortex.LabelPair.newBuilder().
                        setName("kafka_partition").
                        setValue("2")).
                addSamples(Cortex.Sample.newBuilder().
                        setValue(54321).
                        setTimestampMs(12345000)).build());
        sortAndAssertTimeSeries(expectedTimeSeries, timeSeries);
    }
}
