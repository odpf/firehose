package com.gotocompany.firehose.sink.influxdb.builder;

import com.gotocompany.firehose.config.InfluxSinkConfig;
import com.gotocompany.firehose.consumer.TestDurationMessage;
import com.gotocompany.firehose.consumer.TestFeedbackLogMessage;
import com.google.protobuf.Duration;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Timestamp;
import com.gotocompany.firehose.consumer.TestFeedbackSource;
import org.aeonbits.owner.ConfigFactory;
import org.influxdb.dto.Point;
import org.junit.Test;

import java.util.Properties;

public class PointBuilderTest {

    @Test
    public void testMessageWithTimestampIsBuiltIntoMillis() throws InvalidProtocolBufferException {
        Properties influxConfigProps = new Properties();
        influxConfigProps.setProperty("SINK_INFLUX_MEASUREMENT_NAME", "test_point_builder");
        influxConfigProps.setProperty("SINK_INFLUX_PROTO_EVENT_TIMESTAMP_INDEX", "2");
        influxConfigProps.setProperty("SINK_INFLUX_DB_NAME", "test");
        influxConfigProps.setProperty("INPUT_SCHEMA_PROTO_CLASS", TestFeedbackLogMessage.class.getName());
        influxConfigProps.setProperty("SINK_INFLUX_FIELD_NAME_PROTO_INDEX_MAPPING", "{ \"2\": \"event_timestamp\", \"7\": \"tip_amount\" }");
        influxConfigProps.setProperty("SINK_INFLUX_TAG_NAME_PROTO_INDEX_MAPPING", "{ \"4\": \"customer_id\", \"3\": \"driver_id\" }");

        TestFeedbackLogMessage feedbackLogMessage = TestFeedbackLogMessage.newBuilder()
                .setCustomerId("CUSTOMER")
                .setDriverId("DRIVER")
                .setTipAmount(10000)
                .setEventTimestamp(Timestamp.newBuilder().setSeconds(1000000)).build();

        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(TestFeedbackLogMessage.getDescriptor(), feedbackLogMessage.toByteArray());

        InfluxSinkConfig influxSinkConfig = ConfigFactory.create(InfluxSinkConfig.class, influxConfigProps);

        Point point = new PointBuilder(influxSinkConfig)
                .buildPoint(dynamicMessage);

        assert point.lineProtocol().equals("test_point_builder,customer_id=CUSTOMER,driver_id=DRIVER event_timestamp=1000000000i,tip_amount=10000.0 1000000000000000");
    }

    @Test
    public void testMessageWithEnum() throws InvalidProtocolBufferException {
        Properties influxConfigProps = new Properties();
        influxConfigProps.setProperty("SINK_INFLUX_MEASUREMENT_NAME", "test_point_builder");
        influxConfigProps.setProperty("SINK_INFLUX_PROTO_EVENT_TIMESTAMP_INDEX", "2");
        influxConfigProps.setProperty("SINK_INFLUX_DB_NAME", "test");
        influxConfigProps.setProperty("INPUT_SCHEMA_PROTO_CLASS", TestFeedbackLogMessage.class.getName());
        influxConfigProps.setProperty("SINK_INFLUX_FIELD_NAME_PROTO_INDEX_MAPPING", "{ \"10\": \"feedback_source\"}");
        influxConfigProps.setProperty("SINK_INFLUX_TAG_NAME_PROTO_INDEX_MAPPING", "{ \"1\": \"order_number\" }");

        TestFeedbackLogMessage feedbackLogMessage = TestFeedbackLogMessage.newBuilder()
                .setOrderNumber("ORDER")
                .setFeedbackSource(TestFeedbackSource.Enum.DRIVER)
                .build();
        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(TestFeedbackLogMessage.getDescriptor(), feedbackLogMessage.toByteArray());

        InfluxSinkConfig influxSinkConfig = ConfigFactory.create(InfluxSinkConfig.class, influxConfigProps);

        Point point = new PointBuilder(influxSinkConfig)
                .buildPoint(dynamicMessage);

        assert point.lineProtocol().equals("test_point_builder,order_number=ORDER feedback_source=\"DRIVER\" 0");

    }

    @Test
    public void testMessageWithDurationIsBuiltIntoMillis() throws InvalidProtocolBufferException {
        Properties influxConfigProps = new Properties();
        influxConfigProps.setProperty("SINK_INFLUX_MEASUREMENT_NAME", "test_point_builder");
        influxConfigProps.setProperty("SINK_INFLUX_PROTO_EVENT_TIMESTAMP_INDEX", "5");
        influxConfigProps.setProperty("SINK_INFLUX_DB_NAME", "test");
        influxConfigProps.setProperty("INPUT_SCHEMA_PROTO_CLASS", TestFeedbackLogMessage.class.getName());
        influxConfigProps.setProperty("SINK_INFLUX_FIELD_NAME_PROTO_INDEX_MAPPING", "{ \"4\": \"duration\" }");
        influxConfigProps.setProperty("SINK_INFLUX_TAG_NAME_PROTO_INDEX_MAPPING", "{ \"1\": \"order_number\" }");

        TestDurationMessage testDurationMessage = TestDurationMessage.newBuilder()
                .setOrderNumber("ORDER")
                .setDuration(Duration.newBuilder().setSeconds(1000))
                .setEventTimestamp(Timestamp.newBuilder().setSeconds(1000000))
                .build();
        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(TestDurationMessage.getDescriptor(), testDurationMessage.toByteArray());

        InfluxSinkConfig influxSinkConfig = ConfigFactory.create(InfluxSinkConfig.class, influxConfigProps);

        Point point = new PointBuilder(influxSinkConfig)
                .buildPoint(dynamicMessage);

        assert point.lineProtocol().equals("test_point_builder,order_number=ORDER duration=1000000i 1000000000000000");
    }

    @Test
    public void testMessageWithNestedSchemaForFields() throws InvalidProtocolBufferException {
        Properties influxConfigProps = new Properties();
        influxConfigProps.setProperty("SINK_INFLUX_MEASUREMENT_NAME", "test_point_builder");
        influxConfigProps.setProperty("SINK_INFLUX_PROTO_EVENT_TIMESTAMP_INDEX", "2");
        influxConfigProps.setProperty("SINK_INFLUX_DB_NAME", "test");
        influxConfigProps.setProperty("INPUT_SCHEMA_PROTO_CLASS", TestFeedbackLogMessage.class.getName());
        influxConfigProps.setProperty("SINK_INFLUX_FIELD_NAME_PROTO_INDEX_MAPPING", "{ \"2\": \"event_timestamp\", \"7\": \"tip_amount\", \"15\": { \"1\": \"order_completion_time_seconds\" } }");
        influxConfigProps.setProperty("SINK_INFLUX_TAG_NAME_PROTO_INDEX_MAPPING", "{ \"4\": \"customer_id\", \"3\": \"driver_id\" }");

        TestFeedbackLogMessage feedbackLogMessage = TestFeedbackLogMessage.newBuilder()
                .setCustomerId("CUSTOMER")
                .setDriverId("DRIVER")
                .setTipAmount(10000)
                .setEventTimestamp(Timestamp.newBuilder().setSeconds(1000000))
                .setOrderCompletionTime(Timestamp.newBuilder().setSeconds(12345))
                .build();

        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(TestFeedbackLogMessage.getDescriptor(), feedbackLogMessage.toByteArray());

        InfluxSinkConfig influxSinkConfig = ConfigFactory.create(InfluxSinkConfig.class, influxConfigProps);

        Point point = new PointBuilder(influxSinkConfig)
                .buildPoint(dynamicMessage);

        assert point.lineProtocol().equals("test_point_builder,customer_id=CUSTOMER,driver_id=DRIVER event_timestamp=1000000000i,order_completion_time_seconds=12345i,tip_amount=10000.0 1000000000000000");
    }

    @Test
    public void testMessageWithNestedSchemaForTags() throws InvalidProtocolBufferException {
        Properties influxConfigProps = new Properties();
        influxConfigProps.setProperty("SINK_INFLUX_MEASUREMENT_NAME", "test_point_builder");
        influxConfigProps.setProperty("SINK_INFLUX_PROTO_EVENT_TIMESTAMP_INDEX", "2");
        influxConfigProps.setProperty("SINK_INFLUX_DB_NAME", "test");
        influxConfigProps.setProperty("INPUT_SCHEMA_PROTO_CLASS", TestFeedbackLogMessage.class.getName());
        influxConfigProps.setProperty("SINK_INFLUX_FIELD_NAME_PROTO_INDEX_MAPPING", "{ \"2\": \"event_timestamp\", \"7\": \"tip_amount\" }");
        influxConfigProps.setProperty("SINK_INFLUX_TAG_NAME_PROTO_INDEX_MAPPING", "{ \"4\": \"customer_id\", \"3\": \"driver_id\", \"15\": \"order_completion_time_seconds\" }");

        TestFeedbackLogMessage feedbackLogMessage = TestFeedbackLogMessage.newBuilder()
                .setCustomerId("CUSTOMER")
                .setDriverId("DRIVER")
                .setTipAmount(10000)
                .setEventTimestamp(Timestamp.newBuilder().setSeconds(1000000))
                .setOrderCompletionTime(Timestamp.newBuilder().setSeconds(12345))
                .build();

        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(TestFeedbackLogMessage.getDescriptor(), feedbackLogMessage.toByteArray());

        InfluxSinkConfig influxSinkConfig = ConfigFactory.create(InfluxSinkConfig.class, influxConfigProps);

        Point point = new PointBuilder(influxSinkConfig)
                .buildPoint(dynamicMessage);

        assert point.lineProtocol().equals("test_point_builder,customer_id=CUSTOMER,driver_id=DRIVER,order_completion_time_seconds=12345000 event_timestamp=1000000000i,tip_amount=10000.0 1000000000000000");
    }
}
