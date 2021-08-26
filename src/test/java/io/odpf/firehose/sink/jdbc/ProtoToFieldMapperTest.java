package io.odpf.firehose.sink.jdbc;




import io.odpf.firehose.consumer.TestMapMessage;
import io.odpf.firehose.consumer.TestMessage;
import io.odpf.firehose.consumer.TestFeedbackLogMessage;
import io.odpf.firehose.consumer.TestBookingLogMessage;
import io.odpf.firehose.consumer.TestNestedMessage;
import io.odpf.firehose.consumer.TestNestedRepeatedMessage;
import io.odpf.firehose.proto.ProtoToFieldMapper;
import com.google.protobuf.Timestamp;
import io.odpf.stencil.StencilClientFactory;
import io.odpf.stencil.client.StencilClient;
import io.odpf.stencil.parser.ProtoParser;
import net.minidev.json.JSONObject;
import org.gradle.internal.impldep.org.testng.Assert;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;


public class ProtoToFieldMapperTest {

    private TestFeedbackLogMessage message;
    private Properties protoToDbMapping;
    private ProtoParser protoParser;

    private TestMessage testMessage;
    private TestNestedRepeatedMessage nestedMessage;
    private Properties nestedProtoToDbMapping;
    private ProtoParser nestedProtoParser;
    private StencilClient stencilClient;
    private Timestamp defaultTimestamp;
    private Instant now;


    @Before
    public void setUp() throws Exception {
        TestFeedbackLogMessage.Builder builder = TestFeedbackLogMessage.newBuilder();
        now = Instant.now();
        defaultTimestamp = Timestamp.newBuilder().setSeconds(now.getEpochSecond()).setNanos(now.getNano()).build();

        builder.setCustomerId("customer1");
        builder.setDriverId("driver1");
        builder.setFeedbackComment("comment");
        builder.setFeedbackRating(4);
        builder.setOrderNumber("12345");
        builder.setEventTimestamp(defaultTimestamp);
        message = builder.build();

        protoToDbMapping = new Properties();
        protoToDbMapping.put("1", "order_number");
        protoToDbMapping.put("2", "event_timestamp");
        protoToDbMapping.put("3", "driver_id");
        protoToDbMapping.put("4", "customer_id");
        protoToDbMapping.put("5", "feedback_rating");
        protoToDbMapping.put("6", "feedback_comment");
        stencilClient = StencilClientFactory.getClient();

        protoParser = new ProtoParser(stencilClient, TestFeedbackLogMessage.class.getName());


        TestMessage.Builder testBuilder = TestMessage.newBuilder();
        testBuilder.setOrderNumber("order_number");
        testBuilder.setOrderUrl("order_url");
        testBuilder.setOrderDetails("order_details");
        testMessage = testBuilder.build();

        TestNestedRepeatedMessage.Builder nestedBuilder = TestNestedRepeatedMessage.newBuilder();
        nestedBuilder.setSingleMessage(testMessage);
        nestedBuilder.addRepeatedMessage(testMessage);
        nestedBuilder.addRepeatedMessage(testMessage);
        nestedBuilder.setNumberField(42);
        nestedBuilder.addRepeatedNumberField(7);
        nestedBuilder.addRepeatedNumberField(14);
        nestedBuilder.addRepeatedNumberField(21);
        nestedMessage = nestedBuilder.build();

        nestedProtoToDbMapping = new Properties();
        nestedProtoToDbMapping.put("1", "single_message");
        nestedProtoToDbMapping.put("2", "repeated_message");
        nestedProtoToDbMapping.put("3", "number_field");
        nestedProtoToDbMapping.put("4", "repeated_number_field");

        nestedProtoParser = new ProtoParser(stencilClient, TestNestedRepeatedMessage.class.getName());
    }

    @After
    public void tearDown() throws Exception {
        message = null;
        nestedMessage = null;
    }

    @Test
    public void getFieldAtIndex() throws Exception {
        Properties properties = new Properties();
        properties.put("1", "order_number");
        ProtoToFieldMapper protoToFieldMapper = new ProtoToFieldMapper(protoParser, properties);
        Map<String, Object> fields = protoToFieldMapper.getFields(message.toByteArray());
        Assert.assertEquals(fields.get("order_number"), "12345");
    }

    @Test
    public void getField() throws Exception {

        ProtoToFieldMapper protoToFieldMapper = new ProtoToFieldMapper(protoParser, protoToDbMapping);
        Map<String, Object> fields = protoToFieldMapper.getFields(message.toByteArray());
        Assert.assertEquals(fields.get("order_number"), "12345");
        Assert.assertEquals(fields.get("event_timestamp"), now);
        Assert.assertEquals(fields.get("driver_id"), "driver1");
        Assert.assertEquals(fields.get("customer_id"), "customer1");
        Assert.assertEquals(fields.get("feedback_rating"), 4);
        Assert.assertEquals(fields.get("feedback_comment"), "comment");
    }

    @Test
    public void shouldContainNanoSecondsInTimestamp() throws IOException {

        TestFeedbackLogMessage.Builder builder = TestFeedbackLogMessage.newBuilder();
        Instant expectedTime = Instant.now();
        Timestamp timestamp = Timestamp.newBuilder().setSeconds(expectedTime.getEpochSecond()).setNanos(expectedTime.getNano()).build();
        builder.setEventTimestamp(timestamp);
        message = builder.build();

        ProtoToFieldMapper protoToFieldMapper = new ProtoToFieldMapper(protoParser, protoToDbMapping);
        Map<String, Object> fields = protoToFieldMapper.getFields(message.toByteArray());
        Object actualTimestamp = fields.get("event_timestamp");

        org.junit.Assert.assertEquals(expectedTime, actualTimestamp);
    }

    @Test
    public void nestedRepeatedMessageShouldBeJson() throws Exception {
        ProtoToFieldMapper protoToFieldMapper = new ProtoToFieldMapper(nestedProtoParser, nestedProtoToDbMapping);
        Map<String, Object> fields = protoToFieldMapper.getFields(nestedMessage.toByteArray());

        Assert.assertEquals(fields.get("single_message"), "{\"order_number\":\"order_number\",\"order_url\":\"order_url\",\"order_details\":\"order_details\"}");
        Assert.assertEquals(fields.get("repeated_message"), "[{\"order_number\":\"order_number\",\"order_url\":\"order_url\",\"order_details\":\"order_details\"},{\"order_number\":\"order_number\",\"order_url\":\"order_url\",\"order_details\":\"order_details\"}]");
        Assert.assertEquals(fields.get("repeated_number_field"), "[7,14,21]");
        Assert.assertEquals(fields.get("number_field"), 42);
    }

    @Test
    public void nestedRepeatedEmptyMessageShouldBeEmptyJson() throws Exception {
        TestNestedRepeatedMessage.Builder nestedBuilder = TestNestedRepeatedMessage.newBuilder();
        nestedBuilder.setSingleMessage(testMessage);
        nestedBuilder.setNumberField(42);
        nestedBuilder.addRepeatedNumberField(7);
        nestedBuilder.addRepeatedNumberField(14);
        nestedBuilder.addRepeatedNumberField(21);
        nestedMessage = nestedBuilder.build();

        ProtoToFieldMapper protoToFieldMapper = new ProtoToFieldMapper(nestedProtoParser, nestedProtoToDbMapping);
        Map<String, Object> fields = protoToFieldMapper.getFields(nestedMessage.toByteArray());

        Assert.assertEquals(fields.get("single_message"), "{\"order_number\":\"order_number\",\"order_url\":\"order_url\",\"order_details\":\"order_details\"}");
        Assert.assertEquals(fields.get("repeated_message"), "[]");
        Assert.assertEquals(fields.get("repeated_number_field"), "[7,14,21]");
        Assert.assertEquals(fields.get("number_field"), 42);
    }

    @Test
    public void messageWithMapShouldBeConvertedToJson() throws Exception {
        ProtoParser parser = new ProtoParser(stencilClient, TestMapMessage.class.getName());
        Properties mapping = new Properties();
        mapping.put("2", "map_field");
        mapping.put("1", "order_number");
        HashMap<String, String> mapField = new HashMap<>();
        mapField.put("key1", "value1");
        mapField.put("key2", "value2");

        TestMapMessage testMapMessage = TestMapMessage.newBuilder()
                .setOrderNumber("1")
                .putAllCurrentState(mapField)
                .build();

        String expectedMapFieldJSON = JSONObject.toJSONString(mapField);

        ProtoToFieldMapper protoToFieldMapper = new ProtoToFieldMapper(parser, mapping);
        Map<String, Object> fields = protoToFieldMapper.getFields(testMapMessage.toByteArray());
        Assert.assertTrue(fields.get("order_number").equals("1"));
        Assert.assertTrue(fields.get("map_field").equals(expectedMapFieldJSON));

    }

    @Test
    public void nestedMessageWithNestedPropertiesShouldBeMapped() throws Exception {
        ProtoParser parser = new ProtoParser(stencilClient, TestNestedMessage.class.getName());
        Properties mapping = new Properties();
        Properties nestedProperties = new Properties();
        nestedProperties.put("1", "order_number");
        nestedProperties.put("2", "order_url");
        nestedProperties.put("3", "order_details");

        mapping.put("1", "nested_id");
        mapping.put("2", nestedProperties);

        TestMessage myTestMessage = TestMessage.newBuilder()
                .setOrderNumber("1")
                .setOrderUrl("url")
                .setOrderDetails("details").build();

        TestNestedMessage myNestedMessage = TestNestedMessage.newBuilder()
                .setNestedId("1")
                .setSingleMessage(myTestMessage).build();

        ProtoToFieldMapper protoToFieldMapper = new ProtoToFieldMapper(parser, mapping);
        Map<String, Object> fields = protoToFieldMapper.getFields(myNestedMessage.toByteArray());

        Assert.assertTrue(fields.get("order_number").equals("1"));
        Assert.assertTrue(fields.get("nested_id").equals("1"));
        Assert.assertTrue(fields.get("order_url").equals("url"));
        Assert.assertTrue(fields.get("order_details").equals("details"));

    }

    @Test(expected = RuntimeException.class)
    public void shouldThrowClassNotFoundExceptionOnInvalidClasName() {
        protoParser = new ProtoParser(stencilClient, "NonexistentClass");
        ProtoToFieldMapper protoToFieldMapper = new ProtoToFieldMapper(protoParser, protoToDbMapping);

        protoToFieldMapper.getFields(TestBookingLogMessage.newBuilder().setCustomerEmail("test.com").build().toByteArray());
    }
}
