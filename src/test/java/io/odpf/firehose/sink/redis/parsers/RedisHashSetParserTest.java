package io.odpf.firehose.sink.redis.parsers;



import io.odpf.firehose.config.RedisSinkConfig;
import io.odpf.firehose.config.enums.RedisSinkDataType;
import io.odpf.firehose.consumer.Message;
import io.odpf.firehose.consumer.TestKey;
import io.odpf.firehose.consumer.TestMessage;
import io.odpf.firehose.consumer.TestBookingLogMessage;
import io.odpf.firehose.consumer.TestNestedRepeatedMessage;
import io.odpf.firehose.metrics.StatsDReporter;
import io.odpf.firehose.proto.ProtoToFieldMapper;
import io.odpf.firehose.sink.redis.dataentry.RedisHashSetFieldEntry;
import io.odpf.stencil.client.ClassLoadStencilClient;
import io.odpf.stencil.parser.ProtoParser;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.IllegalFormatConversionException;
import java.util.Properties;
import java.util.UnknownFormatConversionException;

import static junit.framework.TestCase.assertEquals;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class RedisHashSetParserTest {

    private final long bookingCustomerTotalFare = 2000L;
    private final float bookingAmountPaidByCash = 12.3F;
    @Rule
    public ExpectedException expectedException = ExpectedException.none();
    @Mock
    private RedisSinkConfig redisSinkConfig;

    @Mock
    private StatsDReporter statsDReporter;

    private Message message;
    private ProtoParser testKeyProtoParser;
    private ProtoParser testMessageProtoParser;
    private ClassLoadStencilClient stencilClient;
    private Message bookingMessage;
    private ProtoParser bookingMessageProtoParser;
    private String bookingOrderNumber = "booking-order-1";

    @Before
    public void setUp() throws Exception {

        TestKey testKey = TestKey.newBuilder().setOrderNumber("ORDER-1-FROM-KEY").build();
        TestBookingLogMessage testBookingLogMessage = TestBookingLogMessage.newBuilder().setOrderNumber(bookingOrderNumber).setCustomerTotalFareWithoutSurge(bookingCustomerTotalFare).setAmountPaidByCash(bookingAmountPaidByCash).build();
        TestMessage testMessage = TestMessage.newBuilder().setOrderNumber("test-order").setOrderDetails("ORDER-DETAILS").build();
        this.message = new Message(testKey.toByteArray(), testMessage.toByteArray(), "test", 1, 11);
        this.bookingMessage = new Message(testKey.toByteArray(), testBookingLogMessage.toByteArray(), "test", 1, 11);
        stencilClient = new ClassLoadStencilClient();
        testMessageProtoParser = new ProtoParser(stencilClient, TestMessage.class.getCanonicalName());
        bookingMessageProtoParser = new ProtoParser(stencilClient, TestBookingLogMessage.class.getCanonicalName());
        testKeyProtoParser = new ProtoParser(stencilClient, TestKey.class.getCanonicalName());
    }

    private void setRedisSinkConfig(String parserMode, String collectionKeyTemplate, RedisSinkDataType redisSinkDataType) {
        when(redisSinkConfig.getKafkaRecordParserMode()).thenReturn(parserMode);
        when(redisSinkConfig.getSinkRedisKeyTemplate()).thenReturn(collectionKeyTemplate);
    }

    @Test
    public void shouldParseStringMessageForCollectionKeyTemplate() {
        setRedisSinkConfig("message", "Test-%s,1", RedisSinkDataType.HASHSET);
        ProtoToFieldMapper protoToFieldMapperForTestMessage = new ProtoToFieldMapper(testMessageProtoParser, getProperties("3", "details"));
        RedisParser redisMessageParser = new RedisHashSetParser(protoToFieldMapperForTestMessage, testMessageProtoParser, redisSinkConfig, statsDReporter);

        RedisHashSetFieldEntry redisHashSetFieldEntry = (RedisHashSetFieldEntry) redisMessageParser.parse(message).get(0);

        assertEquals("ORDER-DETAILS", redisHashSetFieldEntry.getValue());
        assertEquals("details", redisHashSetFieldEntry.getField());
        assertEquals("Test-test-order", redisHashSetFieldEntry.getKey());
    }

    @Test
    public void shouldParseStringMessageWithSpacesForCollectionKeyTemplate() {
        setRedisSinkConfig("message", "Test-%s, 1", RedisSinkDataType.HASHSET);
        ProtoToFieldMapper protoToFieldMapperForTestMessage = new ProtoToFieldMapper(testMessageProtoParser, getProperties("3", "details"));
        RedisParser redisMessageParser = new RedisHashSetParser(protoToFieldMapperForTestMessage, testMessageProtoParser, redisSinkConfig, statsDReporter);
        RedisHashSetFieldEntry redisHashSetFieldEntry = (RedisHashSetFieldEntry) redisMessageParser.parse(message).get(0);

        assertEquals("ORDER-DETAILS", redisHashSetFieldEntry.getValue());
        assertEquals("details", redisHashSetFieldEntry.getField());
        assertEquals("Test-test-order", redisHashSetFieldEntry.getKey());
    }


    @Test
    public void shouldParseFloatMessageForCollectionKeyTemplate() {
        setRedisSinkConfig("message", "Test-%.2f,16", RedisSinkDataType.HASHSET);
        ProtoToFieldMapper protoToFieldMapperForBookingMessage = new ProtoToFieldMapper(testMessageProtoParser, getProperties("2", "order_number_1"));
        RedisParser redisMessageParser = new RedisHashSetParser(protoToFieldMapperForBookingMessage, bookingMessageProtoParser, redisSinkConfig, statsDReporter);

        RedisHashSetFieldEntry redisHashSetFieldEntry = (RedisHashSetFieldEntry) redisMessageParser.parse(bookingMessage).get(0);

        assertEquals("Test-12.30", redisHashSetFieldEntry.getKey());
        assertEquals("order_number_1", redisHashSetFieldEntry.getField());
        assertEquals("booking-order-1", redisHashSetFieldEntry.getValue());
    }

    @Test
    public void shouldParseLongMessageForCollectionKeyTemplate() {
        setRedisSinkConfig("message", "Test-%d,52", RedisSinkDataType.HASHSET);
        ProtoToFieldMapper protoToFieldMapperForBookingMessage = new ProtoToFieldMapper(testMessageProtoParser, getProperties("2", "order_number_1"));
        RedisParser redisMessageParser = new RedisHashSetParser(protoToFieldMapperForBookingMessage, bookingMessageProtoParser, redisSinkConfig, statsDReporter);

        RedisHashSetFieldEntry redisHashSetFieldEntry = (RedisHashSetFieldEntry) redisMessageParser.parse(bookingMessage).get(0);

        assertEquals("Test-2000", redisHashSetFieldEntry.getKey());
        assertEquals("order_number_1", redisHashSetFieldEntry.getField());
        assertEquals("booking-order-1", redisHashSetFieldEntry.getValue());
    }

    @Test
    public void shouldThrowExceptionForInvalidPatternInCollectionKeyTemplate() {
        expectedException.expect(UnknownFormatConversionException.class);
        expectedException.expectMessage("Conversion = '%'");

        setRedisSinkConfig("message", "Test-%,52", RedisSinkDataType.HASHSET);
        ProtoToFieldMapper protoToFieldMapperForBookingMessage = new ProtoToFieldMapper(testMessageProtoParser, getProperties("2", "order_number_1"));
        RedisParser redisMessageParser = new RedisHashSetParser(protoToFieldMapperForBookingMessage, bookingMessageProtoParser, redisSinkConfig, statsDReporter);

        redisMessageParser.parse(bookingMessage);
    }

    @Test
    public void shouldThrowExceptionForIncompatiblePatternInCollectionKeyTemplate() {
        expectedException.expect(IllegalFormatConversionException.class);
        expectedException.expectMessage("f != java.lang.Long");

        setRedisSinkConfig("message", "Test-%f,52", RedisSinkDataType.HASHSET);
        ProtoToFieldMapper protoToFieldMapperForBookingMessage = new ProtoToFieldMapper(testMessageProtoParser, getProperties("2", "order_number_1"));
        RedisParser redisMessageParser = new RedisHashSetParser(protoToFieldMapperForBookingMessage, bookingMessageProtoParser, redisSinkConfig, statsDReporter);

        redisMessageParser.parse(bookingMessage);
    }

    @Test
    public void shouldThrowExceptionForNonExistingDescriptorInCollectionKeyTemplate() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Descriptor not found for index: 20000");

        setRedisSinkConfig("message", "Test-%f,20000", RedisSinkDataType.HASHSET);
        ProtoToFieldMapper protoToFieldMapperForBookingMessage = new ProtoToFieldMapper(testMessageProtoParser, getProperties("2", "order_number_1"));
        RedisParser redisMessageParser = new RedisHashSetParser(protoToFieldMapperForBookingMessage, bookingMessageProtoParser, redisSinkConfig, statsDReporter);

        redisMessageParser.parse(bookingMessage);
    }

    @Test
    public void shouldThrowExceptionForNullCollectionKeyTemplate() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Template 'null' is invalid");

        setRedisSinkConfig("message", null, RedisSinkDataType.HASHSET);
        ProtoToFieldMapper protoToFieldMapperForBookingMessage = new ProtoToFieldMapper(testMessageProtoParser, getProperties("2", "order_number_1"));
        RedisParser redisMessageParser = new RedisHashSetParser(protoToFieldMapperForBookingMessage, bookingMessageProtoParser, redisSinkConfig, statsDReporter);

        redisMessageParser.parse(bookingMessage);
    }

    @Test
    public void shouldThrowExceptionForEmptyCollectionKeyTemplate() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Template '' is invalid");

        setRedisSinkConfig("message", "", RedisSinkDataType.HASHSET);
        ProtoToFieldMapper protoToFieldMapperForBookingMessage = new ProtoToFieldMapper(testMessageProtoParser, getProperties("2", "order_number_1"));
        RedisParser redisMessageParser = new RedisHashSetParser(protoToFieldMapperForBookingMessage, bookingMessageProtoParser, redisSinkConfig, statsDReporter);

        redisMessageParser.parse(bookingMessage);
    }

    @Test
    public void shouldAcceptStringForCollectionKey() {
        setRedisSinkConfig("message", "Test", RedisSinkDataType.HASHSET);
        ProtoToFieldMapper protoToFieldMapperForBookingMessage = new ProtoToFieldMapper(testMessageProtoParser, getProperties("2", "order_number_1"));

        RedisParser redisMessageParser = new RedisHashSetParser(protoToFieldMapperForBookingMessage, bookingMessageProtoParser, redisSinkConfig, statsDReporter);

        RedisHashSetFieldEntry redisHashSetFieldEntry = (RedisHashSetFieldEntry) redisMessageParser.parse(bookingMessage).get(0);
        assertEquals("Test", redisHashSetFieldEntry.getKey());
        assertEquals("booking-order-1", redisHashSetFieldEntry.getValue());
    }

    @Test
    public void shouldAcceptStringWithPatternForCollectionKeyWithEmptyVariables() {
        setRedisSinkConfig("message", "Test-%s", RedisSinkDataType.HASHSET);
        ProtoToFieldMapper protoToFieldMapperForBookingMessage = new ProtoToFieldMapper(testMessageProtoParser, getProperties("2", "order_number_1"));

        RedisParser redisMessageParser = new RedisHashSetParser(protoToFieldMapperForBookingMessage, bookingMessageProtoParser, redisSinkConfig, statsDReporter);
        RedisHashSetFieldEntry redisHashSetFieldEntry = (RedisHashSetFieldEntry) redisMessageParser.parse(bookingMessage).get(0);
        assertEquals("Test-%s", redisHashSetFieldEntry.getKey());
        assertEquals("booking-order-1", redisHashSetFieldEntry.getValue());
    }

    @Test
    public void shouldParseLongMessageForKey() {
        setRedisSinkConfig("message", "Test-%d,52", RedisSinkDataType.HASHSET);
        ProtoToFieldMapper protoToFieldMapperForBookingMessage = new ProtoToFieldMapper(testMessageProtoParser, getProperties("2", "order_number_%d,52"));
        RedisParser redisMessageParser = new RedisHashSetParser(protoToFieldMapperForBookingMessage, bookingMessageProtoParser, redisSinkConfig, statsDReporter);

        RedisHashSetFieldEntry redisHashSetFieldEntry = (RedisHashSetFieldEntry) redisMessageParser.parse(bookingMessage).get(0);

        assertEquals("Test-2000", redisHashSetFieldEntry.getKey());
        assertEquals(String.format("order_number_%s", bookingCustomerTotalFare), redisHashSetFieldEntry.getField());
        assertEquals("booking-order-1", redisHashSetFieldEntry.getValue());
    }

    @Test
    public void shouldParseLongMessageWithSpaceForKey() {
        setRedisSinkConfig("message", "Test-%d,52", RedisSinkDataType.HASHSET);
        ProtoToFieldMapper protoToFieldMapperForBookingMessage = new ProtoToFieldMapper(testMessageProtoParser, getProperties("2", "order_number_%d, 52"));
        RedisParser redisMessageParser = new RedisHashSetParser(protoToFieldMapperForBookingMessage, bookingMessageProtoParser, redisSinkConfig, statsDReporter);

        RedisHashSetFieldEntry redisHashSetFieldEntry = (RedisHashSetFieldEntry) redisMessageParser.parse(bookingMessage).get(0);

        assertEquals("Test-2000", redisHashSetFieldEntry.getKey());
        assertEquals(String.format("order_number_%s", bookingCustomerTotalFare), redisHashSetFieldEntry.getField());
        assertEquals("booking-order-1", redisHashSetFieldEntry.getValue());
    }

    @Test
    public void shouldParseStringMessageForKey() {
        setRedisSinkConfig("message", "Test-%d,52", RedisSinkDataType.HASHSET);
        ProtoToFieldMapper protoToFieldMapperForBookingMessage = new ProtoToFieldMapper(testMessageProtoParser, getProperties("2", "order_number_%s,2"));
        RedisParser redisMessageParser = new RedisHashSetParser(protoToFieldMapperForBookingMessage, bookingMessageProtoParser, redisSinkConfig, statsDReporter);

        RedisHashSetFieldEntry redisHashSetFieldEntry = (RedisHashSetFieldEntry) redisMessageParser.parse(bookingMessage).get(0);

        assertEquals("Test-2000", redisHashSetFieldEntry.getKey());
        assertEquals(String.format("order_number_%s", bookingOrderNumber), redisHashSetFieldEntry.getField());
        assertEquals("booking-order-1", redisHashSetFieldEntry.getValue());
    }

    @Test
    public void shouldHandleStaticStringForKey() {
        setRedisSinkConfig("message", "Test-%d,52", RedisSinkDataType.HASHSET);
        ProtoToFieldMapper protoToFieldMapperForBookingMessage = new ProtoToFieldMapper(testMessageProtoParser, getProperties("2", "order_number"));
        RedisParser redisMessageParser = new RedisHashSetParser(protoToFieldMapperForBookingMessage, bookingMessageProtoParser, redisSinkConfig, statsDReporter);

        RedisHashSetFieldEntry redisHashSetFieldEntry = (RedisHashSetFieldEntry) redisMessageParser.parse(bookingMessage).get(0);
        assertEquals("Test-2000", redisHashSetFieldEntry.getKey());
        assertEquals("order_number", redisHashSetFieldEntry.getField());
        assertEquals("booking-order-1", redisHashSetFieldEntry.getValue());
    }

    @Test
    public void shouldHandleStaticStringWithPatternForKey() {
        setRedisSinkConfig("message", "Test-%d,52", RedisSinkDataType.HASHSET);
        ProtoToFieldMapper protoToFieldMapperForBookingMessage = new ProtoToFieldMapper(testMessageProtoParser, getProperties("2", "order_number%s"));
        RedisParser redisMessageParser = new RedisHashSetParser(protoToFieldMapperForBookingMessage, bookingMessageProtoParser, redisSinkConfig, statsDReporter);

        RedisHashSetFieldEntry redisHashSetFieldEntry = (RedisHashSetFieldEntry) redisMessageParser.parse(bookingMessage).get(0);
        assertEquals("Test-2000", redisHashSetFieldEntry.getKey());
        assertEquals("order_number%s", redisHashSetFieldEntry.getField());
        assertEquals("booking-order-1", redisHashSetFieldEntry.getValue());
    }

    @Test
    public void shouldThrowErrorForInvalidFormatForKey() {
        expectedException.expect(UnknownFormatConversionException.class);
        expectedException.expectMessage("Conversion = '%");

        setRedisSinkConfig("message", "Test-%d,52", RedisSinkDataType.HASHSET);
        ProtoToFieldMapper protoToFieldMapperForBookingMessage = new ProtoToFieldMapper(testMessageProtoParser, getProperties("2", "order_number-%,52"));
        RedisParser redisMessageParser = new RedisHashSetParser(protoToFieldMapperForBookingMessage, bookingMessageProtoParser, redisSinkConfig, statsDReporter);

        redisMessageParser.parse(bookingMessage);
    }

    @Test
    public void shouldThrowErrorForIncompatibleFormatForKey() {
        expectedException.expect(IllegalFormatConversionException.class);
        expectedException.expectMessage("d != java.lang.String");

        setRedisSinkConfig("message", "Test-%d,52", RedisSinkDataType.HASHSET);
        ProtoToFieldMapper protoToFieldMapperForBookingMessage = new ProtoToFieldMapper(testMessageProtoParser, getProperties("2", "order_number-%d,2"));
        RedisParser redisMessageParser = new RedisHashSetParser(protoToFieldMapperForBookingMessage, bookingMessageProtoParser, redisSinkConfig, statsDReporter);

        RedisHashSetFieldEntry redisHashSetFieldEntry = (RedisHashSetFieldEntry) redisMessageParser.parse(message).get(0);

        assertEquals("ORDER-DETAILS", redisHashSetFieldEntry.getValue());
        assertEquals("details", redisHashSetFieldEntry.getField());
        assertEquals("Test-test-order", redisHashSetFieldEntry.getKey());
    }

    @Test
    public void shouldThrowExceptionForEmptyKey() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Template '' is invalid");

        setRedisSinkConfig("message", "Test-%d,52", RedisSinkDataType.HASHSET);
        ProtoToFieldMapper protoToFieldMapperForBookingMessage = new ProtoToFieldMapper(testMessageProtoParser, getProperties("2", ""));
        RedisParser redisMessageParser = new RedisHashSetParser(protoToFieldMapperForBookingMessage, bookingMessageProtoParser, redisSinkConfig, statsDReporter);

        redisMessageParser.parse(bookingMessage);
    }

    @Test
    public void shouldParseKeyWhenKafkaMessageParseModeSetToKey() {
        setRedisSinkConfig("key", "Test-%s,1", RedisSinkDataType.HASHSET);
        ProtoToFieldMapper protoToFieldMapperForKey = new ProtoToFieldMapper(testKeyProtoParser, getProperties("1", "order"));

        RedisParser redisMessageParser = new RedisHashSetParser(protoToFieldMapperForKey, testKeyProtoParser, redisSinkConfig, statsDReporter);
        RedisHashSetFieldEntry redisHashSetFieldEntry = (RedisHashSetFieldEntry) redisMessageParser.parse(bookingMessage).get(0);

        assertEquals(redisHashSetFieldEntry.getValue(), "ORDER-1-FROM-KEY");
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowInvalidProtocolBufferExceptionWhenIncorrectProtocolUsed() {
        setRedisSinkConfig("message", "Test-%s,1", RedisSinkDataType.HASHSET);
        ProtoParser protoParserForTest = new ProtoParser(stencilClient, TestNestedRepeatedMessage.class.getCanonicalName());
        ProtoToFieldMapper protoToFieldMapperForTest = new ProtoToFieldMapper(protoParserForTest, getProperties("3", "details"));
        RedisParser redisMessageParser = new RedisHashSetParser(protoToFieldMapperForTest, protoParserForTest, redisSinkConfig, statsDReporter);

        redisMessageParser.parse(message);
    }

    private Properties getProperties(String s, String order) {
        Properties propertiesForKey = new Properties();
        propertiesForKey.setProperty(s, order);
        return propertiesForKey;
    }
}
