package com.gojek.esb.sink.redis.parsers;

import com.gojek.de.stencil.client.ClassLoadStencilClient;
import com.gojek.de.stencil.parser.ProtoParser;
import com.gojek.esb.config.RedisSinkConfig;
import com.gojek.esb.config.enums.RedisSinkDataType;
import com.gojek.esb.consumer.Message;
import com.gojek.esb.consumer.TestBookingLogMessage;
import com.gojek.esb.consumer.TestKey;
import com.gojek.esb.consumer.TestMessage;
import com.gojek.esb.metrics.StatsDReporter;
import com.gojek.esb.sink.redis.dataentry.RedisListEntry;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static junit.framework.TestCase.assertEquals;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class RedisListParserTest {
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
        when(redisSinkConfig.getSinkRedisListDataProtoIndex()).thenReturn("1");
    }

    @Test
    public void shouldParseStringMessageForCollectionKeyTemplateInList() {
        setRedisSinkConfig("message", "Test-%s,1", RedisSinkDataType.LIST);
        RedisParser redisParser = new RedisListParser(testMessageProtoParser, redisSinkConfig, statsDReporter);

        RedisListEntry redisListEntry = (RedisListEntry) redisParser.parse(message).get(0);

        assertEquals("test-order", redisListEntry.getValue());
        assertEquals("Test-test-order", redisListEntry.getKey());
    }

    @Test
    public void shouldParseKeyWhenKafkaMessageParseModeSetToKey() {
        setRedisSinkConfig("key", "Test-%s,1", RedisSinkDataType.LIST);

        RedisParser redisParser = new RedisListParser(testKeyProtoParser, redisSinkConfig, statsDReporter);
        RedisListEntry redisListEntry = (RedisListEntry) redisParser.parse(bookingMessage).get(0);

        assertEquals(redisListEntry.getValue(), "ORDER-1-FROM-KEY");
    }

    @Test
    public void shouldThrowExceptionForEmptyKey() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Template '' is invalid");

        setRedisSinkConfig("message", "", RedisSinkDataType.LIST);
        RedisParser redisParser = new RedisListParser(bookingMessageProtoParser, redisSinkConfig, statsDReporter);

        redisParser.parse(bookingMessage);
    }

    @Test
    public void shouldThrowExceptionForNoListProtoIndex() {
        setRedisSinkConfig("message", "Test-%s,1", RedisSinkDataType.LIST);
        when(redisSinkConfig.getSinkRedisListDataProtoIndex()).thenReturn(null);
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Please provide sink.redis.list.data.proto.index in list sink");

        RedisParser redisParser = new RedisListParser(bookingMessageProtoParser, redisSinkConfig, statsDReporter);

        redisParser.parse(bookingMessage);
    }
}
