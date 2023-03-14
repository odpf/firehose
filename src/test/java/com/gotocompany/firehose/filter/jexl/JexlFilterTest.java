package com.gotocompany.firehose.filter.jexl;

import com.gotocompany.firehose.config.FilterConfig;
import com.gotocompany.firehose.config.enums.FilterDataSourceType;
import com.gotocompany.firehose.filter.Filter;
import com.gotocompany.firehose.filter.FilterException;
import com.gotocompany.firehose.filter.FilteredMessages;
import com.gotocompany.firehose.message.Message;
import com.gotocompany.firehose.metrics.FirehoseInstrumentation;
import com.gotocompany.firehose.consumer.TestBookingLogKey;
import com.gotocompany.firehose.consumer.TestBookingLogMessage;
import com.gotocompany.firehose.consumer.TestKey;
import com.gotocompany.firehose.consumer.TestMessage;
import org.aeonbits.owner.ConfigFactory;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class JexlFilterTest {
    private FilterConfig kafkaConsumerConfig;
    private Filter filter;
    private TestMessage testMessage;
    private TestKey key;

    @Mock
    private FirehoseInstrumentation firehoseInstrumentation;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        Map<String, String> filterConfigs = new HashMap<>();
        filterConfigs.put("FILTER_DATA_SOURCE", "message");
        filterConfigs.put("FILTER_JEXL_EXPRESSION", "testMessage.getOrderNumber() == 123");
        filterConfigs.put("FILTER_SCHEMA_PROTO_CLASS", TestMessage.class.getName());
        kafkaConsumerConfig = ConfigFactory.create(FilterConfig.class, filterConfigs);

        key = TestKey.newBuilder().setOrderNumber("123").setOrderUrl("abc").build();
        testMessage = TestMessage.newBuilder().setOrderNumber("123").setOrderUrl("abc").setOrderDetails("details").build();
    }

    @Test
    public void shouldFilterEsbMessages() throws FilterException {
        Message message = new Message(key.toByteArray(), this.testMessage.toByteArray(), "topic1", 0, 100);
        filter = new JexlFilter(kafkaConsumerConfig, firehoseInstrumentation);
        FilteredMessages filteredMessages = filter.filter(Arrays.asList(message));
        FilteredMessages expectedMessages = new FilteredMessages();
        expectedMessages.addToValidMessages(message);
        assertEquals(expectedMessages, filteredMessages);
    }

    @Test
    public void shouldNotFilterEsbMessagesForEmptyBooleanValues() throws FilterException {
        TestBookingLogMessage bookingLogMessage = TestBookingLogMessage.newBuilder().setCustomerId("customerId").build();
        TestBookingLogKey bookingLogKey = TestBookingLogKey.newBuilder().build();
        Message message = new Message(bookingLogKey.toByteArray(), bookingLogMessage.toByteArray(), "topic1", 0, 100);
        HashMap<String, String> bookingFilterConfigs = new HashMap<>();
        bookingFilterConfigs.put("FILTER_DATA_SOURCE", "message");
        bookingFilterConfigs.put("FILTER_JEXL_EXPRESSION", "testBookingLogMessage.getCustomerDynamicSurgeEnabled() == false");
        bookingFilterConfigs.put("FILTER_SCHEMA_PROTO_CLASS", TestBookingLogMessage.class.getName());
        FilterConfig bookingConsumerConfig = ConfigFactory.create(FilterConfig.class, bookingFilterConfigs);
        JexlFilter bookingFilter = new JexlFilter(bookingConsumerConfig, firehoseInstrumentation);
        FilteredMessages expectedMessages = new FilteredMessages();
        expectedMessages.addToValidMessages(message);
        FilteredMessages filteredMessages = bookingFilter.filter(Arrays.asList(message));
        assertEquals(expectedMessages, filteredMessages);
    }

    @Test(expected = FilterException.class)
    public void shouldThrowExceptionOnInvalidFilterExpression() throws FilterException {
        Map<String, String> filterConfigs = new HashMap<>();
        filterConfigs.put("FILTER_DATA_SOURCE", "message");
        filterConfigs.put("FILTER_JEXL_EXPRESSION", "1+2");
        filterConfigs.put("FILTER_SCHEMA_PROTO_CLASS", TestMessage.class.getName());
        kafkaConsumerConfig = ConfigFactory.create(FilterConfig.class, filterConfigs);

        filter = new JexlFilter(kafkaConsumerConfig, firehoseInstrumentation);
        key = TestKey.newBuilder().setOrderNumber("123").setOrderUrl("abc").build();
        this.testMessage = TestMessage.newBuilder().setOrderNumber("123").setOrderUrl("abc").setOrderDetails("details").build();

        Message message = new Message(key.toByteArray(), this.testMessage.toByteArray(), "topic1", 0, 100);
        filter.filter(Arrays.asList(message));
    }

    @Test
    public void shouldLogFilterTypeIfFilterTypeIsNotNone() {
        Map<String, String> filterConfigs = new HashMap<>();
        filterConfigs.put("FILTER_DATA_SOURCE", "message");
        filterConfigs.put("FILTER_JEXL_EXPRESSION", "testMessage.getOrderNumber() == 123");
        filterConfigs.put("FILTER_SCHEMA_PROTO_CLASS", TestMessage.class.getName());
        kafkaConsumerConfig = ConfigFactory.create(FilterConfig.class, filterConfigs);

        new JexlFilter(kafkaConsumerConfig, firehoseInstrumentation);
        Mockito.verify(firehoseInstrumentation, Mockito.times(1)).logInfo("\n\tFilter type: {}", FilterDataSourceType.MESSAGE);
        Mockito.verify(firehoseInstrumentation, Mockito.times(1)).logInfo("\n\tFilter schema: {}", TestMessage.class.getName());
        Mockito.verify(firehoseInstrumentation, Mockito.times(1)).logInfo("\n\tFilter expression: {}", "testMessage.getOrderNumber() == 123");
    }
}
