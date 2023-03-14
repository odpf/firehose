package com.gotocompany.firehose.filter;

import com.gotocompany.firehose.message.Message;
import com.gotocompany.firehose.metrics.FirehoseInstrumentation;
import com.gotocompany.firehose.consumer.TestKey;
import com.gotocompany.firehose.consumer.TestMessage;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class NoOpFilterTest {

    @Mock
    private FirehoseInstrumentation firehoseInstrumentation;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void shouldLogNoFilterSelected() {
        new NoOpFilter(firehoseInstrumentation);
        verify(firehoseInstrumentation, times(1)).logInfo("No filter is selected");
    }

    @Test
    public void shouldReturnInputListOfMessagesForProtobufMessageType() throws FilterException {
        TestKey testKeyProto1 = TestKey.newBuilder().setOrderNumber("123").setOrderUrl("abc").build();
        TestMessage testMessageProto1 = TestMessage.newBuilder().setOrderNumber("123").setOrderUrl("abc").setOrderDetails("details").build();
        TestKey testKeyProto2 = TestKey.newBuilder().setOrderNumber("92").setOrderUrl("pqr").build();
        TestMessage testMessageProto2 = TestMessage.newBuilder().setOrderNumber("92").setOrderUrl("pqr").setOrderDetails("details").build();
        Message message1 = new Message(testKeyProto1.toByteArray(), testMessageProto1.toByteArray(), "topic1", 0, 100);
        Message message2 = new Message(testKeyProto2.toByteArray(), testMessageProto2.toByteArray(), "topic1", 0, 101);
        NoOpFilter noOpFilter = new NoOpFilter(firehoseInstrumentation);
        FilteredMessages expectedMessages = new FilteredMessages();
        expectedMessages.addToValidMessages(message1);
        expectedMessages.addToValidMessages(message2);
        FilteredMessages filteredMessages = noOpFilter.filter(Arrays.asList(message1, message2));
        assertEquals(expectedMessages, filteredMessages);
    }

    @Test
    public void shouldReturnInputListOfMessagesForJsonMessageType() throws FilterException {
        String testKeyJson1 = "{\"order_number\":\"123\",\"order_url\":\"abc\"}";
        String testMessageJson1 = "{\"order_number\":\"123\",\"order_url\":\"abc\",\"order_details\":\"details\"}";
        String testKeyJson2 = "{\"order_number\":\"92\",\"order_url\":\"pqr\"}";
        String testMessageJson2 = "{\"order_number\":\"92\",\"order_url\":\"pqr\",\"order_details\":\"details\"}";
        Message message1 = new Message(testKeyJson1.getBytes(), testMessageJson1.getBytes(), "topic1", 0, 100);
        Message message2 = new Message(testKeyJson2.getBytes(), testMessageJson2.getBytes(), "topic1", 0, 101);
        FilteredMessages expectedMessages = new FilteredMessages();
        expectedMessages.addToValidMessages(message1);
        expectedMessages.addToValidMessages(message2);
        NoOpFilter noOpFilter = new NoOpFilter(firehoseInstrumentation);
        FilteredMessages filteredMessages = noOpFilter.filter(Arrays.asList(message1, message2));
        assertEquals(expectedMessages, filteredMessages);
    }
}
