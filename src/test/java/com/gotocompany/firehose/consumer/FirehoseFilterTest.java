package com.gotocompany.firehose.consumer;

import com.gotocompany.firehose.filter.Filter;
import com.gotocompany.firehose.filter.FilterException;
import com.gotocompany.firehose.filter.FilteredMessages;
import com.gotocompany.firehose.message.Message;
import com.gotocompany.firehose.metrics.FirehoseInstrumentation;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;

public class FirehoseFilterTest {

    @Test
    public void shouldReturnAllMessages() throws FilterException {
        TestMessage logMessage1 = TestMessage.newBuilder().setOrderNumber("123").setOrderUrl("abc").setOrderDetails("details").build();
        TestKey logKey1 = TestKey.newBuilder().setOrderNumber("123").setOrderUrl("abc").build();
        Message message1 = new Message(logKey1.toByteArray(), logMessage1.toByteArray(), "Topic1", 0, 100);

        TestMessage logMessage2 = TestMessage.newBuilder().setOrderNumber("567").setOrderUrl("def").setOrderDetails("moredetails").build();
        TestKey logKey2 = TestKey.newBuilder().setOrderNumber("567").setOrderUrl("def").build();
        Message message2 = new Message(logKey2.toByteArray(), logMessage2.toByteArray(), "Topic2", 0, 100);

        TestMessage logMessage3 = TestMessage.newBuilder().setOrderNumber("444").setOrderUrl("xyz").setOrderDetails("moredetails").build();
        TestKey logKey3 = TestKey.newBuilder().setOrderNumber("444").setOrderUrl("xyz").build();
        Message message3 = new Message(logKey3.toByteArray(), logMessage3.toByteArray(), "Topic3", 0, 100);

        List<Message> messages = new ArrayList<Message>() {{
            add(message1);
            add(message2);
            add(message3);
        }};
        Filter filter = Mockito.mock(Filter.class);
        FirehoseInstrumentation firehoseInstrumentation = Mockito.mock(FirehoseInstrumentation.class);

        FirehoseFilter firehoseFilter = new FirehoseFilter(filter, firehoseInstrumentation);
        Mockito.when(filter.filter(messages)).thenReturn(new FilteredMessages() {{
            addToValidMessages(message1);
            addToValidMessages(message2);
            addToValidMessages(message3);
        }});

        FilteredMessages actualFilteredMessage = firehoseFilter.applyFilter(messages);
        Assert.assertEquals(messages, actualFilteredMessage.getValidMessages());
        Assert.assertEquals(new ArrayList<>(), actualFilteredMessage.getInvalidMessages());
        Mockito.verify(filter, Mockito.times(1)).filter(messages);
        Mockito.verify(firehoseInstrumentation, Mockito.times(0)).captureFilteredMessageCount(Mockito.anyInt());
    }


    @Test
    public void shouldPartitiondMessages() throws FilterException {
        TestMessage logMessage1 = TestMessage.newBuilder().setOrderNumber("123").setOrderUrl("abc").setOrderDetails("details").build();
        TestKey logKey1 = TestKey.newBuilder().setOrderNumber("123").setOrderUrl("abc").build();
        Message message1 = new Message(logKey1.toByteArray(), logMessage1.toByteArray(), "Topic1", 0, 100);

        TestMessage logMessage2 = TestMessage.newBuilder().setOrderNumber("567").setOrderUrl("def").setOrderDetails("moredetails").build();
        TestKey logKey2 = TestKey.newBuilder().setOrderNumber("567").setOrderUrl("def").build();
        Message message2 = new Message(logKey2.toByteArray(), logMessage2.toByteArray(), "Topic2", 0, 100);

        TestMessage logMessage3 = TestMessage.newBuilder().setOrderNumber("444").setOrderUrl("xyz").setOrderDetails("moredetails").build();
        TestKey logKey3 = TestKey.newBuilder().setOrderNumber("444").setOrderUrl("xyz").build();
        Message message3 = new Message(logKey3.toByteArray(), logMessage3.toByteArray(), "Topic3", 0, 100);

        List<Message> messages = new ArrayList<Message>() {{
            add(message1);
            add(message2);
            add(message3);
        }};

        Filter filter = Mockito.mock(Filter.class);
        FirehoseInstrumentation firehoseInstrumentation = Mockito.mock(FirehoseInstrumentation.class);
        FirehoseFilter firehoseFilter = new FirehoseFilter(filter, firehoseInstrumentation);
        Mockito.when(filter.filter(messages)).thenReturn(new FilteredMessages() {{
            addToValidMessages(message1);
            addToInvalidMessages(message2);
            addToInvalidMessages(message3);
        }});
        FilteredMessages actualFilteredMessage = firehoseFilter.applyFilter(messages);

        Assert.assertEquals(new ArrayList<Message>() {{
            add(message1);
        }}, actualFilteredMessage.getValidMessages());
        Assert.assertEquals(new ArrayList<Message>() {{
            add(message2);
            add(message3);
        }}, actualFilteredMessage.getInvalidMessages());
        Mockito.verify(filter, Mockito.times(1)).filter(messages);
        Mockito.verify(firehoseInstrumentation, Mockito.times(1)).captureFilteredMessageCount(2);
    }
}
