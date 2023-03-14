package com.gotocompany.firehose.sink.dlq;

import com.gotocompany.firehose.message.Message;
import com.gotocompany.firehose.consumer.TestKey;
import com.gotocompany.firehose.consumer.TestMessage;
import com.gotocompany.firehose.exception.DeserializerException;
import com.gotocompany.firehose.metrics.FirehoseInstrumentation;
import com.gotocompany.firehose.sink.dlq.kafka.KafkaDlqWriter;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class KafkaDlqWriterTest {

    @Mock
    private Message message;

    @Mock
    private KafkaProducer<byte[], byte[]> kafkaProducer;

    @Mock
    private FirehoseInstrumentation firehoseInstrumentation;

    private KafkaDlqWriter kafkaDlqWriter;

    @Before
    public void setUp() throws Exception {
        kafkaDlqWriter = new KafkaDlqWriter(kafkaProducer, "test-topic", firehoseInstrumentation);
    }

    @Test
    public void shouldReturnEmptyListWhenWriteEmptyMessages() throws IOException {
        ArrayList<Message> messages = new ArrayList<>();
        List<Message> messageList = kafkaDlqWriter.write(messages);

        Mockito.verifyNoInteractions(kafkaProducer);
        assertTrue(messageList.isEmpty());
    }

    @Test
    public void shouldWriteToKafkaProducer() {
        ArrayList<Message> messages = new ArrayList<>();
        messages.add(message);
        messages.add(message);

        Thread thread = new Thread(() -> {
            try {
                kafkaDlqWriter.write(messages);
            } catch (IOException | DeserializerException e) {
                e.printStackTrace();
            }
        });

        thread.start();

        ArgumentCaptor<Callback> callbacks = ArgumentCaptor.forClass(Callback.class);
        verify(kafkaProducer, timeout(200).times(2)).send(any(), callbacks.capture());
        List<Callback> calls = callbacks.getAllValues();
        calls.get(0).onCompletion(null, null);
        calls.get(1).onCompletion(null, null);
    }

    @Test
    public void shouldSendWithCorrectArgumentsIfHeadersAreNotSet() {
        ArrayList<Message> messages = new ArrayList<>();
        messages.add(message);
        messages.add(message);

        Thread thread = new Thread(() -> {
            try {
                kafkaDlqWriter.write(messages);
            } catch (IOException | DeserializerException e) {
                e.printStackTrace();
            }
        });

        thread.start();

        ArgumentCaptor<Callback> callbacks = ArgumentCaptor.forClass(Callback.class);
        ArgumentCaptor<ProducerRecord> records = ArgumentCaptor.forClass(ProducerRecord.class);

        verify(kafkaProducer, timeout(200).times(2)).send(records.capture(), callbacks.capture());
        List<Callback> calls = callbacks.getAllValues();
        List<ProducerRecord> actualRecords = records.getAllValues();

        calls.get(0).onCompletion(null, null);
        calls.get(1).onCompletion(null, null);

        assertEquals(expectedRecords(messages.get(0)), actualRecords.get(0));
        assertEquals(expectedRecords(messages.get(1)), actualRecords.get(1));
    }

    private ProducerRecord<byte[], byte[]> expectedRecords(Message expectedMessage) {
        return new ProducerRecord<>("test-topic", null, null, expectedMessage.getLogKey(),
                expectedMessage.getLogMessage(), expectedMessage.getHeaders());
    }

    @Test
    public void shouldSendWithCorrectArgumentsIfHeadersAreSet() {
        ArrayList<Message> messages = new ArrayList<>();
        Headers headers = new RecordHeaders();
        headers.add(new RecordHeader("key1", "value1".getBytes()));
        headers.add(new RecordHeader("key2", "value2".getBytes()));

        TestMessage testMessage = TestMessage.newBuilder().setOrderNumber("123").setOrderUrl("abc")
                .setOrderDetails("details").build();
        TestKey key = TestKey.newBuilder().setOrderNumber("123").setOrderUrl("abc").build();

        Message msg1 = new Message(key.toByteArray(), testMessage.toByteArray(), "topic1", 0, 100, headers, 1L, 1L);
        Message msg2 = new Message(key.toByteArray(), testMessage.toByteArray(), "topic2", 0, 100, headers, 1L, 1L);
        messages.add(msg1);
        messages.add(msg2);

        Thread thread = new Thread(() -> {
            try {
                kafkaDlqWriter.write(messages);
            } catch (IOException | DeserializerException e) {
                e.printStackTrace();
            }
        });

        thread.start();

        ArgumentCaptor<Callback> callbacks = ArgumentCaptor.forClass(Callback.class);
        ArgumentCaptor<ProducerRecord> records = ArgumentCaptor.forClass(ProducerRecord.class);

        verify(kafkaProducer, timeout(200).times(2)).send(records.capture(), callbacks.capture());
        List<Callback> calls = callbacks.getAllValues();
        List<ProducerRecord> actualRecords = records.getAllValues();

        calls.get(0).onCompletion(null, null);
        calls.get(1).onCompletion(null, null);

        assertEquals(expectedRecords(messages.get(0)), actualRecords.get(0));
        assertEquals(expectedRecords(messages.get(1)), actualRecords.get(1));
    }

    @Test
    public void shouldRecordMessagesToBeSendToKafkaRetryQueue() throws Exception {
        ArrayList<Message> messages = new ArrayList<>();
        CountDownLatch completedLatch = new CountDownLatch(1);
        messages.add(message);
        messages.add(message);

        Thread thread = new Thread(() -> {
            try {
                kafkaDlqWriter.write(messages);
                completedLatch.countDown();
            } catch (IOException | DeserializerException e) {
                e.printStackTrace();
            }
        });

        thread.start();

        ArgumentCaptor<Callback> callbacks = ArgumentCaptor.forClass(Callback.class);
        verify(kafkaProducer, timeout(200).times(2)).send(any(), callbacks.capture());
        List<Callback> calls = callbacks.getAllValues();
        calls.get(0).onCompletion(null, null);
        calls.get(1).onCompletion(null, null);
        completedLatch.await();
        verify(firehoseInstrumentation, times(1)).logInfo("Pushing {} messages to retry queue topic : {}", 2, "test-topic");
        verify(firehoseInstrumentation, times(1)).logInfo("Successfully pushed {} messages to {}", 2, "test-topic");
    }

    @Test
    public void shouldReturnFailedMessagesWhenExceptionThrown() throws InterruptedException {
        ArrayList<Message> messages = new ArrayList<>();
        CountDownLatch completedLatch = new CountDownLatch(1);
        messages.add(message);
        messages.add(message);

        final List<Message> retryMessages = new LinkedList<>();
        Thread thread = new Thread(() -> {
            try {
                List<Message> result = kafkaDlqWriter.write(messages);
                retryMessages.addAll(result);
                completedLatch.countDown();
            } catch (IOException | DeserializerException e) {
                e.printStackTrace();
            }
        });

        thread.start();

        ArgumentCaptor<Callback> callbacks = ArgumentCaptor.forClass(Callback.class);
        verify(kafkaProducer, timeout(200).times(2)).send(any(), callbacks.capture());
        List<Callback> calls = callbacks.getAllValues();
        calls.get(0).onCompletion(null, null);
        calls.get(1).onCompletion(null, new Exception());
        completedLatch.await();
        assertEquals(1, retryMessages.size());
    }
}
