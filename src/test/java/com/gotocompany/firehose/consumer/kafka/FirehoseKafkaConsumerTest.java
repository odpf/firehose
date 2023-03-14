package com.gotocompany.firehose.consumer.kafka;

import com.gotocompany.firehose.config.KafkaConsumerConfig;
import com.gotocompany.firehose.message.Message;
import com.gotocompany.firehose.metrics.FirehoseInstrumentation;
import com.gotocompany.firehose.metrics.Metrics;
import com.gotocompany.firehose.consumer.TestKey;
import com.gotocompany.firehose.consumer.TestMessage;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.MockitoJUnitRunner;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.*;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

@RunWith(MockitoJUnitRunner.class)
public class FirehoseKafkaConsumerTest {
    @Mock
    private org.apache.kafka.clients.consumer.KafkaConsumer<byte[], byte[]> kafkaConsumer;
    @Mock
    private ConsumerRecords<byte[], byte[]> consumerRecords;
    @Mock
    private FirehoseInstrumentation firehoseInstrumentation;
    @Mock
    private KafkaConsumerConfig consumerConfig;
    private TestMessage message;
    private TestKey key;
    private FirehoseKafkaConsumer firehoseKafkaConsumer;
    private final String consumerGroupId = "consumer-group-01";

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        message = TestMessage.newBuilder().setOrderNumber("123").setOrderUrl("abc").setOrderDetails("details").build();
        key = TestKey.newBuilder().setOrderNumber("123").setOrderUrl("abc").build();
        firehoseKafkaConsumer = new FirehoseKafkaConsumer(kafkaConsumer, consumerConfig, firehoseInstrumentation);
        when(consumerConfig.getSourceKafkaPollTimeoutMs()).thenReturn(500L);
        when(consumerConfig.getSourceKafkaConsumerGroupId()).thenReturn(consumerGroupId);
        when(kafkaConsumer.poll(Duration.ofMillis(500L))).thenReturn(consumerRecords);
    }

    @Test
    public void getsMessagesFromEsbLog() {
        ConsumerRecord<byte[], byte[]> record1 = new ConsumerRecord<>("topic1", 1, 0, key.toByteArray(), message.toByteArray());
        ConsumerRecord<byte[], byte[]> record2 = new ConsumerRecord<>("topic2", 1, 0, key.toByteArray(), message.toByteArray());
        when(consumerRecords.iterator()).thenReturn(Arrays.asList(record1, record2).iterator());

        Message expectedMsg1 = new Message(key.toByteArray(), message.toByteArray(), "topic1", 0, 100);
        Message expectedMsg2 = new Message(key.toByteArray(), message.toByteArray(), "topic2", 0, 100);

        when(consumerRecords.count()).thenReturn(2);
        List<Message> messages = firehoseKafkaConsumer.readMessages();

        assertNotNull(messages);
        assertEquals(2, messages.size());
        assertArrayEquals(expectedMsg1.getLogMessage(), messages.get(0).getLogMessage());
        assertArrayEquals(expectedMsg2.getLogMessage(), messages.get(1).getLogMessage());
    }

    @Test
    public void getsMessagesFromEsbLogWithHeadersIfKafkaHeadersAreSet() {
        Headers headers = new RecordHeaders();
        ConsumerRecord<byte[], byte[]> record1 = new ConsumerRecord<>("topic1", 1, 0, 0, TimestampType.CREATE_TIME, 0L, 0, 0, key.toByteArray(), message.toByteArray(), headers);
        ConsumerRecord<byte[], byte[]> record2 = new ConsumerRecord<>("topic2", 1, 0, 0, TimestampType.CREATE_TIME, 0L, 0, 0, key.toByteArray(), message.toByteArray(), headers);
        when(consumerRecords.iterator()).thenReturn(Arrays.asList(record1, record2).iterator());

        Message expectedMsg1 = new Message(key.toByteArray(), message.toByteArray(), "topic1", 0, 100, headers, 1L, 1L);
        Message expectedMsg2 = new Message(key.toByteArray(), message.toByteArray(), "topic2", 0, 100, headers, 1L, 1L);

        List<Message> messages = firehoseKafkaConsumer.readMessages();

        assertNotNull(messages);
        assertEquals(2, messages.size());
        assertEquals(expectedMsg1.getTopic(), messages.get(0).getTopic());
        assertEquals(expectedMsg2.getTopic(), messages.get(1).getTopic());
    }

    @Test
    public void shouldrecordStatsFromEsbLog() {
        ConsumerRecord<byte[], byte[]> record1 = new ConsumerRecord<>("topic1", 1, 0, key.toByteArray(), message.toByteArray());
        ConsumerRecord<byte[], byte[]> record2 = new ConsumerRecord<>("topic2", 1, 0, key.toByteArray(), message.toByteArray());
        Iterator iteratorMock = Mockito.mock(Iterator.class);
        when(iteratorMock.hasNext()).thenReturn(true, true, false);
        when(iteratorMock.next()).thenReturn(record1, record2);
        when(consumerRecords.iterator()).thenReturn(iteratorMock);
        when(consumerRecords.count()).thenReturn(2);

        firehoseKafkaConsumer.readMessages();

        verify(firehoseInstrumentation, times(1)).logInfo("Pulled {} messages", 2);
        verify(firehoseInstrumentation, times(1)).capturePulledMessageHistogram(2);
        verify(firehoseInstrumentation, times(1)).logDebug("Pulled record: {}", record1);
        verify(firehoseInstrumentation, times(1)).logDebug("Pulled record: {}", record2);
        verify(firehoseInstrumentation, times(1)).captureGlobalMessageMetrics(Metrics.MessageScope.CONSUMER, 2);
    }

    @Test
    public void shouldCallCommit() {
        Iterator iteratorMock = Mockito.mock(Iterator.class);
        when(iteratorMock.hasNext()).thenReturn(false);
        when(consumerRecords.iterator()).thenReturn(iteratorMock);
        firehoseKafkaConsumer.readMessages();
        firehoseKafkaConsumer.commit();
        verify(kafkaConsumer, times(0)).commitAsync();
        verify(kafkaConsumer, times(1)).commitSync();
    }

    @Test
    public void shouldAsyncCommit() {
        when(consumerConfig.isSourceKafkaAsyncCommitEnable()).thenReturn(true);
        Iterator iteratorMock = Mockito.mock(Iterator.class);
        when(iteratorMock.hasNext()).thenReturn(false);
        when(consumerRecords.iterator()).thenReturn(iteratorMock);
        firehoseKafkaConsumer.readMessages();
        firehoseKafkaConsumer.commit();
        verify(kafkaConsumer, times(1)).commitAsync(any());
        verify(kafkaConsumer, times(0)).commitSync();
    }

    @Test
    public void shouldCallCloseOnConsumer() {
        firehoseKafkaConsumer.close();

        verify(kafkaConsumer).close();
        verify(firehoseInstrumentation).logInfo("Consumer is closing");
    }

    @Test
    public void shouldSuppressExceptionOnClose() {
        doThrow(new RuntimeException()).when(kafkaConsumer).close();

        try {
            firehoseKafkaConsumer.close();
            verify(firehoseInstrumentation, times(1)).logInfo("Consumer is closing");
        } catch (Exception kafkaConsumerException) {
            fail("Failed to supress exception on close");
        }
    }

    @Test
    public void shouldCaptureNonFatalError() {
        doThrow(new RuntimeException()).when(kafkaConsumer).close();
        firehoseKafkaConsumer.close();
        verify(firehoseInstrumentation, times(1)).captureNonFatalError(any(), any(), eq("Exception while closing consumer"));
    }

    @Test
    public void shouldCallAsyncCommitWithOffsets() {
        when(consumerConfig.isSourceKafkaAsyncCommitEnable()).thenReturn(true);
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<TopicPartition, OffsetAndMetadata>() {{
            put(new TopicPartition("topic1", 1), new OffsetAndMetadata(1));
            put(new TopicPartition("topic1", 2), new OffsetAndMetadata(1));
            put(new TopicPartition("topic1", 3), new OffsetAndMetadata(1));
        }};
        firehoseKafkaConsumer.commit(offsets);
        verify(kafkaConsumer, times(1)).commitAsync(eq(offsets), any());
        verify(kafkaConsumer, times(0)).commitSync(offsets);
    }

    @Test
    public void shouldCommitLatestOffsets() {
        when(consumerConfig.isSourceKafkaAsyncCommitEnable()).thenReturn(false);
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<TopicPartition, OffsetAndMetadata>() {{
            put(new TopicPartition("topic1", 1), new OffsetAndMetadata(1));
            put(new TopicPartition("topic1", 2), new OffsetAndMetadata(1));
            put(new TopicPartition("topic1", 3), new OffsetAndMetadata(1));
        }};
        firehoseKafkaConsumer.commit(offsets);
        firehoseKafkaConsumer.commit(offsets);
        verify(kafkaConsumer, times(1)).commitSync(offsets);

        offsets = new HashMap<TopicPartition, OffsetAndMetadata>() {{
            put(new TopicPartition("topic1", 1), new OffsetAndMetadata(1));
            put(new TopicPartition("topic1", 2), new OffsetAndMetadata(1));
            put(new TopicPartition("topic1", 3), new OffsetAndMetadata(2));
        }};
        firehoseKafkaConsumer.commit(offsets);
        verify(kafkaConsumer, times(1)).commitSync(new HashMap<TopicPartition, OffsetAndMetadata>() {{
            put(new TopicPartition("topic1", 3), new OffsetAndMetadata(2));
        }});

        offsets = new HashMap<TopicPartition, OffsetAndMetadata>() {{
            put(new TopicPartition("topic1", 1), new OffsetAndMetadata(5));
            put(new TopicPartition("topic1", 2), new OffsetAndMetadata(1));
            put(new TopicPartition("topic1", 3), new OffsetAndMetadata(2));
            put(new TopicPartition("topic1", 4), new OffsetAndMetadata(5));
        }};
        firehoseKafkaConsumer.commit(offsets);
        verify(kafkaConsumer, times(1)).commitSync(new HashMap<TopicPartition, OffsetAndMetadata>() {{
            put(new TopicPartition("topic1", 1), new OffsetAndMetadata(5));
            put(new TopicPartition("topic1", 4), new OffsetAndMetadata(5));
        }});
    }

    @Test
    public void shouldCommitLatestOffsetsWithAsyncCommit() {
        when(consumerConfig.isSourceKafkaAsyncCommitEnable()).thenReturn(true);
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<TopicPartition, OffsetAndMetadata>() {{
            put(new TopicPartition("topic1", 1), new OffsetAndMetadata(1));
            put(new TopicPartition("topic1", 2), new OffsetAndMetadata(1));
            put(new TopicPartition("topic1", 3), new OffsetAndMetadata(1));
        }};
        firehoseKafkaConsumer.commit(offsets);
        firehoseKafkaConsumer.commit(offsets);
        verify(kafkaConsumer, times(1)).commitAsync(eq(offsets), Mockito.any(OffsetCommitCallback.class));

        offsets = new HashMap<TopicPartition, OffsetAndMetadata>() {{
            put(new TopicPartition("topic1", 1), new OffsetAndMetadata(1));
            put(new TopicPartition("topic1", 2), new OffsetAndMetadata(1));
            put(new TopicPartition("topic1", 3), new OffsetAndMetadata(2));
        }};
        firehoseKafkaConsumer.commit(offsets);
        verify(kafkaConsumer, times(1)).commitAsync(eq(new HashMap<TopicPartition, OffsetAndMetadata>() {{
            put(new TopicPartition("topic1", 3), new OffsetAndMetadata(2));
        }}), Mockito.any(OffsetCommitCallback.class));

        offsets = new HashMap<TopicPartition, OffsetAndMetadata>() {{
            put(new TopicPartition("topic1", 1), new OffsetAndMetadata(5));
            put(new TopicPartition("topic1", 2), new OffsetAndMetadata(1));
            put(new TopicPartition("topic1", 3), new OffsetAndMetadata(2));
            put(new TopicPartition("topic1", 4), new OffsetAndMetadata(5));
        }};
        firehoseKafkaConsumer.commit(offsets);
        verify(kafkaConsumer, times(1)).commitAsync(eq(new HashMap<TopicPartition, OffsetAndMetadata>() {{
            put(new TopicPartition("topic1", 1), new OffsetAndMetadata(5));
            put(new TopicPartition("topic1", 4), new OffsetAndMetadata(5));
        }}), Mockito.any(OffsetCommitCallback.class));
    }
}
