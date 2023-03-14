package com.gotocompany.firehose.consumer;

import com.gotocompany.firehose.config.KafkaConsumerConfig;
import com.gotocompany.firehose.consumer.kafka.ConsumerAndOffsetManager;
import com.gotocompany.firehose.consumer.kafka.FirehoseKafkaConsumer;
import com.gotocompany.firehose.consumer.kafka.OffsetManager;
import com.gotocompany.firehose.message.Message;
import com.gotocompany.firehose.metrics.FirehoseInstrumentation;
import com.gotocompany.firehose.sink.Sink;
import org.aeonbits.owner.ConfigFactory;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class ConsumerAndOffsetManagerTest {


    private Message createMessage(String topic, int partition, int offset) {
        return new Message("".getBytes(), "".getBytes(), topic, partition, offset);
    }

    @Test
    public void shouldCommitToKafka() {
        Sink s1 = Mockito.mock(Sink.class);
        Sink s2 = Mockito.mock(Sink.class);
        Sink s3 = Mockito.mock(Sink.class);
        List<Sink> sinks = new ArrayList<Sink>() {{
            add(s1);
            add(s2);
            add(s3);
        }};
        FirehoseKafkaConsumer consumer = Mockito.mock(FirehoseKafkaConsumer.class);
        KafkaConsumerConfig config = ConfigFactory.create(KafkaConsumerConfig.class, new HashMap<>());
        FirehoseInstrumentation firehoseInstrumentation = Mockito.mock(FirehoseInstrumentation.class);
        OffsetManager offsetManager = new OffsetManager();
        ConsumerAndOffsetManager consumerAndOffsetManager = new ConsumerAndOffsetManager(sinks, offsetManager, consumer, config, firehoseInstrumentation);
        List<Message> messages = new ArrayList<Message>() {{
            add(createMessage("testing", 1, 1));
            add(createMessage("testing", 1, 2));
            add(createMessage("testing", 1, 3));
        }};
        Mockito.when(s1.canManageOffsets()).thenReturn(false);
        Mockito.when(consumer.readMessages()).thenReturn(messages);
        consumerAndOffsetManager.addOffsetsAndSetCommittable(consumerAndOffsetManager.readMessages());
        consumerAndOffsetManager.commit();
        Mockito.verify(consumer, Mockito.times(1)).commit(new HashMap<TopicPartition, OffsetAndMetadata>() {{
            put(new TopicPartition("testing", 1), new OffsetAndMetadata(4));
        }});
    }

    @Test
    public void shouldCommitToKafkaWithSinkManagesOwnOffsets() {
        Sink s1 = Mockito.mock(Sink.class);
        Sink s2 = Mockito.mock(Sink.class);
        Sink s3 = Mockito.mock(Sink.class);
        List<Sink> sinks = new ArrayList<Sink>() {{
            add(s1);
            add(s2);
            add(s3);
        }};
        List<Message> messages = new ArrayList<Message>() {{
            add(createMessage("testing1", 1, 1));
            add(createMessage("testing2", 1, 2));
            add(createMessage("testing3", 1, 3));
        }};
        OffsetManager offsetManager = new OffsetManager();
        FirehoseInstrumentation firehoseInstrumentation = Mockito.mock(FirehoseInstrumentation.class);
        FirehoseKafkaConsumer consumer = Mockito.mock(FirehoseKafkaConsumer.class);
        KafkaConsumerConfig config = ConfigFactory.create(KafkaConsumerConfig.class, new HashMap<>());

        Mockito.when(s1.canManageOffsets()).thenReturn(true);
        Mockito.when(s2.canManageOffsets()).thenReturn(true);
        Mockito.when(s3.canManageOffsets()).thenReturn(true);
        Mockito.when(consumer.readMessages()).thenReturn(messages);
        Mockito.doAnswer(invocation -> {
            offsetManager.addOffsetToBatch("test", messages.get(0));
            offsetManager.setCommittable("test");
            return null;
        }).when(s1).calculateCommittableOffsets();
        Mockito.doAnswer(invocation -> {
            offsetManager.addOffsetToBatch("test", messages.get(1));
            offsetManager.setCommittable("test");
            return null;
        }).when(s2).calculateCommittableOffsets();
        Mockito.doAnswer(invocation -> {
            offsetManager.addOffsetToBatch("test", messages.get(2));
            offsetManager.setCommittable("test");
            return null;
        }).when(s3).calculateCommittableOffsets();
        ConsumerAndOffsetManager consumerAndOffsetManager = new ConsumerAndOffsetManager(sinks, offsetManager, consumer, config, firehoseInstrumentation);
        consumerAndOffsetManager.addOffsetsAndSetCommittable(consumerAndOffsetManager.readMessages());
        consumerAndOffsetManager.commit();
        Mockito.verify(consumer, Mockito.times(1)).commit(new HashMap<TopicPartition, OffsetAndMetadata>() {{
            put(new TopicPartition("testing2", 1), new OffsetAndMetadata(3));
            put(new TopicPartition("testing3", 1), new OffsetAndMetadata(4));
            put(new TopicPartition("testing1", 1), new OffsetAndMetadata(2));
        }});
    }

    @Test
    public void shouldCommitAfterDelay() throws InterruptedException {
        Sink s1 = Mockito.mock(Sink.class);
        Sink s2 = Mockito.mock(Sink.class);
        Sink s3 = Mockito.mock(Sink.class);
        List<Sink> sinks = new ArrayList<Sink>() {{
            add(s1);
            add(s2);
            add(s3);
        }};
        FirehoseKafkaConsumer consumer = Mockito.mock(FirehoseKafkaConsumer.class);
        FirehoseInstrumentation instrumentation = Mockito.mock(FirehoseInstrumentation.class);
        OffsetManager offsetManager = new OffsetManager();
        KafkaConsumerConfig config = ConfigFactory.create(KafkaConsumerConfig.class, new HashMap<String, String>() {{
            put("SOURCE_KAFKA_CONSUMER_CONFIG_MANUAL_COMMIT_MIN_INTERVAL_MS", "500");
        }});
        ConsumerAndOffsetManager consumerAndOffsetManager = new ConsumerAndOffsetManager(sinks, offsetManager, consumer, config, instrumentation);
        consumerAndOffsetManager.commit();
        consumerAndOffsetManager.commit();
        Mockito.verify(consumer, Mockito.times(1)).commit(new HashMap<>());
        Thread.sleep(1000);
        consumerAndOffsetManager.commit();
        Mockito.verify(consumer, Mockito.times(2)).commit(new HashMap<>());
    }

    @Test
    public void shouldCommitWithoutDelay() {
        Sink s1 = Mockito.mock(Sink.class);
        Sink s2 = Mockito.mock(Sink.class);
        Sink s3 = Mockito.mock(Sink.class);
        List<Sink> sinks = new ArrayList<Sink>() {{
            add(s1);
            add(s2);
            add(s3);
        }};
        FirehoseKafkaConsumer consumer = Mockito.mock(FirehoseKafkaConsumer.class);
        FirehoseInstrumentation instrumentation = Mockito.mock(FirehoseInstrumentation.class);
        OffsetManager offsetManager = new OffsetManager();
        KafkaConsumerConfig config = ConfigFactory.create(KafkaConsumerConfig.class, new HashMap<String, String>() {{
            put("SOURCE_KAFKA_CONSUMER_CONFIG_MANUAL_COMMIT_MIN_INTERVAL_MS", "-1");
        }});
        ConsumerAndOffsetManager consumerAndOffsetManager = new ConsumerAndOffsetManager(sinks, offsetManager, consumer, config, instrumentation);
        consumerAndOffsetManager.commit();
        consumerAndOffsetManager.commit();
        consumerAndOffsetManager.commit();
        consumerAndOffsetManager.commit();
        consumerAndOffsetManager.commit();
        consumerAndOffsetManager.commit();
        consumerAndOffsetManager.commit();
        Mockito.verify(consumer, Mockito.times(7)).commit(new HashMap<>());
    }
}
