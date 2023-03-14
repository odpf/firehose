package com.gotocompany.firehose.consumer.kafka;

import com.gotocompany.firehose.message.Message;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

public class OffsetManagerTest {

    private OffsetNode createOffsetNode(Message m) {
        return new OffsetNode(getTopicPartition(m), getOffsetAndMetadata(m));
    }

    private TopicPartition getTopicPartition(Message m) {
        return new TopicPartition(m.getTopic(), m.getPartition());
    }

    private OffsetAndMetadata getOffsetAndMetadata(Message m) {
        return new OffsetAndMetadata(m.getOffset() + 1);
    }

    private Message createMessage(String topic, int partition, int offset) {
        return new Message("".getBytes(), "".getBytes(), topic, partition, offset);
    }

    @Test
    public void shouldAddOffsetToBatch() {
        OffsetManager manger = new OffsetManager();
        Message message1 = createMessage("testing", 1, 1);
        Message message2 = createMessage("testing", 1, 2);
        Message message5 = createMessage("testing", 1, 5);
        List<Message> messages = new ArrayList<Message>() {{
            add(message2);
            add(message5);
            add(message1);
        }};
        OffsetBatchKey key = new OffsetBatchKey("test", 10);
        messages.forEach(message -> manger.addOffsetToBatch(key, message));
        Assert.assertEquals(3, manger.getOffsetsForBatch(key).size());
        Assert.assertTrue(manger.getOffsetsForBatch(key).
                containsAll(messages.stream().map(this::createOffsetNode).collect(Collectors.toList())));
        TreeSet<OffsetNode> offsetsForTopicPartition = manger.getOffsetsForTopicPartition(getTopicPartition(message1));
        Assert.assertEquals(3, offsetsForTopicPartition.size());
        Iterator<OffsetNode> it = offsetsForTopicPartition.iterator();
        Assert.assertEquals(createOffsetNode(message1), it.next());
        Assert.assertEquals(createOffsetNode(message2), it.next());
        Assert.assertEquals(createOffsetNode(message5), it.next());
    }

    @Test
    public void shouldAddMultipleBatches() {
        OffsetManager manger = new OffsetManager();
        Message message1 = createMessage("testing", 1, 1);
        Message message2 = createMessage("testing", 1, 2);
        Message message5 = createMessage("testing", 1, 5);

        List<Message> messages1 = new ArrayList<Message>() {{
            add(message2);
            add(message5);
            add(message1);
        }};
        OffsetBatchKey key1 = new OffsetBatchKey("test", 10);
        messages1.forEach(message -> manger.addOffsetToBatch(key1, message));

        Message message10 = createMessage("testing", 1, 10);
        Message message7 = createMessage("testing", 1, 7);
        Message message9 = createMessage("testing", 1, 9);

        List<Message> messages2 = new ArrayList<Message>() {{
            add(message10);
            add(message7);
            add(message9);
        }};
        OffsetBatchKey key2 = new OffsetBatchKey("test2", 10);
        messages2.forEach(message -> manger.addOffsetToBatch(key2, message));

        Assert.assertEquals(3, manger.getOffsetsForBatch(key1).size());
        Assert.assertTrue(manger.getOffsetsForBatch(key1).
                containsAll(messages1.stream().map(this::createOffsetNode).collect(Collectors.toList())));


        Assert.assertEquals(3, manger.getOffsetsForBatch(key2).size());
        Assert.assertTrue(manger.getOffsetsForBatch(key2).
                containsAll(messages2.stream().map(this::createOffsetNode).collect(Collectors.toList())));

    }

    @Test
    public void shouldAddMultipleTopicPartitions() {
        OffsetManager manger = new OffsetManager();
        Message message1 = createMessage("testing", 1, 1);
        Message message2 = createMessage("testing", 1, 2);
        Message message3 = createMessage("testing", 1, 5);

        Message message4 = createMessage("testing1", 2, 1);
        Message message5 = createMessage("testing1", 2, 2);
        Message message6 = createMessage("testing1", 2, 5);

        List<Message> messages1 = new ArrayList<Message>() {{
            add(message3);
            add(message5);
            add(message4);
            add(message1);
            add(message2);
            add(message6);
        }};
        OffsetBatchKey key1 = new OffsetBatchKey("test", 10);
        messages1.forEach(message -> manger.addOffsetToBatch(key1, message));
        Assert.assertEquals(6, manger.getOffsetsForBatch(key1).size());
        Assert.assertTrue(manger.getOffsetsForBatch(key1).
                containsAll(messages1.stream().map(this::createOffsetNode).collect(Collectors.toList())));

        TreeSet<OffsetNode> offsetsForTopicPartition = manger.getOffsetsForTopicPartition(getTopicPartition(message1));
        Assert.assertEquals(3, offsetsForTopicPartition.size());
        Iterator<OffsetNode> it = offsetsForTopicPartition.iterator();
        Assert.assertEquals(createOffsetNode(message1), it.next());
        Assert.assertEquals(createOffsetNode(message2), it.next());
        Assert.assertEquals(createOffsetNode(message3), it.next());

        offsetsForTopicPartition = manger.getOffsetsForTopicPartition(getTopicPartition(message4));
        Assert.assertEquals(3, offsetsForTopicPartition.size());
        it = offsetsForTopicPartition.iterator();
        Assert.assertEquals(createOffsetNode(message4), it.next());
        Assert.assertEquals(createOffsetNode(message5), it.next());
        Assert.assertEquals(createOffsetNode(message6), it.next());
    }

    @Test
    public void shouldCompactAndFetch() {
        OffsetManager manger = new OffsetManager();
        Message message1 = createMessage("testing", 1, 1);
        Message message2 = createMessage("testing", 1, 2);
        Message message3 = createMessage("testing", 1, 3);
        Message message4 = createMessage("testing", 1, 4);
        Message message5 = createMessage("testing", 1, 5);
        Message message6 = createMessage("testing", 1, 6);
        Message message7 = createMessage("testing1", 2, 7);
        Message message8 = createMessage("testing1", 2, 8);
        Message message9 = createMessage("testing1", 2, 9);
        Message message10 = createMessage("testing1", 2, 10);

        List<Message> messages = new ArrayList<Message>() {{
            add(message3);
            add(message5);
            add(message4);
            add(message1);
            add(message2);
            add(message6);
            add(message10);
            add(message8);
            add(message7);
            add(message9);
        }};
        OffsetBatchKey key1 = new OffsetBatchKey("test", 10);
        messages.forEach(message -> manger.addOffsetToBatch(key1, message));
        TreeSet<OffsetNode> offsetsForTopicPartition = manger.getOffsetsForTopicPartition(getTopicPartition(message1));
        Assert.assertEquals(6, offsetsForTopicPartition.size());

        // Test case 1
        // If the top is not committable then return empty
        Optional<OffsetNode> offsetNode = manger.compactAndFetchFirstCommittableNode(offsetsForTopicPartition);
        Assert.assertFalse(offsetNode.isPresent());

        // Test case 2
        // If only the first element is committable then return that
        // Does not remove anything from the treeSet
        Iterator<OffsetNode> it = offsetsForTopicPartition.iterator();
        it.next().setCommittable(true);
        offsetNode = manger.compactAndFetchFirstCommittableNode(offsetsForTopicPartition);
        Assert.assertTrue(offsetNode.isPresent());
        OffsetNode expected = createOffsetNode(message1);
        expected.setCommittable(true);
        Assert.assertEquals(expected, offsetNode.get());
        Assert.assertEquals(6, offsetsForTopicPartition.size());
        it = offsetsForTopicPartition.iterator();
        Assert.assertTrue(it.next().isCommittable());
        Assert.assertFalse(it.next().isCommittable());

        // Test Case 3
        it = offsetsForTopicPartition.iterator();
        it.next().setCommittable(true);
        it.next();
        it.next().setCommittable(true);
        it.next().setCommittable(true);

        offsetNode = manger.compactAndFetchFirstCommittableNode(offsetsForTopicPartition);
        Assert.assertTrue(offsetNode.isPresent());
        expected = createOffsetNode(message1);
        expected.setCommittable(true);
        Assert.assertEquals(expected, offsetNode.get());
        Assert.assertEquals(6, offsetsForTopicPartition.size());
        it = offsetsForTopicPartition.iterator();
        Assert.assertTrue(it.next().isCommittable());
        Assert.assertFalse(it.next().isCommittable());

        // Test case 4
        // Removes the redundant offsets from the treeSet.
        it = offsetsForTopicPartition.iterator();
        it.next().setCommittable(true);
        it.next().setCommittable(true);
        it.next().setCommittable(true);
        it.next().setCommittable(true);

        expected = createOffsetNode(message4);
        expected.setCommittable(true);
        offsetNode = manger.compactAndFetchFirstCommittableNode(offsetsForTopicPartition);
        Assert.assertTrue(offsetNode.isPresent());
        Assert.assertEquals(expected, offsetNode.get());
        Assert.assertEquals(3, offsetsForTopicPartition.size());
        it = offsetsForTopicPartition.iterator();
        Assert.assertTrue(it.next().isCommittable());
        Assert.assertFalse(it.next().isCommittable());


        //Test Case 5
        //if everything is committable then it should keep only one element
        offsetsForTopicPartition = manger.getOffsetsForTopicPartition(getTopicPartition(message7));
        Assert.assertEquals(4, offsetsForTopicPartition.size());
        it = offsetsForTopicPartition.iterator();
        it.next().setCommittable(true);
        it.next().setCommittable(true);
        it.next().setCommittable(true);
        it.next().setCommittable(true);

        expected = createOffsetNode(message10);
        expected.setCommittable(true);
        offsetNode = manger.compactAndFetchFirstCommittableNode(offsetsForTopicPartition);
        Assert.assertTrue(offsetNode.isPresent());
        Assert.assertEquals(expected, offsetNode.get());
        Assert.assertEquals(1, offsetsForTopicPartition.size());
        it = offsetsForTopicPartition.iterator();
        Assert.assertTrue(it.next().isCommittable());
    }

    @Test
    public void testCommitBatch() {
        OffsetManager manger = new OffsetManager();
        //Path1
        Message message1 = createMessage("testing", 1, 1);
        Message message2 = createMessage("testing1", 1, 2);
        Message message3 = createMessage("testing", 1, 3);
        Message message4 = createMessage("testing2", 1, 4);
        Message message5 = createMessage("testing", 1, 5);
        Message message6 = createMessage("testing3", 1, 6);

        //Path2
        Message message7 = createMessage("testing1", 2, 7);
        Message message8 = createMessage("testing", 2, 8);
        Message message9 = createMessage("testing", 2, 9);
        Message message10 = createMessage("testing1", 2, 10);

        List<Message> messageList1 = new ArrayList<Message>() {{
            add(message3);
            add(message5);
            add(message4);
            add(message1);
            add(message2);
            add(message6);
        }};
        List<Message> messageList2 = new ArrayList<Message>() {{
            add(message7);
            add(message10);
            add(message8);
            add(message9);
        }};

        OffsetBatchKey key1 = new OffsetBatchKey("test", 10);
        messageList1.forEach(message -> manger.addOffsetToBatch(key1, message));

        OffsetBatchKey key2 = new OffsetBatchKey("test2", 100);
        messageList2.forEach(message -> manger.addOffsetToBatch(key2, message));

        Set<OffsetNode> offsetsForBatch1 = manger.getOffsetsForBatch(key1);
        Set<OffsetNode> offsetsForBatch2 = manger.getOffsetsForBatch(key2);

        Assert.assertEquals(6, offsetsForBatch1.size());
        Assert.assertEquals(4, offsetsForBatch2.size());
        offsetsForBatch1.forEach(node -> Assert.assertFalse(node.isCommittable()));
        offsetsForBatch2.forEach(node -> Assert.assertFalse(node.isCommittable()));

        manger.setCommittable(key1);
        offsetsForBatch1.forEach(node -> Assert.assertTrue(node.isCommittable()));

        Message newMessage = createMessage("topic1", 10, 20);
        manger.addOffsetToBatch(key1, newMessage);
        offsetsForBatch1 = manger.getOffsetsForBatch(key1);
        Assert.assertEquals(1, offsetsForBatch1.size());
        Assert.assertTrue(offsetsForBatch1.contains(createOffsetNode(newMessage)));
    }

    @Test
    public void shouldReturnCommittableOffset() {
        OffsetManager manger = new OffsetManager();
        //Path1
        Message message1 = createMessage("topic1", 1, 1);
        Message message2 = createMessage("topic1", 1, 2);
        Message message3 = createMessage("topic2", 1, 1);
        Message message4 = createMessage("topic2", 1, 2);
        Message message5 = createMessage("topic1", 1, 3);
        Message message6 = createMessage("topic1", 1, 4);

        //Path2
        Message message7 = createMessage("topic2", 1, 3);
        Message message8 = createMessage("topic1", 1, 5);
        Message message9 = createMessage("topic2", 1, 4);
        Message message10 = createMessage("topic1", 1, 6);


        //Path3
        Message message11 = createMessage("topic1", 1, 7);
        Message message12 = createMessage("topic1", 1, 8);
        Message message13 = createMessage("topic1", 1, 9);
        Message message14 = createMessage("topic2", 1, 5);
        Message message15 = createMessage("topic2", 1, 6);
        Message message16 = createMessage("topic3", 1, 1);


        List<Message> messageList1 = new ArrayList<Message>() {{
            add(message3);
            add(message5);
            add(message4);
            add(message1);
            add(message2);
            add(message6);
        }};
        List<Message> messageList2 = new ArrayList<Message>() {{
            add(message7);
            add(message10);
            add(message8);
            add(message9);
        }};

        List<Message> messageList3 = new ArrayList<Message>() {{
            add(message11);
            add(message12);
            add(message13);
            add(message14);
            add(message15);
            add(message16);
        }};


        OffsetBatchKey key1 = new OffsetBatchKey("test1", 10);
        messageList1.forEach(message -> manger.addOffsetToBatch(key1, message));

        OffsetBatchKey key2 = new OffsetBatchKey("test2", 10);
        messageList2.forEach(message -> manger.addOffsetToBatch(key2, message));

        OffsetBatchKey key3 = new OffsetBatchKey("test3", 100);
        messageList3.forEach(message -> manger.addOffsetToBatch(key3, message));

        manger.setCommittable(key2);
        Map<TopicPartition, OffsetAndMetadata> committableOffset = manger.getCommittableOffset();
        Assert.assertTrue(committableOffset.isEmpty());

        manger.setCommittable(key1);
        committableOffset = manger.getCommittableOffset();
        Assert.assertEquals(2, committableOffset.size());

        Assert.assertEquals(new OffsetAndMetadata(7), committableOffset.get(new TopicPartition("topic1", 1)));
        Assert.assertEquals(new OffsetAndMetadata(5), committableOffset.get(new TopicPartition("topic2", 1)));

        // Calling getCommittableOffset() should return the same value if nothing is committed in between calls.
        committableOffset = manger.getCommittableOffset();
        Assert.assertEquals(2, committableOffset.size());

        Assert.assertEquals(new OffsetAndMetadata(7), committableOffset.get(new TopicPartition("topic1", 1)));
        Assert.assertEquals(new OffsetAndMetadata(5), committableOffset.get(new TopicPartition("topic2", 1)));


        manger.setCommittable(key3);
        committableOffset = manger.getCommittableOffset();
        Assert.assertEquals(3, committableOffset.size());

        Assert.assertEquals(new OffsetAndMetadata(10), committableOffset.get(new TopicPartition("topic1", 1)));
        Assert.assertEquals(new OffsetAndMetadata(7), committableOffset.get(new TopicPartition("topic2", 1)));
        Assert.assertEquals(new OffsetAndMetadata(2), committableOffset.get(new TopicPartition("topic3", 1)));

    }

    @EqualsAndHashCode
    @Data
    @AllArgsConstructor
    private static class OffsetBatchKey {
        private String data;
        private int integerData;
    }
}
