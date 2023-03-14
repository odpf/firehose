package com.gotocompany.firehose.consumer.kafka;

import com.gotocompany.firehose.message.Message;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * OffsetManager is a data structure which keeps tracks of all offsets that can be committed to kafka.
 * <p>
 * This class is thread safe. Multiple sinks can use the same object.
 */
public class OffsetManager {
    private final Map<Object, Set<OffsetNode>> toBeCommittableBatchOffsets = new HashMap<>();
    private final Map<TopicPartition, TreeSet<OffsetNode>> sortedOffsets = new HashMap<>();

    /**
     * @param offsetKeyToMessagesMap A map of key to list of messages to be added
     */
    public synchronized void addOffsetToBatch(Map<Object, List<Message>> offsetKeyToMessagesMap) {
        offsetKeyToMessagesMap.forEach(this::addOffsetToBatch);
    }

    public synchronized void addOffsetsAndSetCommittable(List<Message> messageList) {
        String syncBatchKey = "sync_batch_key";
        addOffsetToBatch(syncBatchKey, messageList);
        setCommittable(syncBatchKey);
    }

    public synchronized void addOffsetToBatch(Object batch, List<Message> messageList) {
        messageList.forEach(m -> addOffsetToBatch(batch, m));
    }

    /**
     * @param batch   key for which this offset belongs to.
     * @param message message to extract offset metadata.
     */
    public synchronized void addOffsetToBatch(Object batch, Message message) {
        OffsetNode currentNode = new OffsetNode(
                new TopicPartition(message.getTopic(), message.getPartition()),
                new OffsetAndMetadata(message.getOffset() + 1));
        addOffsetToBatch(batch, currentNode);
    }

    private synchronized void addOffsetToBatch(Object batch, OffsetNode node) {
        toBeCommittableBatchOffsets.computeIfAbsent(batch, x -> new HashSet<>()).add(node);
        sortedOffsets.computeIfAbsent(
                node.getTopicPartition(),
                topicPartition -> new TreeSet<>(Comparator.comparingLong(offsetNode -> offsetNode.getOffsetAndMetadata().offset()))).add(node);
    }

    /**
     * @param batch key for which all offsets can be committed.
     *              Removes the batch from the global map for the cleanup.
     */
    public synchronized void setCommittable(Object batch) {
        toBeCommittableBatchOffsets.getOrDefault(batch, new HashSet<>()).forEach(offsetNode -> offsetNode.setCommittable(true));
        toBeCommittableBatchOffsets.remove(batch);
    }

    /**
     * @return offsets for all partitions
     * It also compact internal sorted list per partition by removing redundant offsets.
     */
    public synchronized Map<TopicPartition, OffsetAndMetadata> getCommittableOffset() {
        return sortedOffsets.entrySet().stream().collect(
                Collectors.toMap(
                        Map.Entry::getKey,
                        kv -> compactAndFetchFirstCommittableNode(kv.getValue())
                )).entrySet().stream().filter(kv -> kv.getValue().isPresent()).collect(
                Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().get().getOffsetAndMetadata()));
    }

    /**
     * @param nodes Sorted List of offsets
     * @return the first offset that is set to be committable just before a non-committable offset in the list.
     */
    protected Optional<OffsetNode> compactAndFetchFirstCommittableNode(TreeSet<OffsetNode> nodes) {
        if (nodes.size() == 0) {
            return Optional.empty();
        }
        Iterator<OffsetNode> iterator = nodes.iterator();
        OffsetNode current = null;
        OffsetNode previous;
        while (iterator.hasNext()) {
            previous = current;
            current = iterator.next();
            if (!current.isCommittable()) {
                break;
            }
            if (previous != null) {
                previous.setRemovable(true);
            }
        }

        // Compact
        iterator = nodes.iterator();
        while (iterator.hasNext()) {
            if (iterator.next().isRemovable()) {
                iterator.remove();
            } else {
                break;
            }
        }
        return nodes.first().isCommittable() ? Optional.of(nodes.first()) : Optional.empty();
    }

    protected TreeSet<OffsetNode> getOffsetsForTopicPartition(TopicPartition topicPartition) {
        return sortedOffsets.get(topicPartition);
    }

    protected Set<OffsetNode> getOffsetsForBatch(Object key) {
        return toBeCommittableBatchOffsets.get(key);
    }


}
