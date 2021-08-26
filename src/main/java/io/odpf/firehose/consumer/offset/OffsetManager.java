package io.odpf.firehose.consumer.offset;

import io.odpf.firehose.consumer.Message;
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
 * OffsetManager keeps tracks of all offsets that can be committed to kafka.
 */
public class OffsetManager {
    private final Map<Object, Set<OffsetNode>> toBeCommittableBatchOffsets = new HashMap<>();
    private final Map<TopicPartition, TreeSet<OffsetNode>> sortedOffsets = new HashMap<>();

    /**
     * @param batch   key for which this offset belongs to.
     * @param message message to extract offset metadata.
     */
    public void addOffsetToBatch(Object batch, Message message) {
        OffsetNode currentNode = new OffsetNode(
                new TopicPartition(message.getTopic(), message.getPartition()),
                new OffsetAndMetadata(message.getOffset() + 1));
        addOffsetToBatch(batch, currentNode);
    }

    public void addOffsetToBatch(Object batch, List<Message> messageList) {
        messageList.forEach(m -> addOffsetToBatch(batch, m));
    }

    private void addOffsetToBatch(Object batch, OffsetNode node) {
        toBeCommittableBatchOffsets.computeIfAbsent(batch, x -> new HashSet<>()).add(node);
        sortedOffsets.computeIfAbsent(
                node.getTopicPartition(),
                topicPartition -> new TreeSet<>(Comparator.comparingLong(offsetNode -> offsetNode.getOffsetAndMetadata().offset()))).add(node);
    }

    public Set<OffsetNode> getOffsetsForBatch(Object key) {
        return toBeCommittableBatchOffsets.get(key);
    }

    /**
     * @param batch key for which all offsets can be committed.
     *              Removes the batch from the global map for the cleanup.
     */
    public void setCommittable(Object batch) {
        toBeCommittableBatchOffsets.get(batch).forEach(offsetNode -> offsetNode.setCommittable(true));
        toBeCommittableBatchOffsets.remove(batch);
    }

    public TreeSet<OffsetNode> getOffsetsForTopicPartition(TopicPartition topicPartition) {
        return sortedOffsets.get(topicPartition);
    }

    /**
     * @return offsets for all partitions
     * It also compact internal sorted list per partition by removing redundant offsets.
     */
    public Map<TopicPartition, OffsetAndMetadata> getCommittableOffset() {
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
    public Optional<OffsetNode> compactAndFetchFirstCommittableNode(TreeSet<OffsetNode> nodes) {
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
        nodes.removeIf(OffsetNode::isRemovable);
        return nodes.first().isCommittable() ? Optional.of(nodes.first()) : Optional.empty();
    }
}
