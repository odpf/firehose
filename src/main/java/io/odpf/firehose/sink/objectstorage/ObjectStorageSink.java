package io.odpf.firehose.sink.objectstorage;

import io.odpf.firehose.consumer.ErrorInfo;
import io.odpf.firehose.consumer.ErrorType;
import io.odpf.firehose.consumer.Message;
import io.odpf.firehose.consumer.offset.OffsetManager;
import io.odpf.firehose.exception.DeserializerException;
import io.odpf.firehose.exception.WriterIOException;
import io.odpf.firehose.metrics.Instrumentation;
import io.odpf.firehose.sink.AbstractSink;
import io.odpf.firehose.sink.objectstorage.message.MessageDeSerializer;
import io.odpf.firehose.sink.objectstorage.message.Record;
import io.odpf.firehose.sink.objectstorage.writer.WriterOrchestrator;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.io.IOException;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class ObjectStorageSink extends AbstractSink {

    private final boolean isFailOnDeserializationError;
    private final WriterOrchestrator writerOrchestrator;
    private final MessageDeSerializer messageDeSerializer;
    private final OffsetManager offsetManager;

    private List<Message> messages;

    public ObjectStorageSink(Instrumentation instrumentation, String sinkType, boolean isFailOnDeserializationError, WriterOrchestrator writerOrchestrator, MessageDeSerializer messageDeSerializer) {
        super(instrumentation, sinkType);
        this.isFailOnDeserializationError = isFailOnDeserializationError;
        this.writerOrchestrator = writerOrchestrator;
        this.messageDeSerializer = messageDeSerializer;
        this.offsetManager = new OffsetManager();
    }

    @Override
    public List<Message> execute() throws Exception {
        List<Message> deserializationFailedMessages = new LinkedList<>();
        for (Message message : messages) {
            try {
                Record record = messageDeSerializer.deSerialize(message);
                offsetManager.addOffsetToBatch(writerOrchestrator.write(record), message);
            } catch (DeserializerException e) {
                if (isFailOnDeserializationError) {
                    throw e;
                } else {
                    deserializationFailedMessages.add(new Message(message, new ErrorInfo(e, ErrorType.DESERIALIZATION_ERROR)));
                }
            } catch (Exception e) {
                throw new WriterIOException(e);
            }
        }
        return deserializationFailedMessages;
    }

    @Override
    protected void prepare(List<Message> messageList) throws DeserializerException, IOException, SQLException {
        this.messages = messageList;
    }

    @Override
    public void close() throws IOException {
        writerOrchestrator.close();
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> getCommittableOffsets() {
        writerOrchestrator.getFlushedPaths().forEach(offsetManager::setCommittable);
        return offsetManager.getCommittableOffset();
    }

    @Override
    public boolean canManageOffsets() {
        return true;
    }

    @Override
    public void addOffsets(Object batch, List<Message> messageList) {
        this.offsetManager.addOffsetToBatch(batch, messageList);
    }

    @Override
    public void setCommittable(Object batch) {
        this.offsetManager.setCommittable(batch);
    }
}
