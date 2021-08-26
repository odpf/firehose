package io.odpf.firehose.sink.objectstorage;

import io.odpf.firehose.error.ErrorInfo;
import io.odpf.firehose.error.ErrorType;
import io.odpf.firehose.consumer.Message;
import io.odpf.firehose.consumer.offset.OffsetManager;
import io.odpf.firehose.exception.DeserializerException;
import io.odpf.firehose.exception.SinkException;
import io.odpf.firehose.sink.exception.UnknownFieldsException;
import io.odpf.firehose.sink.exception.EmptyMessageException;
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

    private final WriterOrchestrator writerOrchestrator;
    private final OffsetManager offsetManager = new OffsetManager();
    private final MessageDeSerializer messageDeSerializer;

    private List<Message> messages;

    public ObjectStorageSink(Instrumentation instrumentation, String sinkType, WriterOrchestrator writerOrchestrator, MessageDeSerializer messageDeSerializer) {
        super(instrumentation, sinkType);
        this.writerOrchestrator = writerOrchestrator;
        this.messageDeSerializer = messageDeSerializer;
    }

    @Override
    protected List<Message> execute() throws Exception {
        List<Message> failedMessages = new LinkedList<>();
        for (Message message : messages) {
            try {
                Record record = messageDeSerializer.deSerialize(message);
                String filePath = writerOrchestrator.write(record);
                offsetManager.addOffsetToBatch(filePath, message);
            } catch (EmptyMessageException e) {
                getInstrumentation().logWarn("empty message found on topic: {}, partition: {}, offset: {}",
                        message.getTopic(), message.getPartition(), message.getOffset());
                message.setErrorInfo(new ErrorInfo(e, ErrorType.INVALID_MESSAGE_ERROR));
                failedMessages.add(message);
            } catch (UnknownFieldsException e) {
                getInstrumentation().logWarn(e.getMessage());
                message.setErrorInfo(new ErrorInfo(e, ErrorType.UNKNOWN_FIELDS_ERROR));
                failedMessages.add(message);
            } catch (DeserializerException e) {
                getInstrumentation().logWarn("message deserialization failed on topic: {}, partition: {}, offset: {}, reason: {}",
                        message.getTopic(), message.getPartition(), message.getOffset(), e.getMessage());
                message.setErrorInfo(new ErrorInfo(e, ErrorType.DESERIALIZATION_ERROR));
                failedMessages.add(message);
            } catch (Exception e) {
                throw new SinkException(e);
            }
        }

        return failedMessages;
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
    public void addOffsets(Object key, List<Message> messageList) {
        this.offsetManager.addOffsetToBatch(key, messageList);
    }

    @Override
    public void setCommittable(Object key) {
        this.offsetManager.setCommittable(key);
    }
}
