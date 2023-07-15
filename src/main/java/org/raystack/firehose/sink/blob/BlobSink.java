package org.raystack.firehose.sink.blob;

import org.raystack.firehose.consumer.kafka.OffsetManager;
import org.raystack.firehose.exception.DeserializerException;
import org.raystack.firehose.exception.EmptyMessageException;
import org.raystack.firehose.exception.SinkException;
import org.raystack.firehose.exception.UnknownFieldsException;
import org.raystack.firehose.message.Message;
import org.raystack.firehose.metrics.FirehoseInstrumentation;
import org.raystack.firehose.sink.blob.message.MessageDeSerializer;
import org.raystack.firehose.sink.blob.message.Record;
import org.raystack.firehose.sink.blob.writer.WriterOrchestrator;
import org.raystack.depot.error.ErrorInfo;
import org.raystack.depot.error.ErrorType;
import org.raystack.firehose.sink.AbstractSink;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class BlobSink extends AbstractSink {

    private final WriterOrchestrator writerOrchestrator;
    private final OffsetManager offsetManager;
    private final MessageDeSerializer messageDeSerializer;

    private List<Message> messages;

    public BlobSink(FirehoseInstrumentation firehoseInstrumentation, String sinkType, OffsetManager offsetManager, WriterOrchestrator writerOrchestrator, MessageDeSerializer messageDeSerializer) {
        super(firehoseInstrumentation, sinkType);
        this.offsetManager = offsetManager;
        this.writerOrchestrator = writerOrchestrator;
        this.messageDeSerializer = messageDeSerializer;
    }

    @Override
    protected List<Message> execute() throws Exception {
        List<Message> failedMessages = new LinkedList<>();
        Map<Object, List<Message>> fileToMessages = new HashMap<>();
        for (Message message : messages) {
            try {
                Record record = messageDeSerializer.deSerialize(message);
                String filePath = writerOrchestrator.write(record);
                fileToMessages.computeIfAbsent(filePath, key -> new ArrayList<>()).add(message);
            } catch (EmptyMessageException e) {
                getFirehoseInstrumentation().logWarn("empty message found on topic: {}, partition: {}, offset: {}",
                        message.getTopic(), message.getPartition(), message.getOffset());
                message.setErrorInfo(new ErrorInfo(e, ErrorType.INVALID_MESSAGE_ERROR));
                failedMessages.add(message);
            } catch (UnknownFieldsException e) {
                getFirehoseInstrumentation().logWarn(e.getMessage());
                message.setErrorInfo(new ErrorInfo(e, ErrorType.UNKNOWN_FIELDS_ERROR));
                failedMessages.add(message);
            } catch (DeserializerException e) {
                getFirehoseInstrumentation().logWarn("message deserialization failed on topic: {}, partition: {}, offset: {}, reason: {}",
                        message.getTopic(), message.getPartition(), message.getOffset(), e.getMessage());
                message.setErrorInfo(new ErrorInfo(e, ErrorType.DESERIALIZATION_ERROR));
                failedMessages.add(message);
            } catch (Exception e) {
                throw new SinkException("Failed to deserialize the message", e);
            }
        }
        offsetManager.addOffsetToBatch(fileToMessages);
        return failedMessages;
    }

    @Override
    protected void prepare(List<Message> messageList) throws IOException, SQLException {
        this.messages = messageList;
    }

    @Override
    public void close() throws IOException {
        writerOrchestrator.close();
    }

    @Override
    public void calculateCommittableOffsets() {
        writerOrchestrator.getFlushedPaths().forEach(offsetManager::setCommittable);
    }

    @Override
    public boolean canManageOffsets() {
        return true;
    }

    @Override
    public void addOffsetsAndSetCommittable(List<Message> messageList) {
        offsetManager.addOffsetsAndSetCommittable(messageList);
    }
}
