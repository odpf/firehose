package com.gotocompany.firehose.sink.blob;

import com.google.protobuf.DynamicMessage;
import com.gotocompany.depot.error.ErrorType;
import com.gotocompany.firehose.TestMessageBQ;
import com.gotocompany.firehose.consumer.kafka.OffsetManager;
import com.gotocompany.firehose.message.Message;
import com.gotocompany.firehose.exception.DeserializerException;
import com.gotocompany.firehose.exception.EmptyMessageException;
import com.gotocompany.firehose.exception.UnknownFieldsException;
import com.gotocompany.firehose.exception.SinkException;
import com.gotocompany.firehose.metrics.FirehoseInstrumentation;
import com.gotocompany.firehose.sink.blob.message.MessageDeSerializer;
import com.gotocompany.firehose.sink.blob.message.Record;
import com.gotocompany.firehose.sink.blob.writer.WriterOrchestrator;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class BlobSinkTest {

    @Mock
    private WriterOrchestrator writerOrchestrator;

    @Mock
    private FirehoseInstrumentation firehoseInstrumentation;

    @Mock
    private MessageDeSerializer messageDeSerializer;

    private BlobSink blobSink;

    private OffsetManager offsetManager;

    @Before
    public void setUp() throws Exception {
        offsetManager = new OffsetManager();
        blobSink = new BlobSink(firehoseInstrumentation, "objectstorage", offsetManager, writerOrchestrator, messageDeSerializer);
    }

    @Test
    public void shouldWriteRecords() throws Exception {
        Message message1 = new Message("".getBytes(), "".getBytes(), "booking", 1, 1);
        Message message2 = new Message("".getBytes(), "".getBytes(), "booking", 1, 2);
        Record record1 = mock(Record.class);
        Record record2 = mock(Record.class);
        String path1 = "/tmp/test1";
        String path2 = "/tmp/test2";

        when(messageDeSerializer.deSerialize(message1)).thenReturn(record1);
        when(messageDeSerializer.deSerialize(message2)).thenReturn(record2);
        when(writerOrchestrator.write(record1)).thenReturn(path1);
        when(writerOrchestrator.write(record2)).thenReturn(path2);

        List<Message> retryMessages = blobSink.pushMessage(Arrays.asList(message1, message2));

        verify(writerOrchestrator, times(2)).write(any(Record.class));
        assertEquals(0, retryMessages.size());
    }

    @Test(expected = SinkException.class)
    public void shouldThrowWriterIOExceptionWhenWritingRecordThrowIOException() throws Exception {
        Message message1 = new Message("".getBytes(), "".getBytes(), "booking", 1, 1);
        Record record1 = mock(Record.class);
        when(messageDeSerializer.deSerialize(message1)).thenReturn(record1);
        when(writerOrchestrator.write(record1)).thenThrow(new IOException("error"));

        blobSink.pushMessage(Collections.singletonList(message1));
    }

    @Test
    public void shouldReturnCommittableOffsets() throws Exception {
        Message message1 = new Message("".getBytes(), "".getBytes(), "booking", 1, 1);
        Message message2 = new Message("".getBytes(), "".getBytes(), "booking", 1, 2);
        Record record1 = mock(Record.class);
        Record record2 = mock(Record.class);
        String path1 = "/tmp/test1";

        when(messageDeSerializer.deSerialize(message1)).thenReturn(record1);
        when(messageDeSerializer.deSerialize(message2)).thenReturn(record2);
        when(writerOrchestrator.write(record1)).thenReturn(path1);
        when(writerOrchestrator.write(record2)).thenReturn(path1);
        when(writerOrchestrator.getFlushedPaths()).thenReturn(new HashSet<>());
        blobSink.pushMessage(Arrays.asList(message1, message2));

        assertTrue(blobSink.canManageOffsets());

        when(writerOrchestrator.getFlushedPaths()).thenReturn(new HashSet<String>() {{
            add(path1);
        }});
        blobSink.calculateCommittableOffsets();
        Map<TopicPartition, OffsetAndMetadata> committableOffsets = offsetManager.getCommittableOffset();
        assertEquals(1, committableOffsets.size());
        assertEquals(new OffsetAndMetadata(3), committableOffsets.get(new TopicPartition("booking", 1)));
    }

    @Test
    public void shouldReturnMessageThatCausedDeserializerException() throws Exception {
        blobSink = new BlobSink(firehoseInstrumentation, "objectstorage", new OffsetManager(), writerOrchestrator, messageDeSerializer);

        Message message1 = new Message("".getBytes(), "".getBytes(), "booking", 1, 1);
        Message message2 = new Message("".getBytes(), "".getBytes(), "booking", 1, 2);
        Message message3 = new Message("".getBytes(), "".getBytes(), "booking", 1, 3);
        Message message4 = new Message("".getBytes(), "".getBytes(), "booking", 1, 4);
        Message message5 = new Message("".getBytes(), "".getBytes(), "booking", 2, 1);
        Message message6 = new Message("".getBytes(), "".getBytes(), "booking", 2, 2);
        Record record1 = mock(Record.class);
        Record record2 = mock(Record.class);
        String path1 = "/tmp/test1";
        String path2 = "/tmp/test2";

        when(messageDeSerializer.deSerialize(message1)).thenReturn(record1);
        when(messageDeSerializer.deSerialize(message2)).thenReturn(record2);
        when(messageDeSerializer.deSerialize(message3)).thenThrow(new DeserializerException(""));
        when(messageDeSerializer.deSerialize(message4)).thenThrow(new DeserializerException(""));
        when(messageDeSerializer.deSerialize(message5)).thenThrow(new DeserializerException(""));
        when(messageDeSerializer.deSerialize(message6)).thenThrow(new DeserializerException(""));
        when(writerOrchestrator.write(record1)).thenReturn(path1);
        when(writerOrchestrator.write(record2)).thenReturn(path2);

        List<Message> retryMessages = blobSink.pushMessage(Arrays.asList(message1, message2, message3, message4, message5, message6));

        verify(writerOrchestrator, times(2)).write(any(Record.class));
        assertEquals(4, retryMessages.size());
        retryMessages.forEach(message -> assertNotNull(message.getErrorInfo()));
    }

    @Test
    public void shouldManageOffset() {
        TopicPartition topicPartition1 = new TopicPartition("booking", 1);
        TopicPartition topicPartition2 = new TopicPartition("booking", 2);
        TopicPartition topicPartition3 = new TopicPartition("profile", 1);

        Message message1 = new Message("".getBytes(), "".getBytes(), "booking", 1, 1);
        Message message2 = new Message("".getBytes(), "".getBytes(), "booking", 1, 2);
        Message message3 = new Message("".getBytes(), "".getBytes(), "booking", 2, 1);
        Message message4 = new Message("".getBytes(), "".getBytes(), "booking", 2, 2);
        Message message5 = new Message("".getBytes(), "".getBytes(), "profile", 1, 5);
        Message message6 = new Message("".getBytes(), "".getBytes(), "profile", 1, 6);

        List<Message> messages = Arrays.asList(message1, message2, message3, message4, message5, message6);

        HashMap<TopicPartition, OffsetAndMetadata> offsetAndMetadataHashMap = new HashMap<>();
        offsetAndMetadataHashMap.put(topicPartition1, new OffsetAndMetadata(3));
        offsetAndMetadataHashMap.put(topicPartition2, new OffsetAndMetadata(3));
        offsetAndMetadataHashMap.put(topicPartition3, new OffsetAndMetadata(7));

        blobSink.addOffsetsAndSetCommittable(messages);
        blobSink.calculateCommittableOffsets();
        Map<TopicPartition, OffsetAndMetadata> result = offsetManager.getCommittableOffset();
        assertEquals(offsetAndMetadataHashMap, result);
    }

    @Test
    public void shouldReturnMessagesWhenMessagesHasErrorCausedByEmptyMessageException() {
        blobSink = new BlobSink(firehoseInstrumentation, "objectstorage", new OffsetManager(), writerOrchestrator, messageDeSerializer);

        Message message1 = new Message("".getBytes(), "".getBytes(), "booking", 2, 1);
        Message message2 = new Message("".getBytes(), "".getBytes(), "booking", 2, 2);

        when(messageDeSerializer.deSerialize(message1)).thenThrow(new EmptyMessageException());

        List<Message> retryMessages = blobSink.pushMessage(Arrays.asList(message1, message2));

        assertEquals(retryMessages.size(), 1);
        assertEquals(ErrorType.INVALID_MESSAGE_ERROR, retryMessages.get(0).getErrorInfo().getErrorType());
        retryMessages.forEach(message -> assertNotNull(message.getErrorInfo()));
    }

    @Test
    public void shouldReturnMessagesWhenMessagesHasErrorCausedByUnknownFields() {
        blobSink = new BlobSink(firehoseInstrumentation, "objectstorage", new OffsetManager(), writerOrchestrator, messageDeSerializer);

        Message message1 = new Message("".getBytes(), "".getBytes(), "booking", 2, 1);
        Message message2 = new Message("".getBytes(), "".getBytes(), "booking", 2, 2);

        when(messageDeSerializer.deSerialize(message1)).thenThrow(new UnknownFieldsException(DynamicMessage.newBuilder(TestMessageBQ.getDescriptor()).build()));

        List<Message> retryMessages = blobSink.pushMessage(Arrays.asList(message1, message2));

        assertEquals(retryMessages.size(), 1);
        assertEquals(ErrorType.UNKNOWN_FIELDS_ERROR, retryMessages.get(0).getErrorInfo().getErrorType());
        retryMessages.forEach(message -> assertNotNull(message.getErrorInfo()));
    }
}
