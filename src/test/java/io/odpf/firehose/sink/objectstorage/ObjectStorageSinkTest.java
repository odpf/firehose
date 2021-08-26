package io.odpf.firehose.sink.objectstorage;

import com.google.protobuf.DynamicMessage;
import io.odpf.firehose.TestMessageBQ;
import io.odpf.firehose.consumer.Message;
import io.odpf.firehose.error.ErrorType;
import io.odpf.firehose.exception.DeserializerException;
import io.odpf.firehose.sink.exception.EmptyMessageException;
import io.odpf.firehose.sink.exception.UnknownFieldsException;
import io.odpf.firehose.exception.SinkException;
import io.odpf.firehose.metrics.Instrumentation;
import io.odpf.firehose.sink.objectstorage.message.MessageDeSerializer;
import io.odpf.firehose.sink.objectstorage.message.Record;
import io.odpf.firehose.sink.objectstorage.writer.WriterOrchestrator;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class ObjectStorageSinkTest {

    @Mock
    private WriterOrchestrator writerOrchestrator;

    @Mock
    private Instrumentation instrumentation;

    @Mock
    private MessageDeSerializer messageDeSerializer;

    private ObjectStorageSink objectStorageSink;

    @Before
    public void setUp() throws Exception {
        objectStorageSink = new ObjectStorageSink(instrumentation, "objectstorage", writerOrchestrator, messageDeSerializer);
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

        List<Message> retryMessages = objectStorageSink.pushMessage(Arrays.asList(message1, message2));

        verify(writerOrchestrator, times(2)).write(any(Record.class));
        assertEquals(0, retryMessages.size());
    }

    @Test(expected = SinkException.class)
    public void shouldThrowWriterIOExceptionWhenWritingRecordThrowIOException() throws Exception {
        Message message1 = new Message("".getBytes(), "".getBytes(), "booking", 1, 1);
        Record record1 = mock(Record.class);
        when(messageDeSerializer.deSerialize(message1)).thenReturn(record1);
        when(writerOrchestrator.write(record1)).thenThrow(new IOException("error"));

        objectStorageSink.pushMessage(Collections.singletonList(message1));
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
        objectStorageSink.pushMessage(Arrays.asList(message1, message2));

        assertTrue(objectStorageSink.canManageOffsets());
        assertEquals(0, objectStorageSink.getCommittableOffsets().size());

        when(writerOrchestrator.getFlushedPaths()).thenReturn(new HashSet<String>() {{
            add(path1);
        }});
        Map<TopicPartition, OffsetAndMetadata> committableOffsets = objectStorageSink.getCommittableOffsets();
        assertEquals(1, committableOffsets.size());
        assertEquals(new OffsetAndMetadata(3), committableOffsets.get(new TopicPartition("booking", 1)));
    }

    @Test
    public void shouldReturnMessageThatCausedDeserializerException() throws Exception {
        objectStorageSink = new ObjectStorageSink(instrumentation, "objectstorage", writerOrchestrator, messageDeSerializer);

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

        List<Message> retryMessages = objectStorageSink.pushMessage(Arrays.asList(message1, message2, message3, message4, message5, message6));

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

        objectStorageSink.addOffsets("key", messages);
        objectStorageSink.setCommittable("key");

        Map<TopicPartition, OffsetAndMetadata> result = objectStorageSink.getCommittableOffsets();

        assertEquals(offsetAndMetadataHashMap, result);
    }

    @Test
    public void shouldReturnMessagesWhenMessagesHasErrorCausedByEmptyMessageException() {
        objectStorageSink = new ObjectStorageSink(instrumentation, "objectstorage", writerOrchestrator, messageDeSerializer);

        Message message1 = new Message("".getBytes(), "".getBytes(), "booking", 2, 1);
        Message message2 = new Message("".getBytes(), "".getBytes(), "booking", 2, 2);

        when(messageDeSerializer.deSerialize(message1)).thenThrow(new EmptyMessageException());

        List<Message> retryMessages = objectStorageSink.pushMessage(Arrays.asList(message1, message2));

        assertEquals(retryMessages.size(), 1);
        assertEquals(ErrorType.INVALID_MESSAGE_ERROR, retryMessages.get(0).getErrorInfo().getErrorType());
        retryMessages.forEach(message -> assertNotNull(message.getErrorInfo()));
    }

    @Test
    public void shouldReturnMessagesWhenMessagesHasErrorCausedByUnknownFields() {
        objectStorageSink = new ObjectStorageSink(instrumentation, "objectstorage", writerOrchestrator, messageDeSerializer);

        Message message1 = new Message("".getBytes(), "".getBytes(), "booking", 2, 1);
        Message message2 = new Message("".getBytes(), "".getBytes(), "booking", 2, 2);

        when(messageDeSerializer.deSerialize(message1)).thenThrow(new UnknownFieldsException(DynamicMessage.newBuilder(TestMessageBQ.getDescriptor()).build()));

        List<Message> retryMessages = objectStorageSink.pushMessage(Arrays.asList(message1, message2));

        assertEquals(retryMessages.size(), 1);
        assertEquals(ErrorType.UNKNOWN_FIELDS_ERROR, retryMessages.get(0).getErrorInfo().getErrorType());
        retryMessages.forEach(message -> assertNotNull(message.getErrorInfo()));
    }
}
