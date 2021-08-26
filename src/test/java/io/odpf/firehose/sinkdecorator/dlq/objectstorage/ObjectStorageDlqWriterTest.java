package io.odpf.firehose.sinkdecorator.dlq.objectstorage;

import io.odpf.firehose.error.ErrorInfo;
import io.odpf.firehose.error.ErrorType;
import io.odpf.firehose.consumer.Message;
import io.odpf.firehose.exception.DeserializerException;
import io.odpf.firehose.objectstorage.ObjectStorageException;
import io.odpf.firehose.objectstorage.ObjectStorage;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class ObjectStorageDlqWriterTest {

    @Mock
    private ObjectStorage objectStorage;

    private ObjectStorageDlqWriter objectStorageDLQWriter;

    @Before
    public void setUp() throws Exception {
        objectStorageDLQWriter = new ObjectStorageDlqWriter(objectStorage);
    }

    @Test
    public void shouldWriteMessagesWithoutErrorInfoToObjectStorage() throws IOException, ObjectStorageException {
        long timestamp1 = Instant.parse("2020-01-01T00:00:00Z").toEpochMilli();
        Message message1 = new Message("123".getBytes(), "abc".getBytes(), "booking", 1, 1, null, 0, timestamp1);
        Message message2 = new Message("123".getBytes(), "abc".getBytes(), "booking", 1, 2, null, 0, timestamp1);

        long timestamp2 = Instant.parse("2020-01-02T00:00:00Z").toEpochMilli();
        Message message3 = new Message("123".getBytes(), "abc".getBytes(), "booking", 1, 3, null, 0, timestamp2);
        Message message4 = new Message("123".getBytes(), "abc".getBytes(), "booking", 1, 4, null, 0, timestamp2);

        List<Message> messages = Arrays.asList(message1, message2, message3, message4);
        objectStorageDLQWriter.write(messages);

        verify(objectStorage).store(contains("booking/2020-01-02"),
                eq(("{\"key\":\"123\",\"value\":\"abc\",\"topic\":\"booking\",\"partition\":1,\"offset\":3,\"timestamp\":1577923200000,\"error\":\"\"}\n"
                        + "{\"key\":\"123\",\"value\":\"abc\",\"topic\":\"booking\",\"partition\":1,\"offset\":4,\"timestamp\":1577923200000,\"error\":\"\"}").getBytes()));
        verify(objectStorage).store(contains("booking/2020-01-01"),
                eq(("{\"key\":\"123\",\"value\":\"abc\",\"topic\":\"booking\",\"partition\":1,\"offset\":1,\"timestamp\":1577836800000,\"error\":\"\"}\n"
                        + "{\"key\":\"123\",\"value\":\"abc\",\"topic\":\"booking\",\"partition\":1,\"offset\":2,\"timestamp\":1577836800000,\"error\":\"\"}").getBytes()));
    }

    @Test
    public void shouldWriteMessageErrorTypesToObjectStorage() throws IOException, ObjectStorageException {
        long timestamp1 = Instant.parse("2020-01-01T00:00:00Z").toEpochMilli();
        Message message1 = new Message("123".getBytes(), "abc".getBytes(), "booking", 1, 1, null, 0, timestamp1, new ErrorInfo(new DeserializerException(""), ErrorType.DESERIALIZATION_ERROR));
        Message message2 = new Message("123".getBytes(), "abc".getBytes(), "booking", 1, 2, null, 0, timestamp1, new ErrorInfo(new NullPointerException(), ErrorType.SINK_UNKNOWN_ERROR));

        long timestamp2 = Instant.parse("2020-01-02T00:00:00Z").toEpochMilli();
        Message message3 = new Message("123".getBytes(), "abc".getBytes(), "booking", 1, 3, null, 0, timestamp2, new ErrorInfo(new DeserializerException(""), ErrorType.DESERIALIZATION_ERROR));
        Message message4 = new Message("123".getBytes(), "abc".getBytes(), "booking", 1, 4, null, 0, timestamp2, new ErrorInfo(new DeserializerException(""), ErrorType.SINK_UNKNOWN_ERROR));

        List<Message> messages = Arrays.asList(message1, message2, message3, message4);
        objectStorageDLQWriter.write(messages);

        verify(objectStorage).store(contains("booking/2020-01-02"),
                eq(("{\"key\":\"123\",\"value\":\"abc\",\"topic\":\"booking\",\"partition\":1,\"offset\":3,\"timestamp\":1577923200000,\"error\":\"DESERIALIZATION_ERROR\"}\n"
                        + "{\"key\":\"123\",\"value\":\"abc\",\"topic\":\"booking\",\"partition\":1,\"offset\":4,\"timestamp\":1577923200000,\"error\":\"SINK_UNKNOWN_ERROR\"}").getBytes()));
        verify(objectStorage).store(contains("booking/2020-01-01"),
                eq(("{\"key\":\"123\",\"value\":\"abc\",\"topic\":\"booking\",\"partition\":1,\"offset\":1,\"timestamp\":1577836800000,\"error\":\"DESERIALIZATION_ERROR\"}\n"
                        + "{\"key\":\"123\",\"value\":\"abc\",\"topic\":\"booking\",\"partition\":1,\"offset\":2,\"timestamp\":1577836800000,\"error\":\"SINK_UNKNOWN_ERROR\"}").getBytes()));
    }

    @Test(expected = IOException.class)
    public void shouldThrowIOExceptionWhenWriteFileThrowIOException() throws IOException, ObjectStorageException {
        long timestamp1 = Instant.parse("2020-01-01T00:00:00Z").toEpochMilli();
        Message message1 = new Message("123".getBytes(), "abc".getBytes(), "booking", 1, 1, null, 0, timestamp1);
        Message message2 = new Message("123".getBytes(), "abc".getBytes(), "booking", 1, 2, null, 0, timestamp1);

        long timestamp2 = Instant.parse("2020-01-02T00:00:00Z").toEpochMilli();
        Message message3 = new Message("123".getBytes(), "abc".getBytes(), "booking", 1, 3, null, 0, timestamp2);
        Message message4 = new Message("123".getBytes(), "abc".getBytes(), "booking", 1, 4, null, 0, timestamp2);

        doThrow(new ObjectStorageException("", "", new IOException())).when(objectStorage).store(anyString(), any());

        List<Message> messages = Arrays.asList(message1, message2, message3, message4);
        objectStorageDLQWriter.write(messages);
    }
}
