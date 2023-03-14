package com.gotocompany.firehose.sink.dlq;

import com.gotocompany.depot.error.ErrorInfo;
import com.gotocompany.depot.error.ErrorType;
import com.gotocompany.firehose.message.Message;
import com.gotocompany.firehose.metrics.FirehoseInstrumentation;
import com.gotocompany.firehose.sink.dlq.log.LogDlqWriter;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import java.util.List;

@RunWith(MockitoJUnitRunner.class)
public class LogDlqWriterTest {

    @Mock
    private FirehoseInstrumentation firehoseInstrumentation;

    private LogDlqWriter logDlqWriter;

    @Before
    public void setUp() throws Exception {
        logDlqWriter = new LogDlqWriter(firehoseInstrumentation);
    }

    @Test
    public void shouldWriteMessagesToLog() throws IOException {
        long timestamp = Instant.parse("2020-01-01T00:00:00Z").toEpochMilli();
        Message message = new Message("123".getBytes(), "abc".getBytes(), "booking", 1, 1, null, timestamp, timestamp, new ErrorInfo(new IOException("test"), ErrorType.DESERIALIZATION_ERROR));

        String key = new String(message.getLogKey());
        String value = new String(message.getLogMessage());
        ErrorInfo errorInfo = message.getErrorInfo();
        String error = ExceptionUtils.getStackTrace(errorInfo.getException());

        List<Message> messages = Collections.singletonList(message);
        Assert.assertEquals(0, logDlqWriter.write(messages).size());

        Mockito.verify(firehoseInstrumentation, Mockito.times(1)).logInfo("key: {}\nvalue: {}\nerror: {}", key, value, error);
    }

    @Test
    public void shouldWriteMessagesToLogWhenKeyIsNull() throws IOException {
        long timestamp = Instant.parse("2020-01-01T00:00:00Z").toEpochMilli();
        Message message = new Message(null, "abc".getBytes(), "booking", 1, 1, null, timestamp, timestamp, new ErrorInfo(new IOException("test"), ErrorType.DESERIALIZATION_ERROR));

        String value = new String(message.getLogMessage());
        ErrorInfo errorInfo = message.getErrorInfo();
        String error = ExceptionUtils.getStackTrace(errorInfo.getException());

        List<Message> messages = Collections.singletonList(message);
        Assert.assertEquals(0, logDlqWriter.write(messages).size());

        Mockito.verify(firehoseInstrumentation, Mockito.times(1)).logInfo("key: {}\nvalue: {}\nerror: {}", "", value, error);
    }

    @Test
    public void shouldWriteMessagesToLogWhenValueIsNull() throws IOException {
        long timestamp = Instant.parse("2020-01-01T00:00:00Z").toEpochMilli();
        Message message = new Message("123".getBytes(), null, "booking", 1, 1, null, timestamp, timestamp, new ErrorInfo(new IOException("test"), ErrorType.DESERIALIZATION_ERROR));

        String key = new String(message.getLogKey());
        ErrorInfo errorInfo = message.getErrorInfo();
        String error = ExceptionUtils.getStackTrace(errorInfo.getException());

        List<Message> messages = Collections.singletonList(message);
        Assert.assertEquals(0, logDlqWriter.write(messages).size());

        Mockito.verify(firehoseInstrumentation, Mockito.times(1)).logInfo("key: {}\nvalue: {}\nerror: {}", key, "", error);
    }

    @Test
    public void shouldWriteMessagesToLogWhenErrorInfoIsNull() throws IOException {
        long timestamp = Instant.parse("2020-01-01T00:00:00Z").toEpochMilli();
        Message message = new Message("123".getBytes(), "abc".getBytes(), "booking", 1, 1, null, timestamp, timestamp, null);

        String key = new String(message.getLogKey());
        String value = new String(message.getLogMessage());

        List<Message> messages = Collections.singletonList(message);
        Assert.assertEquals(0, logDlqWriter.write(messages).size());

        Mockito.verify(firehoseInstrumentation, Mockito.times(1)).logInfo("key: {}\nvalue: {}\nerror: {}", key, value, "");
    }

    @Test
    public void shouldWriteMessagesToLogWhenErrorInfoExceptionIsNull() throws IOException {
        long timestamp = Instant.parse("2020-01-01T00:00:00Z").toEpochMilli();
        Message message = new Message("123".getBytes(), "abc".getBytes(), "booking", 1, 1, null, timestamp, timestamp, new ErrorInfo(null, ErrorType.DESERIALIZATION_ERROR));

        String key = new String(message.getLogKey());
        String value = new String(message.getLogMessage());

        List<Message> messages = Collections.singletonList(message);
        Assert.assertEquals(0, logDlqWriter.write(messages).size());

        Mockito.verify(firehoseInstrumentation, Mockito.times(1)).logInfo("key: {}\nvalue: {}\nerror: {}", key, value, "");
    }
}
