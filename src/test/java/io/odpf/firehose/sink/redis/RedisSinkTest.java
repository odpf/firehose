package io.odpf.firehose.sink.redis;

import io.odpf.firehose.consumer.Message;
import io.odpf.firehose.metrics.Instrumentation;
import io.odpf.firehose.sink.redis.client.RedisClient;
import io.odpf.firehose.sink.redis.exception.NoResponseException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.ArrayList;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class RedisSinkTest {
    @Mock
    private RedisClient redisClient;
    @Mock
    private Instrumentation instrumentation;
    private RedisSink redis;

    @Before
    public void setup() {
        redis = new RedisSink(instrumentation, "redis", redisClient);
    }

    @Test
    public void shouldInvokeExecuteOnTheClient() {
        redis.execute();

        verify(redisClient).execute();
    }

    @Test
    public void shouldInvokePrepareOnTheClient() {
        ArrayList<Message> messages = new ArrayList<>();

        redis.prepare(messages);

        verify(redisClient).prepare(messages);
    }

    @Test
    public void shouldInvokeCloseOnTheClient() {
        redis.close();

        verify(redisClient).close();
    }

    @Test
    public void shouldLogWhenClosingConnection() {
        redis.close();

        verify(instrumentation, times(1)).logInfo("Redis connection closing");
    }

    @Test
    public void sendsMetricsForSuccessMessages() {
        ArrayList<Message> messages = new ArrayList<>();

        redis.pushMessage(messages);

        verify(instrumentation, times(1)).capturePreExecutionLatencies(messages);
        verify(instrumentation, times(1)).startExecution();
        verify(instrumentation, times(1)).logDebug("Preparing {} messages", messages.size());
        verify(instrumentation, times(1)).captureSinkExecutionTelemetry(any(), any());
        InOrder inOrder = inOrder(instrumentation);
        inOrder.verify(instrumentation).logDebug("Preparing {} messages", messages.size());
        inOrder.verify(instrumentation).capturePreExecutionLatencies(messages);
        inOrder.verify(instrumentation).startExecution();
        inOrder.verify(instrumentation).captureSinkExecutionTelemetry(any(), any());
    }

    @Test
    public void sendsMetricsForFailedMessages() {
        when(redisClient.execute()).thenThrow(new NoResponseException());
        ArrayList<Message> messages = new ArrayList<>();

        redis.pushMessage(messages);

        verify(instrumentation, times(1)).capturePreExecutionLatencies(messages);
        verify(instrumentation, times(1)).startExecution();
        verify(instrumentation, times(1)).logDebug("Preparing {} messages", messages.size());
        verify(instrumentation, times(1)).captureSinkExecutionTelemetry(any(), any());
        InOrder inOrder = inOrder(instrumentation);
        inOrder.verify(instrumentation).logDebug("Preparing {} messages", messages.size());
        inOrder.verify(instrumentation).capturePreExecutionLatencies(messages);
        inOrder.verify(instrumentation).startExecution();
        inOrder.verify(instrumentation).captureSinkExecutionTelemetry(any(), any());
    }


}
