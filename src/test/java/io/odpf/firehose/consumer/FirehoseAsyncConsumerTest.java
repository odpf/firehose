package io.odpf.firehose.consumer;

import io.odpf.firehose.filter.FilterException;
import io.odpf.firehose.metrics.Instrumentation;
import io.odpf.firehose.tracer.SinkTracer;
import io.odpf.firehose.util.Clock;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.Future;

public class FirehoseAsyncConsumerTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Mock
    private SinkPool sinkPool;
    @Mock
    private Clock clock;
    @Mock
    private SinkTracer tracer;
    @Mock
    private Instrumentation instrumentation;
    @Mock
    private Future<List<Message>> future1;
    @Mock
    private Future<List<Message>> future2;
    @Mock
    private ConsumerAndOffsetManager consumerAndOffsetManager;
    private FirehoseAsyncConsumer asyncConsumer;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        this.asyncConsumer = new FirehoseAsyncConsumer(sinkPool, clock, tracer, consumerAndOffsetManager, instrumentation);
    }

    @Test
    public void shouldPushMessagesInParallel() throws FilterException {
        List<Message> messageList1 = new ArrayList<Message>() {{
            add(new Message(new byte[0], new byte[0], "topic1", 1, 10));
            add(new Message(new byte[0], new byte[0], "topic1", 2, 11));
            add(new Message(new byte[0], new byte[0], "topic1", 3, 12));
        }};

        List<Message> messageList2 = new ArrayList<Message>() {{
            add(new Message(new byte[0], new byte[0], "topic1", 2, 5));
            add(new Message(new byte[0], new byte[0], "topic1", 2, 6));
        }};

        Mockito.when(consumerAndOffsetManager.readMessagesFromKafka()).thenReturn(messageList1);
        Mockito.when(sinkPool.submitTask(messageList1)).thenReturn(future1);
        Mockito.when(sinkPool.submitTask(messageList2)).thenReturn(future2);
        Mockito.when(sinkPool.fetchFinishedSinkTasks()).thenReturn(new HashSet<>());
        Mockito.when(future1.isDone()).thenReturn(false);
        Mockito.when(future2.isDone()).thenReturn(false);
        asyncConsumer.process();
        Mockito.when(consumerAndOffsetManager.readMessagesFromKafka()).thenReturn(messageList2);
        asyncConsumer.process();
        Mockito.verify(consumerAndOffsetManager, Mockito.times(1)).addOffsets(future1, messageList1);
        Mockito.verify(consumerAndOffsetManager, Mockito.times(1)).addOffsets(future2, messageList2);
        Mockito.verify(consumerAndOffsetManager, Mockito.times(0)).setCommittable(Mockito.any());
    }

    @Test
    public void shouldCallSetCommittableForDoneFutures() throws FilterException {
        List<Message> messages = new ArrayList<Message>() {{
            add(new Message(new byte[0], new byte[0], "topic1", 1, 10));
            add(new Message(new byte[0], new byte[0], "topic1", 1, 11));
            add(new Message(new byte[0], new byte[0], "topic1", 1, 12));
        }};
        Mockito.when(consumerAndOffsetManager.readMessagesFromKafka()).thenReturn(messages);

        Mockito.when(sinkPool.submitTask(messages)).thenReturn(future1);
        Mockito.when(sinkPool.fetchFinishedSinkTasks()).thenReturn(new HashSet<Future<List<Message>>>() {{
            add(future1);
        }});
        asyncConsumer.process();

        Mockito.verify(consumerAndOffsetManager, Mockito.times(1)).addOffsets(future1, messages);
        Mockito.verify(consumerAndOffsetManager, Mockito.times(1)).setCommittable(future1);
    }

    @Test
    public void shouldThrowExceptionIfSinkTaskFails() throws Exception {
        expectedException.expect(AsyncConsumerFailedException.class);
        List<Message> messages = new ArrayList<Message>() {{
            add(new Message(new byte[0], new byte[0], "topic1", 1, 10));
            add(new Message(new byte[0], new byte[0], "topic1", 1, 11));
            add(new Message(new byte[0], new byte[0], "topic1", 1, 12));
        }};
        Mockito.when(consumerAndOffsetManager.readMessagesFromKafka()).thenReturn(messages);
        Mockito.when(sinkPool.submitTask(messages)).thenReturn(future1);
        Mockito.when(sinkPool.fetchFinishedSinkTasks()).thenThrow(new AsyncConsumerFailedException(new RuntimeException()));
        asyncConsumer.process();
    }
}
