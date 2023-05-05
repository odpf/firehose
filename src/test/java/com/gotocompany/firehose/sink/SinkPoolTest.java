package com.gotocompany.firehose.sink;

import com.gotocompany.firehose.message.Message;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class SinkPoolTest {

    @Mock
    private BlockingQueue<Sink> workerSinks;
    @Mock
    private ExecutorService executorService;

    private SinkPool sinkPool;

    @Mock
    private Sink sink1;
    @Mock
    private Sink sink2;
    @Mock
    private Future<List<Message>> future1;
    @Mock
    private Future<List<Message>> future2;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        sinkPool = new SinkPool(workerSinks, new ArrayList<>(), executorService, 5);
    }

    @Test
    public void shouldSubmitTask() throws InterruptedException {
        List<Message> messages = new ArrayList<Message>() {{
            add(new Message(new byte[0], new byte[0], "topic1", 1, 10));
            add(new Message(new byte[0], new byte[0], "topic1", 2, 11));
            add(new Message(new byte[0], new byte[0], "topic1", 3, 12));
        }};
        Mockito.when(workerSinks.poll(5, TimeUnit.MILLISECONDS)).thenReturn(sink1);
        Mockito.when(executorService.submit(new SinkPool.SinkTask(sink1, messages))).thenReturn(future1);
        Assert.assertEquals(future1, sinkPool.submitTask(messages));

    }

    @Test
    public void shouldNotSubmitTask() throws InterruptedException {
        List<Message> messages = new ArrayList<Message>() {{
            add(new Message(new byte[0], new byte[0], "topic1", 1, 10));
            add(new Message(new byte[0], new byte[0], "topic1", 2, 11));
            add(new Message(new byte[0], new byte[0], "topic1", 3, 12));
        }};
        Mockito.when(workerSinks.poll(5, TimeUnit.MILLISECONDS)).thenReturn(null);
        Assert.assertNull(sinkPool.submitTask(messages));
    }

    @Test
    public void shouldFetchFinishedFutures() throws InterruptedException {
        List<Message> messageList1 = new ArrayList<Message>() {{
            add(new Message(new byte[0], new byte[0], "topic1", 1, 10));
            add(new Message(new byte[0], new byte[0], "topic1", 2, 11));
            add(new Message(new byte[0], new byte[0], "topic1", 3, 12));
        }};
        Mockito.when(workerSinks.poll(5, TimeUnit.MILLISECONDS)).thenReturn(sink1);
        Mockito.when(executorService.submit(new SinkPool.SinkTask(sink1, messageList1))).thenReturn(future1);

        sinkPool.submitTask(messageList1);

        List<Message> messageList2 = new ArrayList<Message>() {{
            add(new Message(new byte[0], new byte[0], "topic1", 2, 5));
            add(new Message(new byte[0], new byte[0], "topic1", 2, 6));
        }};

        Mockito.when(workerSinks.poll(5, TimeUnit.MILLISECONDS)).thenReturn(sink2);
        Mockito.when(executorService.submit(new SinkPool.SinkTask(sink2, messageList2))).thenReturn(future2);
        sinkPool.submitTask(messageList2);

        Mockito.when(future1.isDone()).thenReturn(false);
        Mockito.when(future2.isDone()).thenReturn(false);
        Set<Future<List<Message>>> finishedTasks = sinkPool.fetchFinishedSinkTasks();
        Assert.assertEquals(0, finishedTasks.size());

        Mockito.when(future1.isDone()).thenReturn(true);
        finishedTasks = sinkPool.fetchFinishedSinkTasks();
        Assert.assertEquals(1, finishedTasks.size());
        Mockito.verify(workerSinks, Mockito.times(1)).put(sink1);
    }
}
