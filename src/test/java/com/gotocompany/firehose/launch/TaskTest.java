package com.gotocompany.firehose.launch;

import com.gotocompany.firehose.metrics.FirehoseInstrumentation;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.Assert.assertEquals;

@RunWith(MockitoJUnitRunner.class)
public class TaskTest {

    private static final int PARALLELISM = 1;
    private static final int THREAD_CLEANUP_DELAY_IN_MS = 100;
    private static final long SLEEP_SECONDS = 10L;

    @Mock
    private FirehoseInstrumentation firehoseInstrumentation;

    @Test
    public void shouldExecuteTaskWithParallelism() throws InterruptedException {
        final List<Long> threadList = new ArrayList<>();
        Task task = new Task(PARALLELISM, THREAD_CLEANUP_DELAY_IN_MS, firehoseInstrumentation, callback -> {
            threadList.add(Thread.currentThread().getId());
            callback.run();
        });

        task.run().waitForCompletion();
        assertEquals(threadList.size(), PARALLELISM);
    }

    @Test @Ignore
    public void shouldExecuteTaskUntilStopped() throws InterruptedException {
        final ConcurrentHashMap<Long, String> threadResults = new ConcurrentHashMap<Long, String>();
        Task task = new Task(PARALLELISM, THREAD_CLEANUP_DELAY_IN_MS, firehoseInstrumentation, callback -> {
            try {
                while (!Thread.interrupted()) {
                    threadResults.put(Thread.currentThread().getId(), "thread started");
                }
            } finally {
                threadResults.put(Thread.currentThread().getId(), "thread closed");
                System.out.println("counting down");
                callback.run();
            }
        });

        task.run();

        for (Long key :threadResults.keySet()) {
            assertEquals(threadResults.get(key), "thread started");
        }

        task.stop();
        delayTaskSoWaitCallCatchesUp();
        task.waitForCompletion();

        for (Long key :threadResults.keySet()) {
            assertEquals(threadResults.get(key), "thread closed");
        }
    }

    private void delayTaskSoWaitCallCatchesUp() {
        try {
            Thread.sleep(SLEEP_SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}

