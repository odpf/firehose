package com.gotocompany.firehose.launch;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Consumer;

import com.gotocompany.firehose.metrics.FirehoseInstrumentation;

/**
 * The Task with parallelism.
 */
public class Task {

    private final ExecutorService executorService;
    private int parallelism;
    private int threadCleanupDelay;
    private Consumer<Runnable> task;
    private Runnable taskFinishCallback;
    private final CountDownLatch countDownLatch;
    private final List<Future<?>> fnFutures;
    private FirehoseInstrumentation firehoseInstrumentation;

    /**
     * Instantiates a new Task.
     *
     * @param parallelism             the parallelism
     * @param threadCleanupDelay      the thread cleanup delay
     * @param firehoseInstrumentation the instrumentation
     * @param task                    the task
     */
    public Task(int parallelism, int threadCleanupDelay, FirehoseInstrumentation firehoseInstrumentation, Consumer<Runnable> task) {
        executorService = Executors.newFixedThreadPool(parallelism);
        this.parallelism = parallelism;
        this.threadCleanupDelay = threadCleanupDelay;
        this.task = task;
        this.countDownLatch = new CountDownLatch(parallelism);
        this.fnFutures = new ArrayList<>(parallelism);
        taskFinishCallback = countDownLatch::countDown;
        this.firehoseInstrumentation = firehoseInstrumentation;
    }

    public Task run() {
        for (int i = 0; i < parallelism; i++) {
            fnFutures.add(executorService.submit(() -> {
                task.accept(taskFinishCallback);
            }));
        }
        return this;
    }

    public void waitForCompletion() throws InterruptedException {
        firehoseInstrumentation.logInfo("waiting for completion");
        countDownLatch.await();
    }

    public Task stop() {
        try {
            firehoseInstrumentation.logInfo("Stopping task thread");
            fnFutures.forEach(consumerThread -> consumerThread.cancel(true));
            firehoseInstrumentation.logInfo("Sleeping thread during clean up for {} duration", threadCleanupDelay);
            Thread.sleep(threadCleanupDelay);
        } catch (InterruptedException e) {
            firehoseInstrumentation.captureNonFatalError("firehose_error_event", e, "error stopping tasks");
        }
        return this;
    }
}
