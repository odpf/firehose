package com.gojek.esb.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Consumer;

public class Task {

    private static final Logger LOGGER = LoggerFactory.getLogger(Task.class);
    private final ExecutorService executorService;
    private int parallelism;
    private int threadCleanupDelay;
    private Consumer<Runnable> task;
    private Runnable taskFinishCallback;
    private final CountDownLatch countDownLatch;
    private final List<Future<?>> fnFutures;

    public Task(int parallelism, int threadCleanupDelay, Consumer<Runnable> task) {
        executorService = Executors.newFixedThreadPool(parallelism);
        this.parallelism = parallelism;
        this.threadCleanupDelay = threadCleanupDelay;
        this.task = task;
        this.countDownLatch = new CountDownLatch(parallelism);
        this.fnFutures = new ArrayList<>(parallelism);
        taskFinishCallback = countDownLatch::countDown;
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
        LOGGER.info("waiting for completion");
        countDownLatch.await();
    }

    public Task stop() {
        try {
            fnFutures.forEach(consumerThread -> consumerThread.cancel(true));
            Thread.sleep(threadCleanupDelay);
        } catch (InterruptedException e) {
            LOGGER.error("error stopping tasks", e);
        }
        return this;
    }
}
