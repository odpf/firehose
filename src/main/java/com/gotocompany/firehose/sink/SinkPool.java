package com.gotocompany.firehose.sink;

import com.gotocompany.firehose.exception.SinkTaskFailedException;
import com.gotocompany.firehose.message.Message;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@AllArgsConstructor
@Slf4j
public class SinkPool implements AutoCloseable {
    private final Set<SinkFuture> sinkFutures = new HashSet<>();
    private final BlockingQueue<Sink> workerSinks;
    private final List<Sink> allSinks;
    private final ExecutorService executorService;
    private final long pollTimeOutMillis;

    public Set<Future<List<Message>>> fetchFinishedSinkTasks() {
        Set<SinkFuture> finished = sinkFutures.stream().filter(x -> {
            if (x.getFuture().isDone()) {
                try {
                    x.getFuture().get();
                    workerSinks.put(x.getSink());
                    return true;
                } catch (InterruptedException e) {
                    throw new SinkTaskFailedException(e);
                } catch (ExecutionException e) {
                    throw new SinkTaskFailedException(e.getCause());
                }
            }
            return false;
        }).collect(Collectors.toSet());
        sinkFutures.removeAll(finished);
        return finished.stream().map(SinkFuture::getFuture).collect(Collectors.toSet());
    }

    public Future<List<Message>> submitTask(List<Message> messages) {
        try {
            Sink workerSink = workerSinks.poll(pollTimeOutMillis, TimeUnit.MILLISECONDS);
            if (workerSink == null) {
                return null;
            }
            Future<List<Message>> future = executorService.submit(new SinkTask(workerSink, messages));
            sinkFutures.add(new SinkFuture(future, workerSink));
            return future;
        } catch (InterruptedException e) {
            return null;
        }
    }

    @Override
    public void close() {
        allSinks.forEach(sink -> {
            try {
                log.info("Closing sink");
                sink.close();
            } catch (IOException e) {
                log.error("Error happened while closing sink");
                e.printStackTrace();
            }
        });
        executorService.shutdown();
    }

    @Data
    @AllArgsConstructor
    @EqualsAndHashCode
    protected static class SinkFuture {
        private Future<List<Message>> future;
        private Sink sink;
    }

    /**
     * Sink Worker task.
     * It calls the pushMessage() and returns the response.
     */
    @AllArgsConstructor
    @EqualsAndHashCode
    protected static class SinkTask implements Callable<List<Message>> {
        private final Sink sink;
        private final List<Message> messages;

        @Override
        public List<Message> call() throws Exception {
            return sink.pushMessage(messages);
        }
    }
}

