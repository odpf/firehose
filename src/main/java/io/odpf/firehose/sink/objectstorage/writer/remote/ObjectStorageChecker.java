package io.odpf.firehose.sink.objectstorage.writer.remote;

import io.odpf.firehose.metrics.Instrumentation;
import io.odpf.firehose.objectstorage.ObjectStorage;
import io.odpf.firehose.objectstorage.ObjectStorageException;
import io.odpf.firehose.sink.objectstorage.writer.local.FileMeta;
import io.odpf.firehose.sink.objectstorage.writer.local.Partition;
import lombok.AllArgsConstructor;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import static io.odpf.firehose.metrics.Metrics.FAILURE_TAG;
import static io.odpf.firehose.metrics.Metrics.SUCCESS_TAG;
import static io.odpf.firehose.metrics.Metrics.tag;
import static io.odpf.firehose.metrics.ObjectStorageMetrics.FILE_UPLOAD_BYTES;
import static io.odpf.firehose.metrics.ObjectStorageMetrics.FILE_UPLOAD_TIME_MILLISECONDS;
import static io.odpf.firehose.metrics.ObjectStorageMetrics.FILE_UPLOAD_TOTAL;
import static io.odpf.firehose.metrics.ObjectStorageMetrics.OBJECT_STORE_ERROR_TYPE_TAG;
import static io.odpf.firehose.metrics.ObjectStorageMetrics.PARTITION_TAG;
import static io.odpf.firehose.metrics.ObjectStorageMetrics.TOPIC_TAG;

@AllArgsConstructor
public class ObjectStorageChecker implements Runnable {

    private final BlockingQueue<FileMeta> toBeFlushedToRemotePaths;
    private final BlockingQueue<String> flushedToRemotePaths;
    private final Set<ObjectStorageWriterWorkerFuture> remoteUploadFutures;
    private final ExecutorService remoteUploadScheduler;
    private final ObjectStorage objectStorage;
    private final Instrumentation instrumentation;

    @Override
    public void run() {
        List<FileMeta> tobeFlushed = new ArrayList<>();
        toBeFlushedToRemotePaths.drainTo(tobeFlushed);
        remoteUploadFutures.addAll(tobeFlushed.stream().map(this::submitTask).collect(Collectors.toList()));
        Set<String> flushedPath = remoteUploadFutures.stream().map(worker -> {
            if (worker.getFuture().isDone()) {
                try {
                    long totalTime = worker.getFuture().get();
                    FileMeta fileMeta = worker.getFileMeta();
                    captureFileUploadSuccessMetric(fileMeta, totalTime);
                    return worker.getFileMeta().getFullPath();
                } catch (InterruptedException e) {
                    captureUploadFailedMetric(worker, e);
                    throw new ObjectStorageFailedException(e);
                } catch (ExecutionException e) {
                    captureUploadFailedMetric(worker, e.getCause());
                    throw new ObjectStorageFailedException(e.getCause());
                }
            }
            return "";
        }).filter(x -> !x.isEmpty()).collect(Collectors.toSet());
        remoteUploadFutures.removeIf(x -> flushedPath.contains(x.getFileMeta().getFullPath()));
        flushedToRemotePaths.addAll(flushedPath);
    }

    private ObjectStorageWriterWorkerFuture submitTask(FileMeta fileMeta) {
        ObjectStorageWorker worker = new ObjectStorageWorker(objectStorage, fileMeta.getFullPath());
        Future<Long> f = remoteUploadScheduler.submit(worker);
        return new ObjectStorageWriterWorkerFuture(f, fileMeta);
    }

    private void captureFileUploadSuccessMetric(FileMeta fileMeta, long totalTime) {
        instrumentation.logInfo("Flushed to Object storage " + fileMeta.getFullPath());
        Partition partition = fileMeta.getPartition();
        String topic = partition.getTopic();
        String datetimeTag = partition.getDatetimePathWithoutPrefix();

        instrumentation.incrementCounter(FILE_UPLOAD_TOTAL,
                SUCCESS_TAG,
                tag(TOPIC_TAG, topic),
                tag(PARTITION_TAG, datetimeTag));
        instrumentation.captureCount(FILE_UPLOAD_BYTES, fileMeta.getFileSizeBytes(),
                tag(TOPIC_TAG, topic),
                tag(PARTITION_TAG, datetimeTag));
        instrumentation.captureDuration(FILE_UPLOAD_TIME_MILLISECONDS, totalTime,
                tag(TOPIC_TAG, topic),
                tag(PARTITION_TAG, datetimeTag));
    }

    private void captureUploadFailedMetric(ObjectStorageWriterWorkerFuture uploadJob, Throwable e) {
        instrumentation.logError("Failed to flush to Object storage {}", e.getMessage());
        String errorType;
        if (e instanceof ObjectStorageException) {
            errorType = ((ObjectStorageException) e).getErrorType();
        } else {
            errorType = "";
        }
        Partition partition = uploadJob.getFileMeta().getPartition();
        String topic = partition.getTopic();
        String datetimeTag = partition.getDatetimePathWithoutPrefix();
        instrumentation.incrementCounter(FILE_UPLOAD_TOTAL,
                FAILURE_TAG,
                tag(OBJECT_STORE_ERROR_TYPE_TAG, errorType),
                tag(TOPIC_TAG, topic),
                tag(PARTITION_TAG, datetimeTag));
    }
}

