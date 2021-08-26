package io.odpf.firehose.sink.objectstorage.writer;

import io.odpf.firehose.metrics.Instrumentation;
import io.odpf.firehose.metrics.StatsDReporter;
import io.odpf.firehose.objectstorage.ObjectStorage;
import io.odpf.firehose.sink.objectstorage.message.Record;
import io.odpf.firehose.sink.objectstorage.writer.local.FileMeta;
import io.odpf.firehose.sink.objectstorage.writer.local.LocalFileChecker;
import io.odpf.firehose.sink.objectstorage.writer.local.LocalFileWriter;
import io.odpf.firehose.sink.objectstorage.writer.local.LocalStorage;
import io.odpf.firehose.sink.objectstorage.writer.local.Partition;
import io.odpf.firehose.sink.objectstorage.writer.remote.ObjectStorageChecker;
import io.odpf.firehose.sink.objectstorage.writer.remote.ObjectStorageWriterWorkerFuture;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * This class manages threads for local and object storage checking.
 * It provides apis to write records to correct path based on time partitions.
 * <p>
 * LocalFileChecker: This thread is responsible for rotation of files based on policies.
 * Once a file is written to disk it adds to a queue to be consumed by ObjectStorageChecker.
 * <p>
 * ObjectStorageChecker: Reads the Local Files and Writes to given ObjectStorage.
 * After the file is written to object storage, it adds to to flushedPath queue.
 */
public class WriterOrchestrator implements Closeable {
    private static final int FILE_CHECKER_THREAD_INITIAL_DELAY_SECONDS = 10;
    private static final int FILE_CHECKER_THREAD_FREQUENCY_SECONDS = 5;
    private final Map<String, LocalFileWriter> timePartitionWriterMap = new ConcurrentHashMap<>();
    private final ScheduledExecutorService localFileCheckerScheduler = Executors.newScheduledThreadPool(1);
    private final ScheduledExecutorService objectStorageCheckerScheduler = Executors.newScheduledThreadPool(1);
    private final ExecutorService remoteUploadScheduler = Executors.newFixedThreadPool(10);
    private final BlockingQueue<String> flushedToRemotePaths = new LinkedBlockingQueue<>();
    private final LocalStorage localStorage;
    private final WriterOrchestratorStatus writerOrchestratorStatus;

    public WriterOrchestrator(LocalStorage localStorage, ObjectStorage objectStorage, StatsDReporter statsDReporter) {
        this.localStorage = localStorage;
        BlockingQueue<FileMeta> toBeFlushedToRemotePaths = new LinkedBlockingQueue<>();
        ScheduledFuture<?> localWriterFuture = localFileCheckerScheduler.scheduleAtFixedRate(
                new LocalFileChecker(
                        toBeFlushedToRemotePaths,
                        timePartitionWriterMap,
                        localStorage, new Instrumentation(statsDReporter, LocalFileChecker.class)),
                FILE_CHECKER_THREAD_INITIAL_DELAY_SECONDS,
                FILE_CHECKER_THREAD_FREQUENCY_SECONDS,
                TimeUnit.SECONDS);

        Set<ObjectStorageWriterWorkerFuture> remoteUploadFutures = new HashSet<>();
        ScheduledFuture<?> objectStorageWriterFuture = objectStorageCheckerScheduler.scheduleWithFixedDelay(
                new ObjectStorageChecker(
                        toBeFlushedToRemotePaths,
                        flushedToRemotePaths,
                        remoteUploadFutures,
                        remoteUploadScheduler,
                        objectStorage,
                        new Instrumentation(statsDReporter, ObjectStorageChecker.class)),
                FILE_CHECKER_THREAD_INITIAL_DELAY_SECONDS,
                FILE_CHECKER_THREAD_FREQUENCY_SECONDS,
                TimeUnit.SECONDS);

        writerOrchestratorStatus = new WriterOrchestratorStatus(false, localWriterFuture, objectStorageWriterFuture, null);
        writerOrchestratorStatus.startCheckerForLocalFileWriterCompletion();
        writerOrchestratorStatus.startCheckerForObjectStorageWriterCompletion();
    }

    /**
     * @return Return all paths which are flushed to remote and drain the list.
     * It also cleans up local paths from the disk.
     */
    public Set<String> getFlushedPaths() {
        Set<String> flushedPaths = new HashSet<>();
        flushedToRemotePaths.drainTo(flushedPaths);
        flushedPaths.forEach(localStorage::deleteLocalFile);
        return flushedPaths;
    }

    private void checkStatus() throws Exception {
        if (writerOrchestratorStatus.isClosed()) {
            throw new IOException(writerOrchestratorStatus.getThrowable());
        }
    }

    /**
     * Writes the records based on the partition configuration.
     *
     * @param record record to be written
     * @return Local path where the record was stored.
     * @throws Exception if local storage fails or writer orchestrator is closed.
     */
    public String write(Record record) throws Exception {
        checkStatus();
        Partition partition = localStorage.getPartitionFactory().getPartition(record);
        return write(record, partition);
    }

    /**
     * Tries to fetch writer from the map, if the writer is closed, try recursive method call.
     *
     * @param record    record to write
     * @param partition partition for the file path
     * @return full path of file.
     * @throws IOException if local storage fails.
     */
    private String write(Record record, Partition partition) throws IOException {
        LocalFileWriter writer = timePartitionWriterMap.computeIfAbsent(
                partition.toString(),
                x -> localStorage.createLocalFileWriter(partition.getPath()));
        if (!writer.write(record)) {
            return write(record, partition);
        }
        return writer.getFullPath();
    }

    @Override
    public void close() throws IOException {
        localFileCheckerScheduler.shutdown();
        objectStorageCheckerScheduler.shutdown();
        remoteUploadScheduler.shutdown();
        writerOrchestratorStatus.setClosed(true);
        synchronized (timePartitionWriterMap) {
            for (LocalFileWriter writer : timePartitionWriterMap.values()) {
                writer.close();
            }
            for (String p : timePartitionWriterMap.keySet()) {
                localStorage.deleteLocalFile(p);
            }
        }
    }
}
