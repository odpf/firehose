package com.gotocompany.firehose.sink.blob.writer.remote;

import com.gotocompany.firehose.metrics.FirehoseInstrumentation;
import com.gotocompany.firehose.sink.common.blobstorage.BlobStorage;
import com.gotocompany.firehose.sink.blob.writer.local.LocalFileMetadata;
import lombok.AllArgsConstructor;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

@AllArgsConstructor
public class BlobStorageChecker implements Runnable {

    private final BlockingQueue<LocalFileMetadata> toBeFlushedToRemotePaths;
    private final BlockingQueue<String> flushedToRemotePaths;
    private final Set<BlobStorageWriterFutureHandler> remoteUploadFutures;
    private final ExecutorService remoteUploadScheduler;
    private final BlobStorage blobStorage;
    private final FirehoseInstrumentation firehoseInstrumentation;

    @Override
    public void run() {
        List<LocalFileMetadata> tobeFlushed = new ArrayList<>();
        toBeFlushedToRemotePaths.drainTo(tobeFlushed);
        remoteUploadFutures.addAll(tobeFlushed.stream().map(this::submitTask).collect(Collectors.toList()));
        Set<BlobStorageWriterFutureHandler> flushed = remoteUploadFutures.stream().filter(BlobStorageWriterFutureHandler::isFinished).collect(Collectors.toSet());
        remoteUploadFutures.removeAll(flushed);
        flushedToRemotePaths.addAll(flushed.stream().map(BlobStorageWriterFutureHandler::getFullPath).collect(Collectors.toSet()));
    }

    private BlobStorageWriterFutureHandler submitTask(LocalFileMetadata localFileMetadata) {
        BlobStorageWorker worker = new BlobStorageWorker(blobStorage, localFileMetadata);
        Future<Long> f = remoteUploadScheduler.submit(worker);
        return new BlobStorageWriterFutureHandler(f, localFileMetadata, firehoseInstrumentation);
    }
}

