package com.gotocompany.firehose.sink.blob.writer.remote;

import com.gotocompany.firehose.metrics.BlobStorageMetrics;
import com.gotocompany.firehose.metrics.FirehoseInstrumentation;
import com.gotocompany.firehose.metrics.Metrics;
import com.gotocompany.firehose.sink.blob.writer.local.LocalFileMetadata;
import com.gotocompany.firehose.sink.common.blobstorage.BlobStorageException;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

@AllArgsConstructor
@Data
public class BlobStorageWriterFutureHandler {
    private Future<Long> future;
    private LocalFileMetadata localFileMetadata;
    private FirehoseInstrumentation firehoseInstrumentation;
    private static final String EMPTY = "";

    public String getFullPath() {
        return localFileMetadata.getFullPath();
    }

    public boolean isFinished() {
        if (!future.isDone()) {
            return false;
        }
        try {
            long totalTime = future.get();
            captureFileUploadSuccessMetric(totalTime);
            return true;
        } catch (InterruptedException e) {
            captureUploadFailedMetric(e);
            throw new BlobStorageFailedException(e);
        } catch (ExecutionException e) {
            captureUploadFailedMetric(e.getCause());
            throw new BlobStorageFailedException(e.getCause());
        }
    }

    private void captureFileUploadSuccessMetric(long totalTime) {
        firehoseInstrumentation.logInfo("Flushed to blob storage {}", localFileMetadata.getFullPath());
        firehoseInstrumentation.incrementCounter(BlobStorageMetrics.FILE_UPLOAD_TOTAL, Metrics.SUCCESS_TAG);
        firehoseInstrumentation.captureCount(BlobStorageMetrics.FILE_UPLOAD_BYTES, localFileMetadata.getSize());
        firehoseInstrumentation.captureCount(BlobStorageMetrics.FILE_UPLOAD_RECORDS_TOTAL, localFileMetadata.getRecordCount());
        firehoseInstrumentation.captureDuration(BlobStorageMetrics.FILE_UPLOAD_TIME_MILLISECONDS, totalTime);
    }

    private void captureUploadFailedMetric(Throwable e) {
        firehoseInstrumentation.logError("Failed to flush to blob storage {}", e.getMessage());
        String errorType;
        if (e instanceof BlobStorageException) {
            errorType = ((BlobStorageException) e).getErrorType();
        } else {
            errorType = "";
        }
        firehoseInstrumentation.incrementCounter(BlobStorageMetrics.FILE_UPLOAD_TOTAL, Metrics.FAILURE_TAG, Metrics.tag(BlobStorageMetrics.BLOB_STORAGE_ERROR_TYPE_TAG, errorType));
    }
}
