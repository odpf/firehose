package org.raystack.firehose.sink.common.blobstorage.gcs;

import com.google.api.gax.retrying.RetrySettings;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;
import org.raystack.firehose.config.GCSConfig;
import org.raystack.firehose.sink.common.blobstorage.BlobStorage;
import org.raystack.firehose.sink.common.blobstorage.BlobStorageException;
import org.raystack.firehose.sink.common.blobstorage.gcs.error.GCSErrorType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.threeten.bp.Duration;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Date;

public class GoogleCloudStorage implements BlobStorage {
    private static final Logger LOGGER = LoggerFactory.getLogger(GoogleCloudStorage.class);
    private final GCSConfig gcsConfig;
    private final Storage storage;

    public GoogleCloudStorage(GCSConfig gcsConfig) throws IOException {
        this(gcsConfig, GoogleCredentials.fromStream(new FileInputStream(gcsConfig.getGCSCredentialPath())));
        checkBucket();
        logRetentionPolicy();
    }

    public GoogleCloudStorage(GCSConfig gcsConfig, GoogleCredentials credentials) {
        this(gcsConfig, StorageOptions.newBuilder()
                .setProjectId(gcsConfig.getGCloudProjectID())
                .setCredentials(credentials)
                .setRetrySettings(RetrySettings.newBuilder()
                        .setMaxAttempts(gcsConfig.getGCSRetryMaxAttempts())
                        .setInitialRetryDelay(Duration.ofMillis(gcsConfig.getGCSRetryInitialDelayMS()))
                        .setMaxRetryDelay(Duration.ofMillis(gcsConfig.getGCSRetryMaxDelayMS()))
                        .setRetryDelayMultiplier(gcsConfig.getGCSRetryDelayMultiplier())
                        .setTotalTimeout(Duration.ofMillis(gcsConfig.getGCSRetryTotalTimeoutMS()))
                        .setInitialRpcTimeout(Duration.ofMillis(gcsConfig.getGCSRetryInitialRPCTimeoutMS()))
                        .setRpcTimeoutMultiplier(gcsConfig.getGCSRetryRPCTimeoutMultiplier())
                        .setMaxRpcTimeout(Duration.ofMillis(gcsConfig.getGCSRetryRPCMaxTimeoutMS()))
                        .build())
                .build().getService());
    }

    public GoogleCloudStorage(GCSConfig gcsConfig, Storage storage) {
        this.gcsConfig = gcsConfig;
        this.storage = storage;
    }

    private void checkBucket() {
        String bucketName = gcsConfig.getGCSBucketName();
        Bucket bucket = storage.get(bucketName, Storage.BucketGetOption.userProject(gcsConfig.getGCloudProjectID()));
        if (bucket == null) {
            LOGGER.info("Bucket does not exist:{}", bucketName);
            LOGGER.info("Please create GCS bucket before running firehose: " + bucketName);
            throw new IllegalArgumentException("GCS Bucket not found " + bucketName);
        }
    }

    private void logRetentionPolicy() {
        String bucketName = gcsConfig.getGCSBucketName();
        Bucket bucket = storage.get(
                bucketName,
                Storage.BucketGetOption.fields(Storage.BucketField.RETENTION_POLICY),
                Storage.BucketGetOption.userProject(gcsConfig.getGCloudProjectID()));
        LOGGER.info("Retention Policy for {}", bucketName);
        LOGGER.info("Retention Period: {}", bucket.getRetentionPeriod());
        if (bucket.retentionPolicyIsLocked() != null && bucket.retentionPolicyIsLocked()) {
            LOGGER.info("Retention Policy is locked");
        }
        if (bucket.getRetentionEffectiveTime() != null) {
            LOGGER.info("Effective Time: {}", new Date(bucket.getRetentionEffectiveTime()));
        }
    }

    @Override
    public void store(String objectName, String filePath) throws BlobStorageException {
        try {
            byte[] content = Files.readAllBytes(Paths.get(filePath));
            store(objectName, content);
        } catch (IOException e) {
            LOGGER.error("Failed to read local file {}", filePath);
            throw new BlobStorageException("file_io_error", "File Read failed", e);
        }
    }

    @Override
    public void store(String objectName, byte[] content) throws BlobStorageException {
        BlobInfo blobInfo = BlobInfo.newBuilder(BlobId.of(gcsConfig.getGCSBucketName(), objectName)).build();
        String blobPath = String.join(File.separator, blobInfo.getBucket(), blobInfo.getName());
        try {
            storage.create(blobInfo, content, Storage.BlobTargetOption.userProject(gcsConfig.getGCloudProjectID()));
            LOGGER.info("Created object in GCS {}", blobPath);
        } catch (StorageException e) {
            LOGGER.error("Failed to create object in GCS {}", blobPath);
            String gcsErrorType = GCSErrorType.valueOfCode(e.getCode()).name();
            throw new BlobStorageException(gcsErrorType, "GCS Upload failed", e);
        }
    }
}
