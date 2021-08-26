package io.odpf.firehose.objectstorage.gcs;

import com.google.api.gax.retrying.RetrySettings;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;
import io.odpf.firehose.config.GCSConfig;
import io.odpf.firehose.objectstorage.ObjectStorage;
import io.odpf.firehose.objectstorage.ObjectStorageException;
import io.odpf.firehose.objectstorage.gcs.error.GCSErrorType;
import io.odpf.firehose.sink.objectstorage.writer.remote.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.threeten.bp.Duration;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class GCSObjectStorage implements ObjectStorage {
    private static final Logger LOGGER = LoggerFactory.getLogger(GCSObjectStorage.class);
    private final GCSConfig gcsConfig;
    private final Storage storage;

    public GCSObjectStorage(GCSConfig gcsConfig) throws IOException {
        this.gcsConfig = gcsConfig;
        RetrySettings retrySettings = RetrySettings.newBuilder()
                .setMaxAttempts(gcsConfig.getGCSRetryMaxAttempts())
                .setInitialRetryDelay(Duration.ofMillis(gcsConfig.getGCSRetryInitialDelayMS()))
                .setMaxRetryDelay(Duration.ofMillis(gcsConfig.getGCSRetryMaxDelayMS()))
                .setRetryDelayMultiplier(gcsConfig.getGCSRetryDelayMultiplier())
                .setTotalTimeout(Duration.ofMillis(gcsConfig.getGCSRetryTotalTimeoutMS()))
                .setInitialRpcTimeout(Duration.ofMillis(gcsConfig.getGCSRetryInitialRPCTimeoutMS()))
                .setRpcTimeoutMultiplier(gcsConfig.getGCSRetryRPCTimeoutMultiplier())
                .setMaxRpcTimeout(Duration.ofMillis(gcsConfig.getGCSRetryRPCMaxTimeoutMS()))
                .build();
        GoogleCredentials credentials = GoogleCredentials.fromStream(new FileInputStream(gcsConfig.getGCSCredentialPath()));
        this.storage = StorageOptions.newBuilder()
                .setProjectId(gcsConfig.getGCloudProjectID())
                .setCredentials(credentials)
                .setRetrySettings(retrySettings)
                .build().getService();
    }

    @Override
    public void store(String localPath) throws ObjectStorageException {
        String objectName = Paths.get(gcsConfig.getGCSLocalDirectory()).relativize(Paths.get(localPath)).toString();
        byte[] content;
        try {
            content = Files.readAllBytes(Paths.get(localPath));
        } catch (IOException e) {
            LOGGER.error("Failed to read local file " + localPath);
            throw new ObjectStorageException(Constants.FILE_IO_ERROR, "File Read failed", e);
        }
        store(objectName, content);
    }

    @Override
    public void store(String objectName, byte[] content) throws ObjectStorageException {
        BlobId blobId = BlobId.of(gcsConfig.getGCSBucketName(), objectName);
        BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
        try {
            storage.create(blobInfo, content);
            LOGGER.info("Created object in GCS " + blobInfo.getBucket() + "/" + blobInfo.getName());
        } catch (StorageException e) {
            LOGGER.error("Failed to create object in GCS " + blobInfo.getBucket() + "/" + blobInfo.getName());
            String gcsErrorType = GCSErrorType.valueOfCode(e.getCode()).name();
            throw new ObjectStorageException(gcsErrorType, "GCS Upload failed", e);
        }
    }
}
