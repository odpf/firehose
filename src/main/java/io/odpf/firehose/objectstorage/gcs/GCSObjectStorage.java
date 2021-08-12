package io.odpf.firehose.objectstorage.gcs;

import com.google.api.gax.retrying.RetrySettings;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;
import io.odpf.firehose.objectstorage.ObjectStorage;
import io.odpf.firehose.objectstorage.ObjectStorageException;
import io.odpf.firehose.objectstorage.gcs.error.GCSErrorType;
import io.odpf.firehose.objectstorage.gcs.exception.GCSException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.threeten.bp.Duration;
import org.threeten.bp.temporal.ChronoUnit;

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
        this.storage = StorageOptions.newBuilder()
                .setRetrySettings(RetrySettings.newBuilder()
                        .setMaxAttempts(gcsConfig.getMaxRetryAttempt())
                        .setTotalTimeout(Duration.of(gcsConfig.getMaxRetryDurationMilliseconds(), ChronoUnit.MILLIS))
                        .build())
                .setProjectId(gcsConfig.getGcsProjectId())
                .setCredentials(GoogleCredentials.fromStream(new FileInputStream(gcsConfig.getCredentialPath())))
                .build().getService();
    }

    @Override
    public void store(String localPath) throws IOException, ObjectStorageException {
        String objectName = gcsConfig.getLocalBasePath().relativize(Paths.get(localPath)).toString();
        byte[] content = Files.readAllBytes(Paths.get(localPath));
        store(objectName, content);
    }

    @Override
    public void store(String objectName, byte[] content) throws ObjectStorageException {
        BlobId blobId = BlobId.of(gcsConfig.getGcsBucketName(), objectName);
        BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
        try {
            storage.create(blobInfo, content);
            LOGGER.info("Created object in GCS " + blobInfo.getBucket() + "/" + blobInfo.getName());
        } catch (StorageException e) {
            GCSErrorType gcsErrorType = GCSErrorType.valueOfCode(e.getCode());
            GCSException gcsException = new GCSException(gcsErrorType, e.getCode(), e.getReason());
            throw new ObjectStorageException(gcsException);
        }
    }
}
