package com.gotocompany.firehose.sink.common.blobstorage.s3;


import com.gotocompany.firehose.config.S3Config;
import com.gotocompany.firehose.sink.common.blobstorage.BlobStorage;
import com.gotocompany.firehose.sink.common.blobstorage.BlobStorageException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.exception.SdkServiceException;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.core.retry.backoff.FullJitterBackoffStrategy;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;

public class S3 implements BlobStorage {
    private static final Logger LOGGER = LoggerFactory.getLogger(S3.class);
    private final S3Client s3Client;
    private final S3Config s3Config;

    public S3(S3Config s3Config) {
        this(s3Config, S3Client.builder()
                .region(Region.of(s3Config.getS3Region()))
                .overrideConfiguration(ClientOverrideConfiguration.builder()
                        .retryPolicy(RetryPolicy.builder()
                                .numRetries(s3Config.getS3RetryMaxAttempts())
                                .backoffStrategy(FullJitterBackoffStrategy.builder()
                                        .baseDelay(Duration.ofMillis(s3Config.getS3BaseDelay()))
                                        .maxBackoffTime(Duration.ofMillis(s3Config.getS3MaxBackoff()))
                                        .build())
                                .build())
                        .apiCallTimeout(Duration.ofMillis(s3Config.getS3ApiTimeout()))
                        .apiCallAttemptTimeout(Duration.ofMillis(s3Config.getS3ApiAttemptTimeout()))
                        .build())
                .build());
        checkBucket();
    }

    public S3(S3Config s3Config, S3Client s3Client) {
        this.s3Client = s3Client;
        this.s3Config = s3Config;
    }

    private void checkBucket() {
        String bucketName = s3Config.getS3BucketName();
        try {
            final HeadBucketRequest request = HeadBucketRequest.builder().bucket(bucketName).build();
            s3Client.headBucket(request);
            LOGGER.info("Bucket found " + bucketName);
        } catch (NoSuchBucketException ex) {
            LOGGER.error("Bucket not found " + bucketName);
            throw new IllegalArgumentException("S3 Bucket not found " + bucketName + "\n" + ex);
        } catch (S3Exception ex) {
            LOGGER.error("Cannot check access " + bucketName);
            throw new IllegalArgumentException("S3 Bucket not found " + bucketName + "\n" + ex);
        } catch (Exception ex) {
            LOGGER.error("Cannot check access", ex);
            throw ex;
        }
    }

    @Override
    public void store(String objectName, String filePath) throws BlobStorageException {
        String finalPath = createPath(objectName);
        try {
            byte[] content = Files.readAllBytes(Paths.get(filePath));
            store(finalPath, content);
        } catch (IOException e) {
            LOGGER.error("Failed to read local file {}", filePath);
            throw new BlobStorageException("file_io_error", "File Read failed", e);
        }
    }

    @Override
    public void store(String objectName, byte[] content) throws BlobStorageException {
        String finalPath = createPath(objectName);
        try {
            PutObjectRequest putObject = PutObjectRequest.builder()
                    .bucket(s3Config.getS3BucketName())
                    .key(finalPath)
                    .build();
            s3Client.putObject(putObject, RequestBody.fromBytes(content));
            LOGGER.info("Created object in S3 {}", objectName);
        } catch (SdkServiceException | SdkClientException ase) {
            LOGGER.error("Failed to create object in S3 {}", objectName);
            throw new BlobStorageException(ase.getMessage(), ase.getMessage(), ase);
        }
    }

    private String createPath(String objectName) {
        String prefix = s3Config.getS3DirectoryPrefix();
        return prefix == null || prefix.isEmpty()
                ? objectName : Paths.get(prefix, objectName).toString();
    }
}
