package com.gotocompany.firehose.sink.common.blobstorage;

import com.aliyun.oss.OSS;
import com.aliyun.oss.model.Bucket;
import com.aliyun.oss.model.BucketList;
import com.aliyun.oss.model.ListBucketsRequest;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class BlobStorageFactoryTest {

    private Map<String, String> validOssConfig;

    @Mock
    private OSS ossClient;

    @Mock
    private BucketList bucketList;

    @Before
    public void setUp() {
        validOssConfig = new HashMap<>();
        validOssConfig.put("OSS_TYPE_OSS_ENDPOINT", "oss-cn-hangzhou.aliyuncs.com");
        validOssConfig.put("OSS_TYPE_OSS_REGION", "cn-hangzhou");
        validOssConfig.put("OSS_TYPE_OSS_ACCESS_ID", "test-access-id");
        validOssConfig.put("OSS_TYPE_OSS_ACCESS_KEY", "test-access-key");
        validOssConfig.put("OSS_TYPE_OSS_BUCKET_NAME", "test-bucket");
        validOssConfig.put("OSS_TYPE_OSS_DIRECTORY_PREFIX", "test-prefix");

        when(ossClient.listBuckets(any(ListBucketsRequest.class))).thenReturn(bucketList);
        when(bucketList.getBucketList()).thenReturn(Collections.singletonList(mock(Bucket.class)));
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionWhenConfigIsNull() {
        BlobStorageFactory.createObjectStorage(BlobStorageType.OSS, null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionWhenConfigIsEmpty() {
        BlobStorageFactory.createObjectStorage(BlobStorageType.OSS, new HashMap<>());
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionWhenEndpointIsMissing() {
        validOssConfig.remove("OSS_TYPE_OSS_ENDPOINT");
        BlobStorageFactory.createObjectStorage(BlobStorageType.OSS, validOssConfig);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionWhenRegionIsMissing() {
        validOssConfig.remove("OSS_TYPE_OSS_REGION");
        BlobStorageFactory.createObjectStorage(BlobStorageType.OSS, validOssConfig);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionWhenAccessIdIsMissing() {
        validOssConfig.remove("OSS_TYPE_OSS_ACCESS_ID");
        BlobStorageFactory.createObjectStorage(BlobStorageType.OSS, validOssConfig);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionWhenAccessKeyIsMissing() {
        validOssConfig.remove("OSS_TYPE_OSS_ACCESS_KEY");
        BlobStorageFactory.createObjectStorage(BlobStorageType.OSS, validOssConfig);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionWhenBucketNameIsMissing() {
        validOssConfig.remove("OSS_TYPE_OSS_BUCKET_NAME");
        BlobStorageFactory.createObjectStorage(BlobStorageType.OSS, validOssConfig);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionWhenEndpointIsInvalid() {
        validOssConfig.put("OSS_TYPE_OSS_ENDPOINT", "");
        BlobStorageFactory.createObjectStorage(BlobStorageType.OSS, validOssConfig);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionWhenAccessIdIsEmpty() {
        validOssConfig.put("OSS_TYPE_OSS_ACCESS_ID", "");
        BlobStorageFactory.createObjectStorage(BlobStorageType.OSS, validOssConfig);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionWhenAccessKeyIsEmpty() {
        validOssConfig.put("OSS_TYPE_OSS_ACCESS_KEY", "");
        BlobStorageFactory.createObjectStorage(BlobStorageType.OSS, validOssConfig);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionWhenBucketNameIsEmpty() {
        validOssConfig.put("OSS_TYPE_OSS_BUCKET_NAME", "");
        BlobStorageFactory.createObjectStorage(BlobStorageType.OSS, validOssConfig);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionWhenSocketTimeoutIsInvalid() {
        validOssConfig.put("OSS_TYPE_OSS_SOCKET_TIMEOUT_MS", "invalid");
        BlobStorageFactory.createObjectStorage(BlobStorageType.OSS, validOssConfig);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionWhenSocketTimeoutIsNegative() {
        validOssConfig.put("OSS_TYPE_OSS_SOCKET_TIMEOUT_MS", "-1000");
        BlobStorageFactory.createObjectStorage(BlobStorageType.OSS, validOssConfig);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionWhenConnectionTimeoutIsInvalid() {
        validOssConfig.put("OSS_TYPE_OSS_CONNECTION_TIMEOUT_MS", "invalid");
        BlobStorageFactory.createObjectStorage(BlobStorageType.OSS, validOssConfig);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionWhenConnectionTimeoutIsNegative() {
        validOssConfig.put("OSS_TYPE_OSS_CONNECTION_TIMEOUT_MS", "-1000");
        BlobStorageFactory.createObjectStorage(BlobStorageType.OSS, validOssConfig);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionWhenRequestTimeoutIsInvalid() {
        validOssConfig.put("OSS_TYPE_OSS_REQUEST_TIMEOUT_MS", "invalid");
        BlobStorageFactory.createObjectStorage(BlobStorageType.OSS, validOssConfig);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionWhenRequestTimeoutIsNegative() {
        validOssConfig.put("OSS_TYPE_OSS_REQUEST_TIMEOUT_MS", "-1000");
        BlobStorageFactory.createObjectStorage(BlobStorageType.OSS, validOssConfig);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionWhenRetryEnabledIsInvalid() {
        validOssConfig.put("OSS_TYPE_OSS_RETRY_ENABLED", "invalid");
        BlobStorageFactory.createObjectStorage(BlobStorageType.OSS, validOssConfig);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionWhenMaxRetryAttemptsIsInvalid() {
        validOssConfig.put("OSS_TYPE_OSS_MAX_RETRY_ATTEMPTS", "invalid");
        BlobStorageFactory.createObjectStorage(BlobStorageType.OSS, validOssConfig);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionWhenMaxRetryAttemptsIsNegative() {
        validOssConfig.put("OSS_TYPE_OSS_MAX_RETRY_ATTEMPTS", "-1");
        BlobStorageFactory.createObjectStorage(BlobStorageType.OSS, validOssConfig);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionWhenMaxRetryAttemptsIsZero() {
        validOssConfig.put("OSS_TYPE_OSS_MAX_RETRY_ATTEMPTS", "0");
        BlobStorageFactory.createObjectStorage(BlobStorageType.OSS, validOssConfig);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionWhenRegionIsInvalid() {
        validOssConfig.put("OSS_TYPE_OSS_REGION", "");
        BlobStorageFactory.createObjectStorage(BlobStorageType.OSS, validOssConfig);
    }
}
