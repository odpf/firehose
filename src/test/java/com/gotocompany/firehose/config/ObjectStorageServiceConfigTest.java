package com.gotocompany.firehose.config;

import org.aeonbits.owner.ConfigFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class ObjectStorageServiceConfigTest {

    private Map<String, String> properties;

    @Before
    public void setup() {
        properties = new HashMap<>();
        properties.put("OSS_TYPE", "SINK");
    }

    @Test
    public void shouldGetOssEndpoint() {
        properties.put("SINK_OSS_ENDPOINT", "http://oss-cn-hangzhou.aliyuncs.com");
        ObjectStorageServiceConfig config = ConfigFactory.create(ObjectStorageServiceConfig.class, properties);
        Assert.assertEquals("http://oss-cn-hangzhou.aliyuncs.com", config.getOssEndpoint());
    }

    @Test
    public void shouldGetOssRegion() {
        properties.put("SINK_OSS_REGION", "cn-hangzhou");
        ObjectStorageServiceConfig config = ConfigFactory.create(ObjectStorageServiceConfig.class, properties);
        Assert.assertEquals("cn-hangzhou", config.getOssRegion());
    }

    @Test
    public void shouldGetOssAccessId() {
        properties.put("SINK_OSS_ACCESS_ID", "test-access-id");
        ObjectStorageServiceConfig config = ConfigFactory.create(ObjectStorageServiceConfig.class, properties);
        Assert.assertEquals("test-access-id", config.getOssAccessId());
    }

    @Test
    public void shouldGetOssAccessKey() {
        properties.put("SINK_OSS_ACCESS_KEY", "test-access-key");
        ObjectStorageServiceConfig config = ConfigFactory.create(ObjectStorageServiceConfig.class, properties);
        Assert.assertEquals("test-access-key", config.getOssAccessKey());
    }

    @Test
    public void shouldGetOssBucketName() {
        properties.put("SINK_OSS_BUCKET_NAME", "test-bucket");
        ObjectStorageServiceConfig config = ConfigFactory.create(ObjectStorageServiceConfig.class, properties);
        Assert.assertEquals("test-bucket", config.getOssBucketName());
    }

    @Test
    public void shouldGetOssDirectoryPrefix() {
        properties.put("SINK_OSS_DIRECTORY_PREFIX", "test/prefix");
        ObjectStorageServiceConfig config = ConfigFactory.create(ObjectStorageServiceConfig.class, properties);
        Assert.assertEquals("test/prefix", config.getOssDirectoryPrefix());
    }

    @Test
    public void shouldGetDefaultSocketTimeout() {
        ObjectStorageServiceConfig config = ConfigFactory.create(ObjectStorageServiceConfig.class, properties);
        Assert.assertEquals(Integer.valueOf(50000), config.getOssSocketTimeoutMs());
    }

    @Test
    public void shouldOverrideDefaultSocketTimeout() {
        properties.put("SINK_OSS_SOCKET_TIMEOUT_MS", "30000");
        ObjectStorageServiceConfig config = ConfigFactory.create(ObjectStorageServiceConfig.class, properties);
        Assert.assertEquals(Integer.valueOf(30000), config.getOssSocketTimeoutMs());
    }

    @Test
    public void shouldGetDefaultConnectionTimeout() {
        ObjectStorageServiceConfig config = ConfigFactory.create(ObjectStorageServiceConfig.class, properties);
        Assert.assertEquals(Integer.valueOf(50000), config.getOssConnectionTimeoutMs());
    }

    @Test
    public void shouldOverrideDefaultConnectionTimeout() {
        properties.put("SINK_OSS_CONNECTION_TIMEOUT_MS", "20000");
        ObjectStorageServiceConfig config = ConfigFactory.create(ObjectStorageServiceConfig.class, properties);
        Assert.assertEquals(Integer.valueOf(20000), config.getOssConnectionTimeoutMs());
    }

    @Test
    public void shouldGetDefaultConnectionRequestTimeout() {
        ObjectStorageServiceConfig config = ConfigFactory.create(ObjectStorageServiceConfig.class, properties);
        Assert.assertEquals(Integer.valueOf(-1), config.getOssConnectionRequestTimeoutMs());
    }

    @Test
    public void shouldOverrideDefaultConnectionRequestTimeout() {
        properties.put("SINK_OSS_CONNECTION_REQUEST_TIMEOUT_MS", "15000");
        ObjectStorageServiceConfig config = ConfigFactory.create(ObjectStorageServiceConfig.class, properties);
        Assert.assertEquals(Integer.valueOf(15000), config.getOssConnectionRequestTimeoutMs());
    }

    @Test
    public void shouldGetDefaultRequestTimeout() {
        ObjectStorageServiceConfig config = ConfigFactory.create(ObjectStorageServiceConfig.class, properties);
        Assert.assertEquals(Integer.valueOf(300000), config.getOssRequestTimeoutMs());
    }

    @Test
    public void shouldOverrideDefaultRequestTimeout() {
        properties.put("SINK_OSS_REQUEST_TIMEOUT_MS", "200000");
        ObjectStorageServiceConfig config = ConfigFactory.create(ObjectStorageServiceConfig.class, properties);
        Assert.assertEquals(Integer.valueOf(200000), config.getOssRequestTimeoutMs());
    }

    @Test
    public void shouldGetDefaultRetryEnabled() {
        ObjectStorageServiceConfig config = ConfigFactory.create(ObjectStorageServiceConfig.class, properties);
        Assert.assertTrue(config.isRetryEnabled());
    }

    @Test
    public void shouldOverrideDefaultRetryEnabled() {
        properties.put("SINK_OSS_RETRY_ENABLED", "false");
        ObjectStorageServiceConfig config = ConfigFactory.create(ObjectStorageServiceConfig.class, properties);
        Assert.assertFalse(config.isRetryEnabled());
    }

    @Test
    public void shouldGetDefaultMaxRetryAttempts() {
        ObjectStorageServiceConfig config = ConfigFactory.create(ObjectStorageServiceConfig.class, properties);
        Assert.assertEquals(3, config.getOssMaxRetryAttempts());
    }

    @Test
    public void shouldOverrideDefaultMaxRetryAttempts() {
        properties.put("SINK_OSS_MAX_RETRY_ATTEMPTS", "5");
        ObjectStorageServiceConfig config = ConfigFactory.create(ObjectStorageServiceConfig.class, properties);
        Assert.assertEquals(5, config.getOssMaxRetryAttempts());
    }

    @Test
    public void shouldHandleEmptyOssType() {
        properties.put("OSS_TYPE", "");
        ObjectStorageServiceConfig config = ConfigFactory.create(ObjectStorageServiceConfig.class, properties);
        Assert.assertNull(config.getOssEndpoint());
    }

    @Test
    public void shouldHandleNullOssType() {
        properties.remove("OSS_TYPE");
        ObjectStorageServiceConfig config = ConfigFactory.create(ObjectStorageServiceConfig.class, properties);
        Assert.assertNull(config.getOssEndpoint());
    }

    @Test
    public void shouldHandleSpecialCharactersInBucketName() {
        properties.put("SINK_OSS_BUCKET_NAME", "test-bucket-123_special");
        ObjectStorageServiceConfig config = ConfigFactory.create(ObjectStorageServiceConfig.class, properties);
        Assert.assertEquals("test-bucket-123_special", config.getOssBucketName());
    }

    @Test
    public void shouldHandleInternalEndpoint() {
        properties.put("SINK_OSS_ENDPOINT", "http://oss-cn-hangzhou-internal.aliyuncs.com");
        ObjectStorageServiceConfig config = ConfigFactory.create(ObjectStorageServiceConfig.class, properties);
        Assert.assertEquals("http://oss-cn-hangzhou-internal.aliyuncs.com", config.getOssEndpoint());
    }

    @Test
    public void shouldHandleHttpsEndpoint() {
        properties.put("SINK_OSS_ENDPOINT", "https://oss-cn-hangzhou.aliyuncs.com");
        ObjectStorageServiceConfig config = ConfigFactory.create(ObjectStorageServiceConfig.class, properties);
        Assert.assertEquals("https://oss-cn-hangzhou.aliyuncs.com", config.getOssEndpoint());
    }

    @Test
    public void shouldHandleCustomEndpoint() {
        properties.put("SINK_OSS_ENDPOINT", "http://custom-domain.com");
        ObjectStorageServiceConfig config = ConfigFactory.create(ObjectStorageServiceConfig.class, properties);
        Assert.assertEquals("http://custom-domain.com", config.getOssEndpoint());
    }

    @Test
    public void shouldHandleZeroTimeouts() {
        properties.put("SINK_OSS_SOCKET_TIMEOUT_MS", "0");
        properties.put("SINK_OSS_CONNECTION_TIMEOUT_MS", "0");
        properties.put("SINK_OSS_REQUEST_TIMEOUT_MS", "0");
        ObjectStorageServiceConfig config = ConfigFactory.create(ObjectStorageServiceConfig.class, properties);
        Assert.assertEquals(Integer.valueOf(0), config.getOssSocketTimeoutMs());
        Assert.assertEquals(Integer.valueOf(0), config.getOssConnectionTimeoutMs());
        Assert.assertEquals(Integer.valueOf(0), config.getOssRequestTimeoutMs());
    }

    @Test
    public void shouldHandleNegativeTimeouts() {
        properties.put("SINK_OSS_SOCKET_TIMEOUT_MS", "-1");
        properties.put("SINK_OSS_CONNECTION_TIMEOUT_MS", "-1");
        properties.put("SINK_OSS_REQUEST_TIMEOUT_MS", "-1");
        ObjectStorageServiceConfig config = ConfigFactory.create(ObjectStorageServiceConfig.class, properties);
        Assert.assertEquals(Integer.valueOf(-1), config.getOssSocketTimeoutMs());
        Assert.assertEquals(Integer.valueOf(-1), config.getOssConnectionTimeoutMs());
        Assert.assertEquals(Integer.valueOf(-1), config.getOssRequestTimeoutMs());
    }

    @Test
    public void shouldHandleMaxIntegerTimeouts() {
        properties.put("SINK_OSS_SOCKET_TIMEOUT_MS", String.valueOf(Integer.MAX_VALUE));
        properties.put("SINK_OSS_CONNECTION_TIMEOUT_MS", String.valueOf(Integer.MAX_VALUE));
        properties.put("SINK_OSS_REQUEST_TIMEOUT_MS", String.valueOf(Integer.MAX_VALUE));
        ObjectStorageServiceConfig config = ConfigFactory.create(ObjectStorageServiceConfig.class, properties);
        Assert.assertEquals(Integer.valueOf(Integer.MAX_VALUE), config.getOssSocketTimeoutMs());
        Assert.assertEquals(Integer.valueOf(Integer.MAX_VALUE), config.getOssConnectionTimeoutMs());
        Assert.assertEquals(Integer.valueOf(Integer.MAX_VALUE), config.getOssRequestTimeoutMs());
    }

    @Test
    public void shouldHandleEmptyDirectoryPrefix() {
        properties.put("SINK_OSS_DIRECTORY_PREFIX", "");
        ObjectStorageServiceConfig config = ConfigFactory.create(ObjectStorageServiceConfig.class, properties);
        Assert.assertEquals("", config.getOssDirectoryPrefix());
    }

    @Test
    public void shouldHandleMultiLevelDirectoryPrefix() {
        properties.put("SINK_OSS_DIRECTORY_PREFIX", "level1/level2/level3");
        ObjectStorageServiceConfig config = ConfigFactory.create(ObjectStorageServiceConfig.class, properties);
        Assert.assertEquals("level1/level2/level3", config.getOssDirectoryPrefix());
    }

    @Test
    public void shouldHandleDirectoryPrefixWithSpecialCharacters() {
        properties.put("SINK_OSS_DIRECTORY_PREFIX", "test-prefix_123/special@chars");
        ObjectStorageServiceConfig config = ConfigFactory.create(ObjectStorageServiceConfig.class, properties);
        Assert.assertEquals("test-prefix_123/special@chars", config.getOssDirectoryPrefix());
    }

    @Test
    public void shouldHandleMaxRetryAttemptsZero() {
        properties.put("SINK_OSS_MAX_RETRY_ATTEMPTS", "0");
        ObjectStorageServiceConfig config = ConfigFactory.create(ObjectStorageServiceConfig.class, properties);
        Assert.assertEquals(0, config.getOssMaxRetryAttempts());
    }

    @Test
    public void shouldHandleMaxRetryAttemptsNegative() {
        properties.put("SINK_OSS_MAX_RETRY_ATTEMPTS", "-1");
        ObjectStorageServiceConfig config = ConfigFactory.create(ObjectStorageServiceConfig.class, properties);
        Assert.assertEquals(-1, config.getOssMaxRetryAttempts());
    }

    @Test
    public void shouldHandleMaxRetryAttemptsMaxInteger() {
        properties.put("SINK_OSS_MAX_RETRY_ATTEMPTS", String.valueOf(Integer.MAX_VALUE));
        ObjectStorageServiceConfig config = ConfigFactory.create(ObjectStorageServiceConfig.class, properties);
        Assert.assertEquals(Integer.MAX_VALUE, config.getOssMaxRetryAttempts());
    }

    @Test
    public void shouldHandleDifferentOssTypes() {
        properties.put("OSS_TYPE", "DLQ");
        properties.put("DLQ_OSS_ENDPOINT", "http://oss-cn-hangzhou.aliyuncs.com");
        ObjectStorageServiceConfig config = ConfigFactory.create(ObjectStorageServiceConfig.class, properties);
        Assert.assertEquals("http://oss-cn-hangzhou.aliyuncs.com", config.getOssEndpoint());
    }

    @Test
    public void shouldHandleAllConfigurationsTogether() {
        properties.put("SINK_OSS_ENDPOINT", "https://oss-cn-beijing.aliyuncs.com");
        properties.put("SINK_OSS_REGION", "cn-beijing");
        properties.put("SINK_OSS_ACCESS_ID", "test-id");
        properties.put("SINK_OSS_ACCESS_KEY", "test-key");
        properties.put("SINK_OSS_BUCKET_NAME", "test-bucket");
        properties.put("SINK_OSS_DIRECTORY_PREFIX", "test/prefix");
        properties.put("SINK_OSS_SOCKET_TIMEOUT_MS", "10000");
        properties.put("SINK_OSS_CONNECTION_TIMEOUT_MS", "10000");
        properties.put("SINK_OSS_CONNECTION_REQUEST_TIMEOUT_MS", "5000");
        properties.put("SINK_OSS_REQUEST_TIMEOUT_MS", "60000");
        properties.put("SINK_OSS_RETRY_ENABLED", "true");
        properties.put("SINK_OSS_MAX_RETRY_ATTEMPTS", "5");

        ObjectStorageServiceConfig config = ConfigFactory.create(ObjectStorageServiceConfig.class, properties);

        Assert.assertEquals("https://oss-cn-beijing.aliyuncs.com", config.getOssEndpoint());
        Assert.assertEquals("cn-beijing", config.getOssRegion());
        Assert.assertEquals("test-id", config.getOssAccessId());
        Assert.assertEquals("test-key", config.getOssAccessKey());
        Assert.assertEquals("test-bucket", config.getOssBucketName());
        Assert.assertEquals("test/prefix", config.getOssDirectoryPrefix());
        Assert.assertEquals(Integer.valueOf(10000), config.getOssSocketTimeoutMs());
        Assert.assertEquals(Integer.valueOf(10000), config.getOssConnectionTimeoutMs());
        Assert.assertEquals(Integer.valueOf(5000), config.getOssConnectionRequestTimeoutMs());
        Assert.assertEquals(Integer.valueOf(60000), config.getOssRequestTimeoutMs());
        Assert.assertTrue(config.isRetryEnabled());
        Assert.assertEquals(5, config.getOssMaxRetryAttempts());
    }
}
