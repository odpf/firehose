package com.gotocompany.firehose.config;

import org.aeonbits.owner.ConfigFactory;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class S3ConfigTest {
    @Test
    public void shouldParseConfigForSink() {
        Map<String, String> properties = new HashMap<String, String>() {{
            put("SINK_OBJECT_STORAGE_LOCAL_DIRECTORY", "/tmp/test");
            put("SINK_OBJECT_STORAGE_S3_REGION", "test_region");
            put("SINK_OBJECT_STORAGE_S3_BUCKET_NAME", "test_bucket");
            put("SINK_OBJECT_STORAGE_S3_ACCESS_KEY", "access_key");
            put("SINK_OBJECT_STORAGE_S3_SECRET_KEY", "secret_key");
            put("S3_TYPE", "SINK_OBJECT_STORAGE");
        }};
        S3Config s3Config = ConfigFactory.create(S3Config.class, properties);
        Assert.assertEquals("test_region", s3Config.getS3Region());
        Assert.assertEquals("test_bucket", s3Config.getS3BucketName());
        Assert.assertEquals("access_key", s3Config.getS3AccessKey());
        Assert.assertEquals("secret_key", s3Config.getS3SecretKey());
    }

    @Test
    public void shouldParseConfigForDLQ() {
        Map<String, String> properties = new HashMap<String, String>() {{
            put("DLQ_OBJECT_STORAGE_LOCAL_DIRECTORY", "/tmp/test");
            put("DLQ_OBJECT_STORAGE_S3_REGION", "test_region");
            put("DLQ_OBJECT_STORAGE_S3_BUCKET_NAME", "test_bucket");
            put("DLQ_OBJECT_STORAGE_S3_ACCESS_KEY", "access_key");
            put("DLQ_OBJECT_STORAGE_S3_SECRET_KEY", "secret_key");
            put("S3_TYPE", "DLQ_OBJECT_STORAGE");
        }};
        S3Config s3Config = ConfigFactory.create(S3Config.class, properties);
        Assert.assertEquals("test_region", s3Config.getS3Region());
        Assert.assertEquals("test_bucket", s3Config.getS3BucketName());
        Assert.assertEquals("access_key", s3Config.getS3AccessKey());
        Assert.assertEquals("secret_key", s3Config.getS3SecretKey());
    }
}
