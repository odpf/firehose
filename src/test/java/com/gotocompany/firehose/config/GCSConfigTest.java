package com.gotocompany.firehose.config;

import org.aeonbits.owner.ConfigFactory;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class GCSConfigTest {

    @Test
    public void shouldParseConfigForSink() {
        Map<String, String> properties = new HashMap<String, String>() {{
            put("SINK_OBJECT_STORAGE_LOCAL_DIRECTORY", "/tmp/test");
            put("SINK_OBJECT_STORAGE_GCS_GOOGLE_CLOUD_PROJECT_ID", "GCloud-ID");
            put("SINK_OBJECT_STORAGE_GCS_BUCKET_NAME", "testing");
            put("SINK_OBJECT_STORAGE_GCS_CREDENTIAL_PATH", "/tmp/path/to/cred");
            put("GCS_TYPE", "SINK_OBJECT_STORAGE");
        }};
        GCSConfig gcsConfig = ConfigFactory.create(GCSConfig.class, properties);
        Assert.assertEquals("GCloud-ID", gcsConfig.getGCloudProjectID());
        Assert.assertEquals("testing", gcsConfig.getGCSBucketName());
        Assert.assertEquals("/tmp/path/to/cred", gcsConfig.getGCSCredentialPath());
    }

    @Test
    public void shouldParseConfigForDLQ() {
        Map<String, String> properties = new HashMap<String, String>() {{
            put("DLQ_OBJECT_STORAGE_LOCAL_DIRECTORY", "/tmp/test");
            put("DLQ_OBJECT_STORAGE_GCS_GOOGLE_CLOUD_PROJECT_ID", "GCloud-ID");
            put("DLQ_OBJECT_STORAGE_GCS_BUCKET_NAME", "testing");
            put("DLQ_OBJECT_STORAGE_GCS_CREDENTIAL_PATH", "/tmp/path/to/cred");
            put("GCS_TYPE", "DLQ_OBJECT_STORAGE");
        }};
        GCSConfig gcsConfig = ConfigFactory.create(GCSConfig.class, properties);
        Assert.assertEquals("GCloud-ID", gcsConfig.getGCloudProjectID());
        Assert.assertEquals("testing", gcsConfig.getGCSBucketName());
        Assert.assertEquals("/tmp/path/to/cred", gcsConfig.getGCSCredentialPath());
    }
}
