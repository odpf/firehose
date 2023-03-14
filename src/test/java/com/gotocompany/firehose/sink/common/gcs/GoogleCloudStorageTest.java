package com.gotocompany.firehose.sink.common.gcs;

import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.gotocompany.firehose.config.GCSConfig;
import com.gotocompany.firehose.sink.common.blobstorage.BlobStorageException;
import com.gotocompany.firehose.sink.common.blobstorage.gcs.GoogleCloudStorage;
import org.aeonbits.owner.ConfigFactory;
import org.junit.Assert;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.mockito.Mockito;

import java.util.HashMap;

public class GoogleCloudStorageTest {

    @Test
    public void shouldCallStorage() throws BlobStorageException {
        GCSConfig config = ConfigFactory.create(GCSConfig.class, new HashMap<Object, Object>() {{
            put("GCS_TYPE", "SOME_TYPE");
            put("SOME_TYPE_GCS_BUCKET_NAME", "TestBucket");
            put("SOME_TYPE_GCS_GOOGLE_CLOUD_PROJECT_ID", "projectID");
        }});
        Storage storage = Mockito.mock(Storage.class);
        GoogleCloudStorage gcs = new GoogleCloudStorage(config, storage);
        BlobInfo blobInfo = BlobInfo.newBuilder(BlobId.of("TestBucket", "test")).build();
        gcs.store("test", new byte[]{});
        Mockito.verify(storage, Mockito.times(1)).create(blobInfo, new byte[]{}, Storage.BlobTargetOption.userProject("projectID"));
    }

    @Test
    public void shouldThrowBlobStorageException() {
        GCSConfig config = ConfigFactory.create(GCSConfig.class, new HashMap<Object, Object>() {{
            put("GCS_TYPE", "SOME_TYPE");
            put("SOME_TYPE_GCS_BUCKET_NAME", "TestBucket");
            put("SOME_TYPE_GCS_GOOGLE_CLOUD_PROJECT_ID", "projectID");
        }});
        Storage storage = Mockito.mock(Storage.class);
        GoogleCloudStorage gcs = new GoogleCloudStorage(config, storage);
        BlobInfo blobInfo = BlobInfo.newBuilder(BlobId.of("TestBucket", "test")).build();
        StorageException storageException = new StorageException(401, "some error");
        Mockito.when(storage.create(blobInfo, new byte[]{}, Storage.BlobTargetOption.userProject("projectID"))).thenThrow(storageException);
        BlobStorageException thrown = Assertions
                .assertThrows(BlobStorageException.class, () -> gcs.store("test", new byte[]{}), "BlobStorageException error was expected");
        Assert.assertEquals(new BlobStorageException("UNAUTHORIZED", "GCS Upload failed", storageException), thrown);
        Mockito.verify(storage, Mockito.times(1)).create(blobInfo, new byte[]{}, Storage.BlobTargetOption.userProject("projectID"));
    }
}
