package com.gotocompany.firehose.sink.common.s3;

import com.gotocompany.firehose.config.S3Config;
import com.gotocompany.firehose.sink.common.blobstorage.BlobStorageException;
import com.gotocompany.firehose.sink.common.blobstorage.s3.S3;
import org.aeonbits.owner.ConfigFactory;
import org.junit.Assert;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.io.IOException;
import java.util.HashMap;

public class S3Test {

    @Test
    public void shouldCallStorage() throws BlobStorageException, IOException {
        S3Config s3Config = ConfigFactory.create(S3Config.class, new HashMap<Object, Object>() {{
            put("S3_TYPE", "SOME_TYPE");
            put("SOME_TYPE_S3_BUCKET_NAME", "TestBucket");
            put("SOME_TYPE_S3_REGION", "asia");
        }});
        S3Client s3Client = Mockito.mock(S3Client.class);
        S3 s3Storage = new S3(s3Config, s3Client);
        PutObjectRequest putObject = PutObjectRequest.builder()
                .bucket("TestBucket")
                .key("test")
                .build();
        byte[] content = "test".getBytes();
        s3Storage.store("test", content);
        ArgumentCaptor<PutObjectRequest> putObjectRequestArgumentCaptor = ArgumentCaptor.forClass(PutObjectRequest.class);
        ArgumentCaptor<RequestBody> requestBodyArgumentCaptor = ArgumentCaptor.forClass(RequestBody.class);
        Mockito.verify(s3Client, Mockito.times(1)).putObject(putObjectRequestArgumentCaptor.capture(), requestBodyArgumentCaptor.capture());
        Assert.assertEquals(putObject, putObjectRequestArgumentCaptor.getValue());
        byte[] expectedBytes = new byte[4];
        byte[] actualBytes = new byte[4];
        RequestBody.fromBytes(content).contentStreamProvider().newStream().read(expectedBytes);
        requestBodyArgumentCaptor.getValue().contentStreamProvider().newStream().read(actualBytes);
        Assert.assertEquals(new String(expectedBytes), new String(actualBytes));
    }

    @Test
    public void shouldThrowException() throws IOException {
        S3Config s3Config = ConfigFactory.create(S3Config.class, new HashMap<Object, Object>() {{
            put("S3_TYPE", "SOME_TYPE");
            put("SOME_TYPE_S3_BUCKET_NAME", "TestBucket");
            put("SOME_TYPE_S3_REGION", "asia");
        }});
        S3Client s3Client = Mockito.mock(S3Client.class);
        S3 s3Storage = new S3(s3Config, s3Client);
        PutObjectRequest putObject = PutObjectRequest.builder()
                .bucket("TestBucket")
                .key("test")
                .build();
        byte[] content = "test".getBytes();
        SdkClientException exception = SdkClientException.create("test");
        Mockito.when(s3Client.putObject(Mockito.any(PutObjectRequest.class), Mockito.any(RequestBody.class))).thenThrow(exception);
        BlobStorageException thrown = Assertions
                .assertThrows(BlobStorageException.class, () -> s3Storage.store("test", content), "BlobStorageException error was expected");
        ArgumentCaptor<PutObjectRequest> putObjectRequestArgumentCaptor = ArgumentCaptor.forClass(PutObjectRequest.class);
        ArgumentCaptor<RequestBody> requestBodyArgumentCaptor = ArgumentCaptor.forClass(RequestBody.class);
        Mockito.verify(s3Client, Mockito.times(1)).putObject(putObjectRequestArgumentCaptor.capture(), requestBodyArgumentCaptor.capture());
        Assert.assertEquals(putObject, putObjectRequestArgumentCaptor.getValue());
        byte[] expectedBytes = new byte[4];
        byte[] actualBytes = new byte[4];
        RequestBody.fromBytes(content).contentStreamProvider().newStream().read(expectedBytes);
        requestBodyArgumentCaptor.getValue().contentStreamProvider().newStream().read(actualBytes);
        Assert.assertEquals(new String(expectedBytes), new String(actualBytes));
        Assertions.assertEquals(new BlobStorageException("test", "test", exception), thrown);
    }
}

