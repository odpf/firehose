package com.gotocompany.firehose.sink.common.blobstorage.oss;

import com.aliyun.oss.ClientException;
import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClient;
import com.aliyun.oss.OSSException;
import com.aliyun.oss.common.auth.Credentials;
import com.aliyun.oss.common.auth.DefaultCredentialProvider;
import com.aliyun.oss.common.comm.ServiceClient;
import com.aliyun.oss.internal.OSSOperation;
import com.aliyun.oss.model.Bucket;
import com.aliyun.oss.model.BucketList;
import com.aliyun.oss.model.ListBucketsRequest;
import com.aliyun.oss.model.PutObjectRequest;
import com.gotocompany.firehose.config.ObjectStorageServiceConfig;
import com.gotocompany.firehose.sink.common.blobstorage.BlobStorageException;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

public class ObjectStorageServiceTest {

    @Test
    public void shouldInitializeOssGivenObjectStorageServiceConfig() throws NoSuchFieldException, IllegalAccessException {
        ObjectStorageServiceConfig objectStorageServiceConfig = Mockito.mock(ObjectStorageServiceConfig.class);
        when(objectStorageServiceConfig.getOssEndpoint()).thenReturn("http://localhost:9000");
        when(objectStorageServiceConfig.getOssRegion()).thenReturn("ap-southeast-5");
        when(objectStorageServiceConfig.getOssAccessId()).thenReturn("accessId");
        when(objectStorageServiceConfig.getOssAccessKey()).thenReturn("accessKey");

        OSSClient oss = (OSSClient) ObjectStorageService.initializeOss(objectStorageServiceConfig);
        Field credentialProviderField = oss.getClass().getDeclaredField("credsProvider");
        credentialProviderField.setAccessible(true);
        Field credentialsField = DefaultCredentialProvider.class.getDeclaredField("creds");
        credentialsField.setAccessible(true);
        Field endpointField = oss.getClass().getDeclaredField("endpoint");
        endpointField.setAccessible(true);
        Field ossBucketOperationField = oss.getClass().getDeclaredField("bucketOperation");
        ossBucketOperationField.setAccessible(true);
        Field region = OSSOperation.class.getDeclaredField("region");
        region.setAccessible(true);

        Credentials credentials = (Credentials) credentialsField.get(credentialProviderField.get(oss));
        assertEquals("http://localhost:9000", endpointField.get(oss).toString());
        assertEquals("ap-southeast-5", region.get(ossBucketOperationField.get(oss)));
        assertEquals("accessId", credentials.getAccessKeyId());
        assertEquals("accessKey", credentials.getSecretAccessKey());
    }

    @Test
    public void shouldInitializeOssGivenObjectStorageServiceConfigWithRetryConfig() throws NoSuchFieldException, IllegalAccessException {
        ObjectStorageServiceConfig objectStorageServiceConfig = Mockito.mock(ObjectStorageServiceConfig.class);
        when(objectStorageServiceConfig.getOssEndpoint()).thenReturn("http://localhost:9000");
        when(objectStorageServiceConfig.getOssRegion()).thenReturn("ap-southeast-5");
        when(objectStorageServiceConfig.getOssAccessId()).thenReturn("accessId");
        when(objectStorageServiceConfig.getOssAccessKey()).thenReturn("accessKey");
        when(objectStorageServiceConfig.isRetryEnabled()).thenReturn(true);
        when(objectStorageServiceConfig.getOssMaxRetryAttempts()).thenReturn(3);

        OSSClient oss = (OSSClient) ObjectStorageService.initializeOss(objectStorageServiceConfig);
        Field serviceClient = oss.getClass().getDeclaredField("serviceClient");
        serviceClient.setAccessible(true);
        Field credentialProviderField = oss.getClass().getDeclaredField("credsProvider");
        credentialProviderField.setAccessible(true);
        Field credentialsField = DefaultCredentialProvider.class.getDeclaredField("creds");
        credentialsField.setAccessible(true);
        Field endpointField = oss.getClass().getDeclaredField("endpoint");
        endpointField.setAccessible(true);
        Field ossBucketOperationField = oss.getClass().getDeclaredField("bucketOperation");
        ossBucketOperationField.setAccessible(true);
        Field region = OSSOperation.class.getDeclaredField("region");
        region.setAccessible(true);

        Credentials credentials = (Credentials) credentialsField.get(credentialProviderField.get(oss));
        assertEquals("http://localhost:9000", endpointField.get(oss).toString());
        assertEquals("ap-southeast-5", region.get(ossBucketOperationField.get(oss)));
        assertEquals("accessId", credentials.getAccessKeyId());
        assertEquals("accessKey", credentials.getSecretAccessKey());
        assertEquals(3, ((ServiceClient) serviceClient.get(oss)).getClientConfiguration().getMaxErrorRetry());
    }

    @Test
    public void shouldStoreObjectGivenFilePath() throws BlobStorageException {
        ObjectStorageServiceConfig objectStorageServiceConfig = Mockito.mock(ObjectStorageServiceConfig.class);
        when(objectStorageServiceConfig.getOssEndpoint()).thenReturn("http://localhost:9000");
        when(objectStorageServiceConfig.getOssRegion()).thenReturn("ap-southeast-5");
        when(objectStorageServiceConfig.getOssAccessId()).thenReturn("accessId");
        when(objectStorageServiceConfig.getOssAccessKey()).thenReturn("accessKey");
        when(objectStorageServiceConfig.getOssBucketName()).thenReturn("bucket_name");
        when(objectStorageServiceConfig.getOssDirectoryPrefix()).thenReturn("dir_prefix");
        OSS oss = Mockito.spy(OSS.class);
        ArgumentCaptor<PutObjectRequest> argumentCaptor = ArgumentCaptor.forClass(PutObjectRequest.class);
        when(oss.putObject(Mockito.any(PutObjectRequest.class))).thenReturn(null);
        BucketList bucketList = new BucketList();
        bucketList.setBucketList(Collections.singletonList(Mockito.mock(Bucket.class)));
        when(oss.listBuckets(Mockito.any(ListBucketsRequest.class))).thenReturn(bucketList);
        ObjectStorageService objectStorageService = new ObjectStorageService(objectStorageServiceConfig, oss);

        objectStorageService.store("objectName", "filePath");

        Mockito.verify(oss, Mockito.times(1))
                .putObject(argumentCaptor.capture());
        assertEquals("bucket_name", argumentCaptor.getValue().getBucketName());
        assertEquals("dir_prefix/objectName", argumentCaptor.getValue().getKey());
        assertEquals(new File("filePath"), argumentCaptor.getValue().getFile());
    }

    @Test
    public void shouldStoreObjectGivenFileContent() throws BlobStorageException, IOException {
        ObjectStorageServiceConfig objectStorageServiceConfig = Mockito.mock(ObjectStorageServiceConfig.class);
        when(objectStorageServiceConfig.getOssEndpoint()).thenReturn("http://localhost:9000");
        when(objectStorageServiceConfig.getOssRegion()).thenReturn("ap-southeast-5");
        when(objectStorageServiceConfig.getOssAccessId()).thenReturn("accessId");
        when(objectStorageServiceConfig.getOssAccessKey()).thenReturn("accessKey");
        when(objectStorageServiceConfig.getOssBucketName()).thenReturn("bucket_name");
        when(objectStorageServiceConfig.getOssDirectoryPrefix()).thenReturn("dir_prefix");
        OSS oss = Mockito.spy(OSS.class);
        when(oss.putObject(Mockito.any(PutObjectRequest.class))).thenReturn(null);
        BucketList bucketList = new BucketList();
        bucketList.setBucketList(Collections.singletonList(Mockito.mock(Bucket.class)));
        when(oss.listBuckets(Mockito.any(ListBucketsRequest.class))).thenReturn(bucketList);
        ObjectStorageService objectStorageService = new ObjectStorageService(objectStorageServiceConfig, oss);

        objectStorageService.store("objectName", "content".getBytes());

        ArgumentCaptor<PutObjectRequest> argumentCaptor = ArgumentCaptor.forClass(PutObjectRequest.class);
        Mockito.verify(oss, Mockito.times(1))
                .putObject(argumentCaptor.capture());
        assertEquals("bucket_name", argumentCaptor.getValue().getBucketName());
        assertEquals("dir_prefix/objectName", argumentCaptor.getValue().getKey());
        InputStream inputStream = argumentCaptor.getValue().getInputStream();
        assertEquals("content", getContent(inputStream));
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowIllegalArgumentExceptionWhenGivenBucketIsNotExists() throws BlobStorageException {
        ObjectStorageServiceConfig objectStorageServiceConfig = Mockito.mock(ObjectStorageServiceConfig.class);
        when(objectStorageServiceConfig.getOssEndpoint()).thenReturn("http://localhost:9000");
        when(objectStorageServiceConfig.getOssRegion()).thenReturn("ap-southeast-5");
        when(objectStorageServiceConfig.getOssAccessId()).thenReturn("accessId");
        when(objectStorageServiceConfig.getOssAccessKey()).thenReturn("accessKey");
        when(objectStorageServiceConfig.getOssBucketName()).thenReturn("bucket_name");
        when(objectStorageServiceConfig.getOssDirectoryPrefix()).thenReturn("dir_prefix");
        OSS oss = Mockito.spy(OSS.class);
        BucketList bucketList = new BucketList();
        bucketList.setBucketList(new ArrayList<>());
        when(oss.listBuckets(Mockito.any(ListBucketsRequest.class))).thenReturn(bucketList);
        ObjectStorageService objectStorageService = new ObjectStorageService(objectStorageServiceConfig, oss);

        objectStorageService.store("objectName", "content".getBytes());
    }

    @Test(expected = BlobStorageException.class)
    public void shouldWrapToBlobStorageExceptionWhenClientExceptionIsThrown() throws BlobStorageException {
        ObjectStorageServiceConfig objectStorageServiceConfig = Mockito.mock(ObjectStorageServiceConfig.class);
        when(objectStorageServiceConfig.getOssEndpoint()).thenReturn("http://localhost:9000");
        when(objectStorageServiceConfig.getOssRegion()).thenReturn("ap-southeast-5");
        when(objectStorageServiceConfig.getOssAccessId()).thenReturn("accessId");
        when(objectStorageServiceConfig.getOssAccessKey()).thenReturn("accessKey");
        when(objectStorageServiceConfig.getOssBucketName()).thenReturn("bucket_name");
        when(objectStorageServiceConfig.getOssDirectoryPrefix()).thenReturn("dir_prefix");
        OSS oss = Mockito.spy(OSS.class);
        when(oss.putObject(Mockito.any(PutObjectRequest.class))).thenThrow(new ClientException("client_error"));
        BucketList bucketList = new BucketList();
        bucketList.setBucketList(Collections.singletonList(Mockito.mock(Bucket.class)));
        when(oss.listBuckets(Mockito.any(ListBucketsRequest.class))).thenReturn(bucketList);
        ObjectStorageService objectStorageService = new ObjectStorageService(objectStorageServiceConfig, oss);

        objectStorageService.store("objectName", "content".getBytes());
    }

    @Test(expected = BlobStorageException.class)
    public void shouldWrapToBlobStorageExceptionWhenOSSExceptionIsThrown() throws BlobStorageException {
        ObjectStorageServiceConfig objectStorageServiceConfig = Mockito.mock(ObjectStorageServiceConfig.class);
        when(objectStorageServiceConfig.getOssEndpoint()).thenReturn("http://localhost:9000");
        when(objectStorageServiceConfig.getOssRegion()).thenReturn("ap-southeast-5");
        when(objectStorageServiceConfig.getOssAccessId()).thenReturn("accessId");
        when(objectStorageServiceConfig.getOssAccessKey()).thenReturn("accessKey");
        when(objectStorageServiceConfig.getOssBucketName()).thenReturn("bucket_name");
        when(objectStorageServiceConfig.getOssDirectoryPrefix()).thenReturn("dir_prefix");
        OSS oss = Mockito.spy(OSS.class);
        when(oss.putObject(Mockito.any(PutObjectRequest.class))).thenThrow(new OSSException("server is down"));
        BucketList bucketList = new BucketList();
        bucketList.setBucketList(Collections.singletonList(Mockito.mock(Bucket.class)));
        when(oss.listBuckets(Mockito.any(ListBucketsRequest.class))).thenReturn(bucketList);
        ObjectStorageService objectStorageService = new ObjectStorageService(objectStorageServiceConfig, oss);

        objectStorageService.store("objectName", "content".getBytes());
    }

    private static String getContent(InputStream inputStream) throws IOException {
        ByteArrayOutputStream result = new ByteArrayOutputStream();
        byte[] buffer = new byte[1024];
        int length;
        while ((length = inputStream.read(buffer)) != -1) {
            result.write(buffer, 0, length);
        }
        return result.toString("UTF-8");
    }

}
