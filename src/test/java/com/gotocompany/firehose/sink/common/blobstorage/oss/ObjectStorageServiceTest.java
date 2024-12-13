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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

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
        when(oss.putObject(any(PutObjectRequest.class))).thenReturn(null);
        BucketList bucketList = new BucketList();
        bucketList.setBucketList(Collections.singletonList(Mockito.mock(Bucket.class)));
        when(oss.listBuckets(any(ListBucketsRequest.class))).thenReturn(bucketList);
        ObjectStorageService objectStorageService = new ObjectStorageService(objectStorageServiceConfig, oss);

        objectStorageService.store("objectName", "filePath");

        verify(oss, times(1))
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
        when(oss.putObject(any(PutObjectRequest.class))).thenReturn(null);
        BucketList bucketList = new BucketList();
        bucketList.setBucketList(Collections.singletonList(Mockito.mock(Bucket.class)));
        when(oss.listBuckets(any(ListBucketsRequest.class))).thenReturn(bucketList);
        ObjectStorageService objectStorageService = new ObjectStorageService(objectStorageServiceConfig, oss);

        objectStorageService.store("objectName", "content".getBytes());

        ArgumentCaptor<PutObjectRequest> argumentCaptor = ArgumentCaptor.forClass(PutObjectRequest.class);
        verify(oss, times(1))
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
        when(oss.listBuckets(any(ListBucketsRequest.class))).thenReturn(bucketList);
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
        when(oss.putObject(any(PutObjectRequest.class))).thenThrow(new ClientException("client_error"));
        BucketList bucketList = new BucketList();
        bucketList.setBucketList(Collections.singletonList(Mockito.mock(Bucket.class)));
        when(oss.listBuckets(any(ListBucketsRequest.class))).thenReturn(bucketList);
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
        when(oss.putObject(any(PutObjectRequest.class))).thenThrow(new OSSException("server is down"));
        BucketList bucketList = new BucketList();
        bucketList.setBucketList(Collections.singletonList(Mockito.mock(Bucket.class)));
        when(oss.listBuckets(any(ListBucketsRequest.class))).thenReturn(bucketList);
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

    @Test
    public void shouldHandleLargeFileUpload() throws BlobStorageException {
        ObjectStorageServiceConfig config = Mockito.mock(ObjectStorageServiceConfig.class);
        when(config.getOssBucketName()).thenReturn("bucket_name");
        OSS oss = Mockito.spy(OSS.class);
        BucketList bucketList = new BucketList();
        bucketList.setBucketList(Collections.singletonList(Mockito.mock(Bucket.class)));
        when(oss.listBuckets(any())).thenReturn(bucketList);
        ObjectStorageService service = new ObjectStorageService(config, oss);

        byte[] largeContent = new byte[10 * 1024 * 1024];
        service.store("large_file.dat", largeContent);

        verify(oss).putObject(any(PutObjectRequest.class));
    }

    @Test(expected = BlobStorageException.class)
    public void shouldHandleNetworkDisconnection() throws BlobStorageException {
        ObjectStorageServiceConfig config = Mockito.mock(ObjectStorageServiceConfig.class);
        when(config.getOssBucketName()).thenReturn("bucket_name");
        OSS oss = Mockito.spy(OSS.class);
        BucketList bucketList = new BucketList();
        bucketList.setBucketList(Collections.singletonList(Mockito.mock(Bucket.class)));
        when(oss.listBuckets(any())).thenReturn(bucketList);
        when(oss.putObject(any())).thenThrow(new ClientException("Network disconnected"));
        ObjectStorageService service = new ObjectStorageService(config, oss);

        service.store("test.txt", "content".getBytes());
    }

    @Test(expected = BlobStorageException.class)
    public void shouldHandleInvalidCredentials() throws BlobStorageException {
        ObjectStorageServiceConfig config = Mockito.mock(ObjectStorageServiceConfig.class);
        when(config.getOssBucketName()).thenReturn("bucket_name");
        OSS oss = Mockito.spy(OSS.class);
        BucketList bucketList = new BucketList();
        bucketList.setBucketList(Collections.singletonList(Mockito.mock(Bucket.class)));
        when(oss.listBuckets(any())).thenReturn(bucketList);
        when(oss.putObject(any())).thenThrow(new OSSException("InvalidAccessKeyId"));
        ObjectStorageService service = new ObjectStorageService(config, oss);

        service.store("test.txt", "content".getBytes());
    }

    @Test(expected = BlobStorageException.class)
    public void shouldHandleRateLimitExceeded() throws BlobStorageException {
        ObjectStorageServiceConfig config = Mockito.mock(ObjectStorageServiceConfig.class);
        when(config.getOssBucketName()).thenReturn("bucket_name");
        OSS oss = Mockito.spy(OSS.class);
        BucketList bucketList = new BucketList();
        bucketList.setBucketList(Collections.singletonList(Mockito.mock(Bucket.class)));
        when(oss.listBuckets(any())).thenReturn(bucketList);
        when(oss.putObject(any())).thenThrow(new OSSException("RequestTimeTooSkewed"));
        ObjectStorageService service = new ObjectStorageService(config, oss);

        service.store("test.txt", "content".getBytes());
    }

    @Test
    public void shouldHandleEmptyFile() throws BlobStorageException {
        ObjectStorageServiceConfig config = Mockito.mock(ObjectStorageServiceConfig.class);
        when(config.getOssBucketName()).thenReturn("bucket_name");
        OSS oss = Mockito.spy(OSS.class);
        BucketList bucketList = new BucketList();
        bucketList.setBucketList(Collections.singletonList(Mockito.mock(Bucket.class)));
        when(oss.listBuckets(any())).thenReturn(bucketList);
        ObjectStorageService service = new ObjectStorageService(config, oss);

        service.store("empty.txt", new byte[0]);

        verify(oss).putObject(any(PutObjectRequest.class));
    }

    @Test
    public void shouldHandleMultipleDirectoryLevels() throws BlobStorageException {
        ObjectStorageServiceConfig config = Mockito.mock(ObjectStorageServiceConfig.class);
        when(config.getOssBucketName()).thenReturn("bucket_name");
        when(config.getOssDirectoryPrefix()).thenReturn("level1/level2/level3");
        OSS oss = Mockito.spy(OSS.class);
        BucketList bucketList = new BucketList();
        bucketList.setBucketList(Collections.singletonList(Mockito.mock(Bucket.class)));
        when(oss.listBuckets(any())).thenReturn(bucketList);
        ObjectStorageService service = new ObjectStorageService(config, oss);

        service.store("test.txt", "content".getBytes());

        ArgumentCaptor<PutObjectRequest> captor = ArgumentCaptor.forClass(PutObjectRequest.class);
        verify(oss).putObject(captor.capture());
        assertEquals("level1/level2/level3/test.txt", captor.getValue().getKey());
    }


    @Test(expected = BlobStorageException.class)
    public void shouldHandleServerSideError() throws BlobStorageException {
        ObjectStorageServiceConfig config = Mockito.mock(ObjectStorageServiceConfig.class);
        when(config.getOssBucketName()).thenReturn("bucket_name");
        OSS oss = Mockito.spy(OSS.class);
        BucketList bucketList = new BucketList();
        bucketList.setBucketList(Collections.singletonList(Mockito.mock(Bucket.class)));
        when(oss.listBuckets(any())).thenReturn(bucketList);
        when(oss.putObject(any())).thenThrow(new OSSException("InternalError"));
        ObjectStorageService service = new ObjectStorageService(config, oss);

        service.store("test.txt", "content".getBytes());
    }

    @Test(expected = BlobStorageException.class)
    public void shouldHandleBucketPermissionDenied() throws BlobStorageException {
        ObjectStorageServiceConfig config = Mockito.mock(ObjectStorageServiceConfig.class);
        when(config.getOssBucketName()).thenReturn("bucket_name");
        OSS oss = Mockito.spy(OSS.class);
        BucketList bucketList = new BucketList();
        bucketList.setBucketList(Collections.singletonList(Mockito.mock(Bucket.class)));
        when(oss.listBuckets(Mockito.any())).thenReturn(bucketList);
        when(oss.putObject(any())).thenThrow(new OSSException("AccessDenied"));
        ObjectStorageService service = new ObjectStorageService(config, oss);

        service.store("test.txt", "content".getBytes());
    }

    @Test(expected = BlobStorageException.class)
    public void shouldHandleBucketNotFound() throws BlobStorageException {
        ObjectStorageServiceConfig config = Mockito.mock(ObjectStorageServiceConfig.class);
        when(config.getOssBucketName()).thenReturn("nonexistent_bucket");
        OSS oss = Mockito.spy(OSS.class);
        BucketList bucketList = new BucketList();
        bucketList.setBucketList(Collections.singletonList(Mockito.mock(Bucket.class)));
        when(oss.listBuckets(Mockito.any())).thenReturn(bucketList);
        when(oss.putObject(any())).thenThrow(new OSSException("NoSuchBucket"));
        ObjectStorageService service = new ObjectStorageService(config, oss);

        service.store("test.txt", "content".getBytes());
    }

    @Test
    public void shouldHandleVeryLongObjectNames() throws BlobStorageException {
        ObjectStorageServiceConfig config = Mockito.mock(ObjectStorageServiceConfig.class);
        when(config.getOssBucketName()).thenReturn("bucket_name");
        OSS oss = Mockito.spy(OSS.class);
        BucketList bucketList = new BucketList();
        bucketList.setBucketList(Collections.singletonList(Mockito.mock(Bucket.class)));
        when(oss.listBuckets(Mockito.any())).thenReturn(bucketList);
        ObjectStorageService service = new ObjectStorageService(config, oss);

        StringBuilder longName = new StringBuilder();
        for (int i = 0; i < 100; i++) {
            longName.append("very_long_name_");
        }

        service.store(longName.toString(), "content".getBytes());

        verify(oss).putObject(any(PutObjectRequest.class));
    }

    @Test(expected = BlobStorageException.class)
    public void shouldHandleQuotaExceeded() throws BlobStorageException {
        ObjectStorageServiceConfig config = Mockito.mock(ObjectStorageServiceConfig.class);
        when(config.getOssBucketName()).thenReturn("bucket_name");
        OSS oss = Mockito.spy(OSS.class);
        BucketList bucketList = new BucketList();
        bucketList.setBucketList(Collections.singletonList(Mockito.mock(Bucket.class)));
        when(oss.listBuckets(Mockito.any())).thenReturn(bucketList);
        when(oss.putObject(any())).thenThrow(new OSSException("QuotaExceeded"));
        ObjectStorageService service = new ObjectStorageService(config, oss);

        service.store("test.txt", "content".getBytes());
    }

    @Test(expected = BlobStorageException.class)
    public void shouldHandleEntityTooLarge() throws BlobStorageException {
        ObjectStorageServiceConfig config = Mockito.mock(ObjectStorageServiceConfig.class);
        when(config.getOssBucketName()).thenReturn("bucket_name");
        OSS oss = Mockito.spy(OSS.class);
        BucketList bucketList = new BucketList();
        bucketList.setBucketList(Collections.singletonList(Mockito.mock(Bucket.class)));
        when(oss.listBuckets(Mockito.any())).thenReturn(bucketList);
        when(oss.putObject(any())).thenThrow(new OSSException("EntityTooLarge"));
        ObjectStorageService service = new ObjectStorageService(config, oss);

        service.store("test.txt", "content".getBytes());
    }

    @Test
    public void shouldHandleWhitespaceInObjectName() throws BlobStorageException {
        ObjectStorageServiceConfig config = Mockito.mock(ObjectStorageServiceConfig.class);
        when(config.getOssBucketName()).thenReturn("bucket_name");
        OSS oss = Mockito.spy(OSS.class);
        BucketList bucketList = new BucketList();
        bucketList.setBucketList(Collections.singletonList(Mockito.mock(Bucket.class)));
        when(oss.listBuckets(Mockito.any())).thenReturn(bucketList);
        ObjectStorageService service = new ObjectStorageService(config, oss);

        service.store("file with spaces.txt", "content".getBytes());

        ArgumentCaptor<PutObjectRequest> captor = ArgumentCaptor.forClass(PutObjectRequest.class);
        verify(oss).putObject(captor.capture());
        assertEquals("file with spaces.txt", captor.getValue().getKey());
    }

    @Test(expected = BlobStorageException.class)
    public void shouldHandleSignatureDoesNotMatch() throws BlobStorageException {
        ObjectStorageServiceConfig config = Mockito.mock(ObjectStorageServiceConfig.class);
        when(config.getOssBucketName()).thenReturn("bucket_name");
        OSS oss = Mockito.spy(OSS.class);
        BucketList bucketList = new BucketList();
        bucketList.setBucketList(Collections.singletonList(Mockito.mock(Bucket.class)));
        when(oss.listBuckets(Mockito.any())).thenReturn(bucketList);
        when(oss.putObject(any())).thenThrow(new OSSException("SignatureDoesNotMatch"));
        ObjectStorageService service = new ObjectStorageService(config, oss);

        service.store("test.txt", "content".getBytes());
    }

    @Test(expected = BlobStorageException.class)
    public void shouldHandleRequestTimeout() throws BlobStorageException {
        ObjectStorageServiceConfig config = Mockito.mock(ObjectStorageServiceConfig.class);
        when(config.getOssBucketName()).thenReturn("bucket_name");
        OSS oss = Mockito.spy(OSS.class);
        BucketList bucketList = new BucketList();
        bucketList.setBucketList(Collections.singletonList(Mockito.mock(Bucket.class)));
        when(oss.listBuckets(Mockito.any())).thenReturn(bucketList);
        when(oss.putObject(any())).thenThrow(new ClientException("RequestTimeout"));
        ObjectStorageService service = new ObjectStorageService(config, oss);

        service.store("test.txt", "content".getBytes());
    }
}
