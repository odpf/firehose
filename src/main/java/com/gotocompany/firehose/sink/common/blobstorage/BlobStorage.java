package com.gotocompany.firehose.sink.common.blobstorage;

/**
 * Abstraction of any storage that store binary bytes as file.
 */
public interface BlobStorage {
    void store(String objectName, String filePath) throws BlobStorageException;

    void store(String objectName, byte[] content) throws BlobStorageException;
}
