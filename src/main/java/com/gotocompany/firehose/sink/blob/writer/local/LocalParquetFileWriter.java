package com.gotocompany.firehose.sink.blob.writer.local;

import com.google.protobuf.Descriptors;
import com.gotocompany.firehose.config.BlobSinkConfig;
import com.gotocompany.firehose.sink.blob.message.Record;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.proto.ProtoParquetWriter;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class LocalParquetFileWriter implements LocalFileWriter {

    private final ParquetWriter parquetWriter;
    private final long createdTimestampMillis;
    private final String fullPath;
    private final String basePath;
    private long recordCount = 0;
    private boolean isClosed = false;
    private final BlobSinkConfig sinkConfig;

    public LocalParquetFileWriter(long createdTimestampMillis, String basePath, String fullPath, BlobSinkConfig sinkConfig, Descriptors.Descriptor messageDescriptor, List<Descriptors.FieldDescriptor> metadataFieldDescriptor) throws IOException {
        this.parquetWriter = new ProtoParquetWriter(new Path(fullPath),
                messageDescriptor,
                metadataFieldDescriptor,
                CompressionCodecName.GZIP,
                sinkConfig.getLocalFileWriterParquetBlockSize(),
                sinkConfig.getLocalFileWriterParquetPageSize());
        this.createdTimestampMillis = createdTimestampMillis;
        this.fullPath = fullPath;
        this.basePath = basePath;
        this.sinkConfig = sinkConfig;
    }

    @Override
    public LocalFileMetadata getMetadata() {
        return new LocalFileMetadata(
                basePath,
                fullPath,
                createdTimestampMillis,
                recordCount,
                parquetWriter.getDataSize());
    }

    public synchronized boolean write(Record record) throws IOException {
        if (isClosed) {
            return false;
        }
        if (sinkConfig.getOutputIncludeKafkaMetadataEnable()) {
            parquetWriter.write(Arrays.asList(record.getMessage(), record.getMetadata()));
        } else {
            parquetWriter.write(record.getMessage());
        }
        recordCount++;
        return true;
    }

    @Override
    public synchronized void close() throws IOException {
        this.isClosed = true;
        parquetWriter.close();
    }

    @Override
    public synchronized LocalFileMetadata closeAndFetchMetaData() throws IOException {
        LocalFileMetadata metadata = getMetadata();
        this.close();
        return metadata;
    }

    @Override
    public String getFullPath() {
        return fullPath;
    }
}
