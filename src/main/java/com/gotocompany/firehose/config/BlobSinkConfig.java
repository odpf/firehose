package com.gotocompany.firehose.config;

import com.gotocompany.firehose.config.converter.BlobSinkFilePartitionTypeConverter;
import com.gotocompany.firehose.config.converter.BlobSinkLocalFileWriterTypeConverter;
import com.gotocompany.firehose.config.converter.BlobStorageTypeConverter;
import com.gotocompany.firehose.sink.common.blobstorage.BlobStorageType;
import com.gotocompany.firehose.sink.blob.Constants;

public interface BlobSinkConfig extends AppConfig {

    @Key("SINK_BLOB_STORAGE_TYPE")
    @DefaultValue("GCS")
    @ConverterClass(BlobStorageTypeConverter.class)
    BlobStorageType getBlobStorageType();

    @Key("SINK_BLOB_LOCAL_DIRECTORY")
    @DefaultValue("/tmp/firehose")
    String getLocalDirectory();

    @Key("SINK_BLOB_LOCAL_FILE_WRITER_TYPE")
    @DefaultValue("parquet")
    @ConverterClass(BlobSinkLocalFileWriterTypeConverter.class)
    Constants.WriterType getLocalFileWriterType();

    @Key("SINK_BLOB_LOCAL_FILE_WRITER_PARQUET_BLOCK_SIZE")
    @DefaultValue("134217728")
    int getLocalFileWriterParquetBlockSize();

    @Key("SINK_BLOB_LOCAL_FILE_WRITER_PARQUET_PAGE_SIZE")
    @DefaultValue("1048576")
    int getLocalFileWriterParquetPageSize();

    @Key("SINK_BLOB_OUTPUT_KAFKA_METADATA_COLUMN_NAME")
    @DefaultValue("")
    String getOutputKafkaMetadataColumnName();

    @Key("SINK_BLOB_OUTPUT_INCLUDE_KAFKA_METADATA_ENABLE")
    @DefaultValue("false")
    boolean getOutputIncludeKafkaMetadataEnable();

    @Key("SINK_BLOB_LOCAL_FILE_ROTATION_DURATION_MS")
    @DefaultValue("3600000")
    long getLocalFileRotationDurationMS();

    @Key("SINK_BLOB_LOCAL_FILE_ROTATION_MAX_SIZE_BYTES")
    @DefaultValue("268435456")
    long getLocalFileRotationMaxSizeBytes();

    @Key("SINK_BLOB_FILE_PARTITION_PROTO_TIMESTAMP_FIELD_NAME")
    String getFilePartitionProtoTimestampFieldName();

    @Key("SINK_BLOB_FILE_PARTITION_TIME_GRANULARITY_TYPE")
    @DefaultValue("day")
    @ConverterClass(BlobSinkFilePartitionTypeConverter.class)
    Constants.FilePartitionType getFilePartitionTimeGranularityType();

    @Key("SINK_BLOB_FILE_PARTITION_PROTO_TIMESTAMP_TIMEZONE")
    @DefaultValue("UTC")
    String getFilePartitionProtoTimestampTimezone();

    @Key("SINK_BLOB_FILE_PARTITION_TIME_DATE_PREFIX")
    @DefaultValue("dt=")
    String getFilePartitionTimeDatePrefix();

    @Key("SINK_BLOB_FILE_PARTITION_TIME_HOUR_PREFIX")
    @DefaultValue("hr=")
    String getFilePartitionTimeHourPrefix();
}
