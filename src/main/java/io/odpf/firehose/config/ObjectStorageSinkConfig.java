package io.odpf.firehose.config;

import io.odpf.firehose.config.converter.ObjectStorageSinkLocalFileWriterTypeConverter;
import io.odpf.firehose.config.converter.ObjectStorageSinkPartitioningTypeConverter;
import io.odpf.firehose.config.converter.ObjectStorageTypeConverter;
import io.odpf.firehose.objectstorage.ObjectStorageType;
import io.odpf.firehose.sink.objectstorage.Constants;

public interface ObjectStorageSinkConfig extends AppConfig {

    @Key("SINK_OBJECT_STORAGE_LOCAL_DIRECTORY")
    String getLocalDirectory();

    @Key("SINK_OBJECT_STORAGE_LOCAL_FILE_WRITER_TYPE")
    @DefaultValue("parquet")
    @ConverterClass(ObjectStorageSinkLocalFileWriterTypeConverter.class)
    Constants.WriterType getFileWriterType();

    @Key("SINK_OBJECT_STORAGE_KAFKA_METADATA_COLUMN_NAME")
    @DefaultValue("")
    String getKafkaMetadataColumnName();

    @Key("SINK_OBJECT_STORAGE_WRITE_KAFKA_METADATA")
    boolean getWriteKafkaMetadata();

    @Key("SINK_OBJECT_STORAGE_WRITER_BLOCK_SIZE")
    @DefaultValue("134217728")
    int getWriterBlockSize();

    @Key("SINK_OBJECT_STORAGE_WRITER_PAGE_SIZE")
    @DefaultValue("1048576")
    int getWriterPageSize();

    @Key("SINK_OBJECT_STORAGE_ROTATION_DURATION_MS")
    @DefaultValue("3600000")
    long getFileRotationDurationMS();

    @Key("SINK_OBJECT_STORAGE_ROTATION_MAX_SIZE_BYTES")
    @DefaultValue("268435456")
    long getFileRotationMaxSizeBytes();

    @Key("SINK_OBJECT_STORAGE_TIME_PARTITIONING_FIELD_NAME")
    String getTimePartitioningFieldName();

    @Key("SINK_OBJECT_STORAGE_TIME_PARTITIONING_TIME_ZONE")
    @DefaultValue("UTC")
    String getTimePartitioningTimeZone();

    @Key("SINK_OBJECT_STORAGE_TIME_PARTITIONING_DATE_PREFIX")
    @DefaultValue("dt=")
    String getTimePartitioningDatePrefix();

    @Key("SINK_OBJECT_STORAGE_TIME_PARTITIONING_TYPE")
    @DefaultValue("day")
    @ConverterClass(ObjectStorageSinkPartitioningTypeConverter.class)
    Constants.PartitioningType getPartitioningType();

    @Key("SINK_OBJECT_STORAGE_TIME_PARTITIONING_HOUR_PREFIX")
    @DefaultValue("hr=")
    String getTimePartitioningHourPrefix();

    @Key("SINK_OBJECT_STORAGE_TYPE")
    @DefaultValue("GCS")
    @ConverterClass(ObjectStorageTypeConverter.class)
    ObjectStorageType getObjectStorageType();
}
