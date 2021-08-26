package io.odpf.firehose.metrics;


public class ObjectStorageMetrics {
    public static final String LOCAL_FILE_OPEN_TOTAL = Metrics.APPLICATION_PREFIX + Metrics.SINK_PREFIX + Metrics.OBJECTSTORAGE_SINK_PREFIX + "file_open_total";
    public static final String LOCAL_FILE_CLOSE_TOTAL = Metrics.APPLICATION_PREFIX + Metrics.SINK_PREFIX + Metrics.OBJECTSTORAGE_SINK_PREFIX + "file_close_total";
    public static final String LOCAL_FILE_CLOSING_TIME_MILLISECONDS = Metrics.APPLICATION_PREFIX + Metrics.SINK_PREFIX + Metrics.OBJECTSTORAGE_SINK_PREFIX + "file_closing_time_milliseconds";
    public static final String LOCAL_FILE_SIZE_BYTES = Metrics.APPLICATION_PREFIX + Metrics.SINK_PREFIX + Metrics.OBJECTSTORAGE_SINK_PREFIX + "local_file_size_bytes";
    public static final String FILE_UPLOAD_TIME_MILLISECONDS = Metrics.APPLICATION_PREFIX + Metrics.SINK_PREFIX + Metrics.OBJECTSTORAGE_SINK_PREFIX + "file_upload_time_milliseconds";
    public static final String FILE_UPLOAD_TOTAL = Metrics.APPLICATION_PREFIX + Metrics.SINK_PREFIX + Metrics.OBJECTSTORAGE_SINK_PREFIX + "file_upload_total";
    public static final String FILE_UPLOAD_BYTES = Metrics.APPLICATION_PREFIX + Metrics.SINK_PREFIX + Metrics.OBJECTSTORAGE_SINK_PREFIX + "file_upload_bytes";

    public static final String TOPIC_TAG = "topic";
    public static final String OBJECT_STORE_ERROR_TYPE_TAG = "error_type";
    public static final String PARTITION_TAG = "partition";
}
