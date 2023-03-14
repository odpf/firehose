package com.gotocompany.firehose.metrics;


public class BlobStorageMetrics {
    public static final String LOCAL_FILE_OPEN_TOTAL = Metrics.APPLICATION_PREFIX + Metrics.SINK_PREFIX + Metrics.BLOB_SINK_PREFIX + "local_file_open_total";
    public static final String LOCAL_FILE_CLOSE_TOTAL = Metrics.APPLICATION_PREFIX + Metrics.SINK_PREFIX + Metrics.BLOB_SINK_PREFIX + "local_file_close_total";
    public static final String LOCAL_FILE_RECORDS_TOTAL = Metrics.APPLICATION_PREFIX + Metrics.SINK_PREFIX + Metrics.BLOB_SINK_PREFIX + "local_file_records_total";
    public static final String LOCAL_FILE_CLOSING_TIME_MILLISECONDS = Metrics.APPLICATION_PREFIX + Metrics.SINK_PREFIX + Metrics.BLOB_SINK_PREFIX + "local_file_closing_time_milliseconds";
    public static final String LOCAL_FILE_SIZE_BYTES = Metrics.APPLICATION_PREFIX + Metrics.SINK_PREFIX + Metrics.BLOB_SINK_PREFIX + "local_file_size_bytes";
    public static final String FILE_UPLOAD_TIME_MILLISECONDS = Metrics.APPLICATION_PREFIX + Metrics.SINK_PREFIX + Metrics.BLOB_SINK_PREFIX + "remote_file_upload_time_milliseconds";
    public static final String FILE_UPLOAD_TOTAL = Metrics.APPLICATION_PREFIX + Metrics.SINK_PREFIX + Metrics.BLOB_SINK_PREFIX + "remote_file_upload_total";
    public static final String FILE_UPLOAD_BYTES = Metrics.APPLICATION_PREFIX + Metrics.SINK_PREFIX + Metrics.BLOB_SINK_PREFIX + "remote_file_upload_bytes";
    public static final String FILE_UPLOAD_RECORDS_TOTAL = Metrics.APPLICATION_PREFIX + Metrics.SINK_PREFIX + Metrics.BLOB_SINK_PREFIX + "remote_file_upload_records_total";

    public static final String BLOB_STORAGE_ERROR_TYPE_TAG = "error_type";
}
