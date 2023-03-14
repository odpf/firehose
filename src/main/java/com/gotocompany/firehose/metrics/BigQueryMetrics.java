package com.gotocompany.firehose.metrics;

public class BigQueryMetrics {
    public enum BigQueryAPIType {
        TABLE_UPDATE,
        TABLE_CREATE,
        DATASET_UPDATE,
        DATASET_CREATE,
        TABLE_INSERT_ALL,
    }

    public enum BigQueryErrorType {
        UNKNOWN_ERROR,
        INVALID_SCHEMA_ERROR,
        OOB_ERROR,
        STOPPED_ERROR,
    }

    public static final String BIGQUERY_SINK_PREFIX = "bigquery_";
    public static final String BIGQUERY_TABLE_TAG = "table=%s";
    public static final String BIGQUERY_DATASET_TAG = "dataset=%s";
    public static final String BIGQUERY_API_TAG = "api=%s";
    public static final String BIGQUERY_ERROR_TAG = "error=%s";
    // BigQuery SINK MEASUREMENTS
    public static final String SINK_BIGQUERY_OPERATION_TOTAL = Metrics.APPLICATION_PREFIX + Metrics.SINK_PREFIX + BIGQUERY_SINK_PREFIX + "operation_total";
    public static final String SINK_BIGQUERY_OPERATION_LATENCY_MILLISECONDS = Metrics.APPLICATION_PREFIX + Metrics.SINK_PREFIX + BIGQUERY_SINK_PREFIX + "operation_latency_milliseconds";
    public static final String SINK_BIGQUERY_ERRORS_TOTAL = Metrics.APPLICATION_PREFIX + Metrics.SINK_PREFIX + BIGQUERY_SINK_PREFIX + "errors_total";

}
