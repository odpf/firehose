package com.gotocompany.firehose.metrics;

public class Metrics {
    //APPLICATION PREFIX
    public static final String APPLICATION_PREFIX = "firehose_";

    //SOURCE PREFIXES
    public static final String SOURCE_PREFIX = "source_";
    public static final String KAFKA_PREFIX = "kafka_";

    //SINK PREFIXES
    public static final String SINK_PREFIX = "sink_";
    public static final String HTTP_SINK_PREFIX = "http_";
    public static final String GRPC_SINK_PREFIX = "grpc_";
    public static final String BLOB_SINK_PREFIX = "blob_";

    public static final String MONGO_SINK_PREFIX = "mongo_";


    //RETRY PREFIX
    public static final String RETRY_PREFIX = "retry_";

    //DLQ PREFIX
    public static final String DLQ_PREFIX = "dlq_";

    //GLOBAL PREFIX
    public static final String GLOBAL_PREFIX = "global_";

    //PIPELINE PREFIX
    public static final String PIPELINE_PREFIX = "pipeline_";

    //ERROR PREFIX
    public static final String ERROR_PREFIX = "error_";

    // SOURCE MEASUREMENTS
    public static final String SOURCE_KAFKA_MESSAGES_FILTER_TOTAL = APPLICATION_PREFIX + SOURCE_PREFIX + KAFKA_PREFIX + "messages_filter_total";
    public static final String SOURCE_KAFKA_MESSAGES_COMMIT_TOTAL = APPLICATION_PREFIX + SOURCE_PREFIX + KAFKA_PREFIX + "messages_commit_total";
    public static final String SOURCE_KAFKA_PARTITIONS_PROCESS_TIME_MILLISECONDS = APPLICATION_PREFIX + SOURCE_PREFIX + KAFKA_PREFIX + "partitions_process_milliseconds";
    public static final String SOURCE_KAFKA_PULL_BATCH_SIZE_TOTAL = APPLICATION_PREFIX + SOURCE_PREFIX + KAFKA_PREFIX + "pull_batch_size_total";

    // SINK MEASUREMENTS
    public static final String SINK_MESSAGES_TOTAL = APPLICATION_PREFIX + SINK_PREFIX + "messages_total";
    public static final String SINK_RESPONSE_TIME_MILLISECONDS = APPLICATION_PREFIX + SINK_PREFIX + "response_time_milliseconds";
    public static final String SINK_MESSAGES_DROP_TOTAL = APPLICATION_PREFIX + SINK_PREFIX + "messages_drop_total";
    public static final String SINK_HTTP_RESPONSE_CODE_TOTAL = APPLICATION_PREFIX + SINK_PREFIX + HTTP_SINK_PREFIX + "response_code_total";
    public static final String SINK_PUSH_BATCH_SIZE_TOTAL = APPLICATION_PREFIX + SINK_PREFIX + "push_batch_size_total";
    public static final String SINK_GRPC_ERROR_TOTAL = APPLICATION_PREFIX + GRPC_SINK_PREFIX + "error_total";

    // MONGO SINK MEASUREMENTS
    public static final String SINK_MONGO_INSERTED_TOTAL = APPLICATION_PREFIX + SINK_PREFIX + MONGO_SINK_PREFIX + "inserted_total";
    public static final String SINK_MONGO_UPDATED_TOTAL = APPLICATION_PREFIX + SINK_PREFIX + MONGO_SINK_PREFIX + "updated_total";
    public static final String SINK_MONGO_MODIFIED_TOTAL = APPLICATION_PREFIX + SINK_PREFIX + MONGO_SINK_PREFIX + "modified_total";

    // RETRY MEASUREMENT
    public static final String RETRY_MESSAGES_TOTAL = APPLICATION_PREFIX + RETRY_PREFIX + "messages_total";
    public static final String RETRY_ATTEMPTS_TOTAL = APPLICATION_PREFIX + RETRY_PREFIX + "attempts_total";
    public static final String RETRY_SLEEP_TIME_MILLISECONDS = APPLICATION_PREFIX + RETRY_PREFIX + "backoff_sleep_milliseconds";

    // DLQ MEASUREMENTS
    public static final String DLQ_RETRY_ATTEMPTS_TOTAL = APPLICATION_PREFIX + DLQ_PREFIX + RETRY_PREFIX + "attempts_total";
    public static final String DLQ_MESSAGES_TOTAL = APPLICATION_PREFIX + DLQ_PREFIX + "messages_total";

    // GLOBAL MEASUREMENTS
    public static final String GLOBAL_MESSAGES_TOTAL = APPLICATION_PREFIX + GLOBAL_PREFIX + "messages_total";

    // PIPELINE DURATION MEASUREMENTS
    public static final String PIPELINE_END_LATENCY_MILLISECONDS = APPLICATION_PREFIX + PIPELINE_PREFIX + "end_latency_milliseconds";
    public static final String PIPELINE_EXECUTION_LIFETIME_MILLISECONDS = APPLICATION_PREFIX + PIPELINE_PREFIX + "execution_lifetime_milliseconds";

    // ERROR MEASUREMENT
    public static final String ERROR_EVENT = APPLICATION_PREFIX + ERROR_PREFIX + "event";
    public static final String ERROR_MESSAGES_TOTAL = APPLICATION_PREFIX + ERROR_PREFIX + "messages_total";

    // CONSUMER TAGS
    public static final String CONSUMER_GROUP_ID_TAG = "consumer_group_id";

    // EXECUTION TAGS
    public static final String SUCCESS_TAG = "success=true";
    public static final String FAILURE_TAG = "success=false";
    public static final String MESSAGE_TYPE_TAG = "type=%s"; // total, success, failure
    public static final String MESSAGE_SCOPE_TAG = "scope=%s";

    //ERROR TAGS
    public static final String ERROR_TYPE_TAG = "error_type=%s";

    // ERROR TAGS
    public static final String ERROR_MESSAGE_CLASS_TAG = "class";
    public static final String NON_FATAL_ERROR = "nonfatal";
    public static final String FATAL_ERROR = "fatal";

    public static String tag(String key, String value) {
        return String.format("%s=%s", key, value);
    }

    // MESSAGE SCOPE
    public enum MessageScope {
        CONSUMER,
        SINK,
        DLQ,
        IGNORED,
        FILTERED
    }

    // MESSAGE TYPE {
    public enum MessageType {
        TOTAL,
        SUCCESS,
        FAILURE
    }
}
