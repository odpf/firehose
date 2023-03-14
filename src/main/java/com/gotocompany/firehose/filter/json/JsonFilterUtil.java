package com.gotocompany.firehose.filter.json;

import com.gotocompany.firehose.metrics.FirehoseInstrumentation;
import com.gotocompany.firehose.config.FilterConfig;
import lombok.experimental.UtilityClass;

import static com.gotocompany.firehose.config.enums.FilterMessageFormatType.PROTOBUF;

/**
 * The type Json filter util.
 */
@UtilityClass
public class JsonFilterUtil {

    /**
     * Log configs.
     *
     * @param filterConfig    the filter config
     * @param firehoseInstrumentation the instrumentation
     */
    public static void logConfigs(FilterConfig filterConfig, FirehoseInstrumentation firehoseInstrumentation) {
        firehoseInstrumentation.logInfo("\n\tFilter data source type: {}", filterConfig.getFilterDataSource());
        firehoseInstrumentation.logInfo("\n\tFilter JSON Schema: {}", filterConfig.getFilterJsonSchema());
        firehoseInstrumentation.logInfo("\n\tFilter ESB message format: {}", filterConfig.getFilterESBMessageFormat());
        if (filterConfig.getFilterESBMessageFormat() == PROTOBUF) {
            firehoseInstrumentation.logInfo("\n\tMessage Proto class: {}", filterConfig.getFilterSchemaProtoClass());
        }
    }

    /**
     * Validate configs.
     *
     * @param filterConfig    the filter config
     * @param firehoseInstrumentation the instrumentation
     */
    public static void validateConfigs(FilterConfig filterConfig, FirehoseInstrumentation firehoseInstrumentation) {
        if (filterConfig.getFilterJsonSchema() == null) {
            firehoseInstrumentation.logError("Failed to create filter due to invalid config");
            throw new IllegalArgumentException("Filter JSON Schema is invalid");
        }
        if (filterConfig.getFilterESBMessageFormat() == null) {
            firehoseInstrumentation.logError("Failed to create filter due to invalid config");
            throw new IllegalArgumentException("Filter ESB message type cannot be null");
        }
        if (filterConfig.getFilterESBMessageFormat() == PROTOBUF && filterConfig.getFilterSchemaProtoClass() == null) {
            firehoseInstrumentation.logError("Failed to create filter due to invalid config");
            throw new IllegalArgumentException("Proto Schema class cannot be null");
        }
    }
}
