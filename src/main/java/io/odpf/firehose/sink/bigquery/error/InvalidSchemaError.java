package io.odpf.firehose.sink.bigquery.error;

import lombok.AllArgsConstructor;

@AllArgsConstructor
/**
 * This error returns when there is any kind of invalid input
 * other than an invalid query, such as missing required fields
 * or an invalid table schema.
 *
 * https://cloud.google.com/bigquery/docs/error-messages
 * */
public class InvalidSchemaError implements ErrorDescriptor {

    private final String reason;
    private final String message;

    @Override
    public boolean matches() {
        return reason.equals("invalid") && message.contains("no such field");
    }

    @Override
    public String toString() {
        return "InvalidSchemaError";
    }
}
