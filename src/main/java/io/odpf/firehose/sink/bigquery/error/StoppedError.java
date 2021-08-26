package io.odpf.firehose.sink.bigquery.error;

import lombok.AllArgsConstructor;

@AllArgsConstructor
/**
 * stopped 200 This status code returns when a job is canceled.
 * This will be returned if a batch of insertion has some bad records
 * which caused the job to be cancelled. Bad records will have some *other* error
 * but rest of records will be marked as "stopped" and can be sent as is
 *
 * https://cloud.google.com/bigquery/docs/error-messages
 * */
public class StoppedError implements ErrorDescriptor {

    private final String reason;

    @Override
    public boolean matches() {
        return reason.equals("stopped");
    }

    @Override
    public String toString() {
        return "Stopped";
    }
}
