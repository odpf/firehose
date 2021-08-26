package io.odpf.firehose.sink.bigquery.handler;


import com.google.cloud.bigquery.InsertAllRequest;
import io.odpf.firehose.sink.bigquery.models.Record;

/**
 * Fetches BQ insertable row from the base record {@link Record}. The implementations can differ if unique rows need to be inserted or not.
 */
public interface BigQueryRow {

    InsertAllRequest.RowToInsert of(Record record);
}

