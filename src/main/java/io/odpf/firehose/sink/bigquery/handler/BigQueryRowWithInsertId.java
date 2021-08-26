package io.odpf.firehose.sink.bigquery.handler;

import com.google.cloud.bigquery.InsertAllRequest;
import io.odpf.firehose.sink.bigquery.models.Record;

public class BigQueryRowWithInsertId implements BigQueryRow {
    @Override
    public InsertAllRequest.RowToInsert of(Record record) {
        return InsertAllRequest.RowToInsert.of(record.getId(), record.getColumns());
    }
}
