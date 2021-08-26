package io.odpf.firehose.sink.bigquery.handler;

import com.google.cloud.bigquery.InsertAllRequest;
import io.odpf.firehose.sink.bigquery.models.Record;

public class BigQueryRowWithoutInsertId implements BigQueryRow {

    @Override
    public InsertAllRequest.RowToInsert of(Record record) {
        return InsertAllRequest.RowToInsert.of(record.getColumns());
    }
}
