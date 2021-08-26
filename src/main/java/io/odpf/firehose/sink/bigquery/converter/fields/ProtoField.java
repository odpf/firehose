package io.odpf.firehose.sink.bigquery.converter.fields;

public interface ProtoField {

    Object getValue();

    boolean matches();
}
