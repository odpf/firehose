package io.odpf.firehose.sink.bigquery.converter.fields;

import com.google.protobuf.Descriptors;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class DefaultProtoField implements ProtoField {
    private final Descriptors.FieldDescriptor descriptor;
    private final Object fieldValue;

    // handles primitives, repeated field
    @Override
    public Object getValue() {
        return fieldValue;
    }

    @Override
    public boolean matches() {
        return false;
    }
}
