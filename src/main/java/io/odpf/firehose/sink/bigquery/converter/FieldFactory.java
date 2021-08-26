package io.odpf.firehose.sink.bigquery.converter;

import com.google.protobuf.Descriptors;
import io.odpf.firehose.sink.bigquery.converter.fields.ByteField;
import io.odpf.firehose.sink.bigquery.converter.fields.DefaultProtoField;
import io.odpf.firehose.sink.bigquery.converter.fields.EnumField;
import io.odpf.firehose.sink.bigquery.converter.fields.NestedField;
import io.odpf.firehose.sink.bigquery.converter.fields.ProtoField;
import io.odpf.firehose.sink.bigquery.converter.fields.StructField;
import io.odpf.firehose.sink.bigquery.converter.fields.TimestampField;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

public class FieldFactory {

    public static ProtoField getField(Descriptors.FieldDescriptor descriptor, Object fieldValue) {
        List<ProtoField> protoFields = Arrays.asList(
                new TimestampField(descriptor, fieldValue),
                new EnumField(descriptor, fieldValue),
                new ByteField(descriptor, fieldValue),
                new StructField(descriptor, fieldValue),
                new NestedField(descriptor, fieldValue)
        );
        Optional<ProtoField> first = protoFields
                .stream()
                .filter(ProtoField::matches)
                .findFirst();
        return first.orElseGet(() -> new DefaultProtoField(descriptor, fieldValue));
    }

}
