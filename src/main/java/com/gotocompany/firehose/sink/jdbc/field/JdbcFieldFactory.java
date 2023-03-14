package com.gotocompany.firehose.sink.jdbc.field;

import com.gotocompany.firehose.sink.jdbc.field.message.JdbcDefaultMessageField;
import com.gotocompany.firehose.sink.jdbc.field.message.JdbcCollectionField;
import com.gotocompany.firehose.sink.jdbc.field.message.JdbcTimestampField;
import com.google.protobuf.Descriptors;

import java.util.Arrays;
import java.util.List;

/**
 * Jdbc field factory.
 */
public class JdbcFieldFactory {
    /**
     * Returns the field based on configuration.
     *
     * @param columnValue     the column value
     * @param fieldDescriptor the field descriptor
     * @return the field
     */
    public static JdbcField getField(Object columnValue, Descriptors.FieldDescriptor fieldDescriptor) {
        List<JdbcField> jdbcFields = Arrays.asList(
                new JdbcCollectionField(columnValue, fieldDescriptor),
                new JdbcMapField(columnValue, fieldDescriptor),
                new JdbcTimestampField(columnValue),
                new JdbcDefaultMessageField(columnValue));
        return jdbcFields.stream()
                .filter(JdbcField::canProcess)
                .findFirst()
                .orElseGet(() -> new JdbcDefaultField(columnValue));
    }

}
