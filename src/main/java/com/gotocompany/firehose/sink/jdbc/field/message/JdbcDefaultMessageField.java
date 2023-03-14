package com.gotocompany.firehose.sink.jdbc.field.message;

import com.gotocompany.firehose.sink.jdbc.field.JdbcField;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;

public class JdbcDefaultMessageField implements JdbcField {
    private Object columnValue;
    private JsonFormat.Printer jsonPrinter = JsonFormat.printer()
            .omittingInsignificantWhitespace()
            .preservingProtoFieldNames()
            .includingDefaultValueFields();

    public JdbcDefaultMessageField(Object columnValue) {
        this.columnValue = columnValue;
    }

    @Override
    public Object getColumn() throws RuntimeException {
        try {
            columnValue = this.jsonPrinter.print((Message) columnValue);
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
        }
        return columnValue;
    }

    @Override
    public boolean canProcess() {
        return columnValue instanceof Message;
    }
}
