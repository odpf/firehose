package com.gotocompany.firehose.sink.jdbc.field;

public class JdbcDefaultField implements JdbcField {
    private Object columnValue;

    public JdbcDefaultField(Object columnValue) {
        this.columnValue = columnValue;
    }

    @Override
    public Object getColumn() throws RuntimeException {
        return columnValue;
    }

    @Override
    public boolean canProcess() {
        return false;
    }
}
