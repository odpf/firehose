package com.gotocompany.firehose.sink.jdbc.field;

/**
 * Jdbc field for JDBC sink.
 */
public interface JdbcField {
    /**
     * Get column value for the field.
     *
     * @return the column
     * @throws RuntimeException the runtime exception
     */
    Object getColumn() throws RuntimeException;

    /**
     * @return true if this jdbc field is set by config
     */
    boolean canProcess();
}
