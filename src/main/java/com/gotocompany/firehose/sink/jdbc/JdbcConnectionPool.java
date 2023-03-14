package com.gotocompany.firehose.sink.jdbc;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Interface for database connection pool.
 */
public interface JdbcConnectionPool {
    /**
     * Get connection from the pool.
     *
     * @return the connection
     * @throws SQLException the sql exception
     */
    Connection getConnection() throws SQLException;

    /**
     * Release the connection held.
     *
     * @param connection the connection
     */
    void release(Connection connection) throws SQLException;

    /**
     * Shutdown the connection pool.
     *
     * @throws InterruptedException the interrupted exception
     */
    void shutdown() throws InterruptedException;
}
