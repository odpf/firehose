package com.gotocompany.firehose.sink.jdbc;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Wraps Hikari database connection pool as a JDBCConnectionPool.
 */
public class HikariJdbcConnectionPool implements JdbcConnectionPool {

    private static final Integer CONNECTION_TIMEOUT_THRESHOLD = 250;
    private static final Integer IDLE_TIMEOUT_THRESHOLD = 0;
    private final HikariDataSource hikariDataSource;

    /**
     * Instantiates a new Hikari jdbc connection pool.
     *
     * @param jdbcUrl           the jdbc url
     * @param username          the username
     * @param password          the password
     * @param maximumPoolSize   the maximum pool size
     * @param connectionTimeout the connection timeout
     * @param idleTimeout       the idle timeout
     * @param minimumIdle       the minimum idle
     */
    public HikariJdbcConnectionPool(String jdbcUrl, String username, String password, int maximumPoolSize,
                                    long connectionTimeout, long idleTimeout, int minimumIdle) {
        HikariConfig config = new HikariConfig();
        config.setRegisterMbeans(true);
        config.setJdbcUrl(jdbcUrl);
        config.setUsername(username);
        config.setPassword(password);
        config.setMaximumPoolSize(maximumPoolSize);
        config.setMinimumIdle(minimumIdle);
        if (connectionTimeout >= CONNECTION_TIMEOUT_THRESHOLD) {
            config.setConnectionTimeout(connectionTimeout);
        }
        if (idleTimeout >= IDLE_TIMEOUT_THRESHOLD) {
            config.setIdleTimeout(idleTimeout);
        }
        hikariDataSource = new HikariDataSource(config);
    }

    HikariJdbcConnectionPool(HikariDataSource hikariDataSource) {
        this.hikariDataSource = hikariDataSource;
    }

    @Override
    public Connection getConnection() throws SQLException {
        return hikariDataSource.getConnection();
    }

    @Override
    public void release(Connection connection) throws SQLException {
        connection.close();
    }

    @Override
    public void shutdown() {
        hikariDataSource.close();
    }
}
