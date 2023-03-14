package com.gotocompany.firehose.sink.jdbc;

import com.zaxxer.hikari.HikariDataSource;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.sql.Connection;
import java.sql.SQLException;

@RunWith(MockitoJUnitRunner.class)
public class HikariJdbcConnectionPoolTest {
    @Mock
    private HikariDataSource hikariDataSource;
    @Mock
    private Connection connection;

    private JdbcConnectionPool jdbcConnectionPool;

    @Before
    public void setup() {
        jdbcConnectionPool = new HikariJdbcConnectionPool(hikariDataSource);
    }

    @Test
    public void shouldGetConnectionFromDataSource() throws SQLException {
        jdbcConnectionPool.getConnection();

        Mockito.verify(hikariDataSource).getConnection();
    }

    @Test
    public void shouldCloseTheConnectionWhenReleased() throws SQLException {
        jdbcConnectionPool.release(connection);

        Mockito.verify(connection).close();
    }

    @Test
    public void shouldCloseDataSourceWhenShutdown() throws SQLException, InterruptedException {
        jdbcConnectionPool.shutdown();

        Mockito.verify(hikariDataSource).close();
    }
}
