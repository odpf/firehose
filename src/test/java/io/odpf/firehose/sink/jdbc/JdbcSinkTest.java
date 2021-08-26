package io.odpf.firehose.sink.jdbc;


import io.odpf.firehose.consumer.Message;
import io.odpf.firehose.exception.DeserializerException;
import io.odpf.firehose.metrics.Instrumentation;
import io.odpf.stencil.client.StencilClient;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

import static junit.framework.TestCase.assertEquals;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class JdbcSinkTest {

    @Mock
    private QueryTemplate queryTemplate;

    private JdbcSink jdbcSink;

    @Mock
    private StencilClient stencilClient;

    @Mock
    private Instrumentation instrumentation;

    @Mock
    private JdbcConnectionPool jdbcConnectionPool;

    @Mock
    private Connection connection;

    @Mock
    private Statement statement;

    @Before
    public void setUp() throws SQLException {
        when(jdbcConnectionPool.getConnection()).thenReturn(connection);
        when(connection.createStatement()).thenReturn(statement);
        jdbcSink = new JdbcSink(instrumentation, "db", jdbcConnectionPool, queryTemplate, stencilClient);
    }

    @Test
    public void shouldPopulateQueryString() throws IOException, DeserializerException, SQLException {
        Message message = new Message("key".getBytes(), "msg".getBytes(), "topic1", 0, 100);
        jdbcSink.pushMessage(Arrays.asList(message));

        verify(queryTemplate, times(1)).toQueryString(any(Message.class));
    }

    @Test
    public void shouldUseBatchForPushMessage() throws SQLException, IOException, DeserializerException {
        List<Message> messages = Arrays.asList(new Message(new byte[0], new byte[0], "topic", 0, 100),
                new Message(new byte[0], new byte[0], "topic", 0, 100));
        jdbcSink.pushMessage(messages);

        verify(instrumentation, times(1)).startExecution();
        verify(instrumentation, times(1)).captureSuccessExecutionTelemetry("db", messages.size());
    }

    @Test
    public void shouldCallStartExecutionBeforeCaptureSuccessAttempt() throws IOException, DeserializerException, SQLException {
        List<Message> messages = Arrays.asList(new Message(new byte[0], new byte[0], "topic", 0, 100),
                new Message(new byte[0], new byte[0], "topic", 0, 100));
        jdbcSink.pushMessage(messages);

        verify(instrumentation, times(1)).startExecution();
        verify(instrumentation, times(1)).captureSuccessExecutionTelemetry("db", messages.size());
        InOrder inOrder = inOrder(instrumentation);
        inOrder.verify(instrumentation).startExecution();
        inOrder.verify(instrumentation).captureSuccessExecutionTelemetry("db", messages.size());
    }

    @Test
    public void shouldReturnEmptyListWhenNoException() throws IOException, DeserializerException {
        List<Message> messages = Arrays.asList(new Message(new byte[0], new byte[0], "topic", 0, 100),
                new Message(new byte[0], new byte[0], "topic", 0, 100));

        assertEquals(jdbcSink.pushMessage(messages).size(), 0);
        verify(instrumentation, times(1)).captureSuccessExecutionTelemetry("db", messages.size());
    }

    @Test
    public void shouldReturnFailedMessagesWhenExecuteThrowsException() throws SQLException, IOException, DeserializerException {
        SQLException sqlException = new SQLException();
        when(connection.createStatement()).thenThrow(sqlException);
        List<Message> messages = Arrays.asList(new Message(new byte[0], new byte[0], "topic", 0, 100),
                new Message(new byte[0], new byte[0], "topic", 0, 100));

        assertEquals(jdbcSink.pushMessage(messages).size(), 2);
        verify(instrumentation, times(1)).captureFailedExecutionTelemetry(sqlException, messages.size());
    }

    @Test
    public void shouldPrepareBatchForQueries() throws SQLException {
        List<String> queries = Arrays.asList("select * from table", "select count(*) from table");
        List<Message> messages = Arrays.asList(new Message(new byte[0], new byte[0], "topic", 0, 100),
                new Message(new byte[0], new byte[0], "topic", 0, 100));

        JdbcSinkStub dbSinkStub = new JdbcSinkStub(instrumentation, "db", jdbcConnectionPool, queryTemplate, stencilClient, queries);

        dbSinkStub.prepare(messages);
        verify(statement, times(queries.size())).addBatch(anyString());
    }

    @Test
    public void shouldReleaseConnectionAfterSuccessfulQuery() throws Exception {
        when(statement.executeBatch()).thenReturn(new int[]{});
        String sql = "select * from table";
        List<Message> messages = Arrays.asList(new Message(new byte[0], new byte[0], "topic", 0, 100),
                new Message(new byte[0], new byte[0], "topic", 0, 100));
        JdbcSinkStub dbSinkStub = new JdbcSinkStub(instrumentation, "db", jdbcConnectionPool, queryTemplate, stencilClient, Arrays.asList(sql));

        dbSinkStub.pushMessage(messages);

        verify(jdbcConnectionPool).release(connection);
    }

    @Test
    public void shouldReleaseConnectionOnException() throws Exception {
        List<Message> messages = Arrays.asList(new Message(new byte[0], new byte[0], "topic", 0, 100),
                new Message(new byte[0], new byte[0], "topic", 0, 100));

        jdbcSink.pushMessage(messages);

        verify(jdbcConnectionPool).release(connection);
    }

    @Test
    public void shouldNotReleaseConnectionWhenNull() throws Exception {
        String sql = "select * from table";
        JdbcSink sink = new JdbcSink(instrumentation, "db", jdbcConnectionPool, queryTemplate, stencilClient, statement, null);

        sink.execute();

        verify(jdbcConnectionPool, never()).release(connection);
    }


    @Test
    public void shouldCloseConnectionPool() throws IOException, InterruptedException {
        String sql = "select * from table";
        JdbcSinkStub dbSinkStub = new JdbcSinkStub(instrumentation, "db", jdbcConnectionPool, queryTemplate, stencilClient, Arrays.asList(sql));
        dbSinkStub.close();

        verify(jdbcConnectionPool, times(1)).shutdown();
    }

    @Test
    public void shouldCloseStencilClient() throws IOException {
        String sql = "select * from table";
        JdbcSinkStub dbSinkStub = new JdbcSinkStub(instrumentation, "db", jdbcConnectionPool, queryTemplate, stencilClient, Arrays.asList(sql));
        dbSinkStub.close();

        verify(stencilClient, times(1)).close();
    }

    @Test
    public void shouldLogWhenClosingConnection() throws IOException {
        String sql = "select * from table";
        JdbcSinkStub dbSinkStub = new JdbcSinkStub(instrumentation, "db", jdbcConnectionPool, queryTemplate, stencilClient, Arrays.asList(sql));
        dbSinkStub.close();

        verify(instrumentation, times(1)).logInfo("Database connection closing");
    }

    @Test(expected = IOException.class)
    public void shouldThrowIOExceptionWhenFailToClose() throws InterruptedException, IOException {
        doThrow(InterruptedException.class).when(jdbcConnectionPool).shutdown();

        List<String> queriesList = Arrays.asList("select * from table", "select * from table2");
        JdbcSinkStub dbSinkStub = new JdbcSinkStub(instrumentation, "db", jdbcConnectionPool, queryTemplate, stencilClient, queriesList);

        dbSinkStub.close();
    }

    @Test
    public void shouldLogQueryString() {
        Message message = new Message("key".getBytes(), "msg".getBytes(), "topic1", 0, 100);
        jdbcSink.createQueries(Arrays.asList(message));

        verify(instrumentation, times(1)).logDebug(queryTemplate.toQueryString(message));
    }

    @Test
    public void shouldLogDbResponse() throws Exception {
        int[] updateCounts = new int[]{};
        List<String> queries = Arrays.asList("select * from table");
        Message message = new Message(new byte[0], new byte[0], "topic", 0, 100);
        List<Message> messages = Arrays.asList(message);

        when(statement.executeBatch()).thenReturn(updateCounts);
        JdbcSinkStub dbSinkStub = new JdbcSinkStub(instrumentation, "db", jdbcConnectionPool, queryTemplate, stencilClient, queries);

        dbSinkStub.pushMessage(messages);

        verify(statement, times(1)).addBatch("select * from table");
        verify(instrumentation, times(1)).logDebug("Preparing {} messages", 1);
        verify(instrumentation, times(1)).logDebug("DB response: {}", Arrays.toString(updateCounts));
    }

    public class JdbcSinkStub extends JdbcSink {
        private List<String> queries;


        public JdbcSinkStub(Instrumentation instrumentation, String sinkType, JdbcConnectionPool pool, QueryTemplate queryTemplate, StencilClient stencilClient, List<String> queries) {
            super(instrumentation, sinkType, pool, queryTemplate, stencilClient);
            this.queries = queries;
        }

        protected List<String> createQueries(List<Message> messages) {
            return queries;
        }
    }
}
