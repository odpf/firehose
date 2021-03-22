package io.odpf.firehose.sink.jdbc;


import com.gojek.de.stencil.client.StencilClient;
import com.gojek.de.stencil.parser.ProtoParser;
import io.odpf.firehose.config.JdbcSinkConfig;
import io.odpf.firehose.sink.AbstractSink;
import io.odpf.firehose.sink.SinkFactory;
import io.odpf.firehose.metrics.Instrumentation;
import io.odpf.firehose.metrics.StatsDReporter;
import io.odpf.firehose.proto.ProtoToFieldMapper;
import org.aeonbits.owner.ConfigFactory;

import java.util.Map;

/**
 * Factory class to create the JDBCSink.
 * <p>
 * The consumer framework would reflectively instantiate this factory
 * using the configurations supplied and invoke {@see #create(Map < String, String > configuration, StatsDClient client)}
 * to obtain the JDBCSink sink implementation.
 */
public class JdbcSinkFactory implements SinkFactory {

    /**
     * Create JDBC sink.
     *
     * @param configuration  the configuration
     * @param statsDReporter the stats d reporter
     * @param client         the client
     * @return the abstract sink
     */
    public AbstractSink create(Map<String, String> configuration, StatsDReporter statsDReporter, StencilClient client) {
        JdbcSinkConfig jdbcSinkConfig = ConfigFactory.create(JdbcSinkConfig.class, configuration);

        Instrumentation instrumentation = new Instrumentation(statsDReporter, JdbcSinkFactory.class);
        String dbConfig = String.format(""
                        + "\n\tJDBC URL: %s\n\tJDBC Username: %s\n\tJDBC Tablename: %s\n\tMax connection pool size: %s\n\tConnection timeout: %s"
                        + "\n\tIdle timeout: %s\n\tMinimum idle: %s\n\tUnique keys: %s",
                jdbcSinkConfig.getSinkJdbcUrl(), jdbcSinkConfig.getSinkJdbcUsername(), jdbcSinkConfig.getSinkJdbcTableName(), jdbcSinkConfig.getSinkJdbcConnectionPoolMaxSize(),
                jdbcSinkConfig.getSinkJdbcConnectionPoolTimeoutMs(), jdbcSinkConfig.getSinkJdbcConnectionPoolIdleTimeoutMs(), jdbcSinkConfig.getSinkJdbcConnectionPoolMinIdle(), jdbcSinkConfig.getSinkJdbcUniqueKeys());
        instrumentation.logDebug(dbConfig);
        JdbcConnectionPool connectionPool = new HikariJdbcConnectionPool(jdbcSinkConfig.getSinkJdbcUrl(), jdbcSinkConfig.getSinkJdbcUsername(),
                jdbcSinkConfig.getSinkJdbcPassword(), jdbcSinkConfig.getSinkJdbcConnectionPoolMaxSize(),
                jdbcSinkConfig.getSinkJdbcConnectionPoolTimeoutMs(), jdbcSinkConfig.getSinkJdbcConnectionPoolIdleTimeoutMs(), jdbcSinkConfig.getSinkJdbcConnectionPoolMinIdle());
        instrumentation.logInfo("JDBC Connection established");
        QueryTemplate queryTemplate = createQueryTemplate(jdbcSinkConfig, client);

        return new JdbcSink(new Instrumentation(statsDReporter, JdbcSink.class), "db", connectionPool, queryTemplate, client);
    }


    private QueryTemplate createQueryTemplate(JdbcSinkConfig jdbcSinkConfig, StencilClient stencilClient) {
        ProtoParser protoParser = new ProtoParser(stencilClient, jdbcSinkConfig.getInputSchemaProtoClass());
        ProtoToFieldMapper protoToFieldMapper = new ProtoToFieldMapper(protoParser, jdbcSinkConfig.getInputSchemaProtoToColumnMapping());
        return new QueryTemplate(jdbcSinkConfig, protoToFieldMapper);
    }
}
