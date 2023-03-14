package com.gotocompany.firehose.sink.jdbc;


import com.gotocompany.firehose.config.JdbcSinkConfig;
import com.gotocompany.firehose.metrics.FirehoseInstrumentation;
import com.gotocompany.firehose.proto.ProtoToFieldMapper;
import com.gotocompany.depot.metrics.StatsDReporter;
import com.gotocompany.firehose.sink.AbstractSink;
import com.gotocompany.stencil.client.StencilClient;
import com.gotocompany.stencil.Parser;
import org.aeonbits.owner.ConfigFactory;

import java.util.Map;

/**
 * Factory class to create the JDBC Sink.
 * <p>
 * The consumer framework would reflectively instantiate this factory
 * using the configurations supplied and invoke {@see #create(Map < String, String > configuration, StatsDReporter statsDReporter, StencilClient client)}
 * to obtain the JDBCSink sink implementation.
 */
public class JdbcSinkFactory {

    /**
     * Create JDBC sink.
     *
     * @param configuration  the configuration
     * @param statsDReporter the stats d reporter
     * @param client         the client
     * @return the abstract sink
     */
    public static AbstractSink create(Map<String, String> configuration, StatsDReporter statsDReporter, StencilClient client) {
        JdbcSinkConfig jdbcSinkConfig = ConfigFactory.create(JdbcSinkConfig.class, configuration);

        FirehoseInstrumentation firehoseInstrumentation = new FirehoseInstrumentation(statsDReporter, JdbcSinkFactory.class);
        String dbConfig = String.format(""
                        + "\n\tJDBC URL: %s\n\tJDBC Username: %s\n\tJDBC Tablename: %s\n\tUnique keys: %s",
                jdbcSinkConfig.getSinkJdbcUrl(), jdbcSinkConfig.getSinkJdbcUsername(), jdbcSinkConfig.getSinkJdbcTableName(), jdbcSinkConfig.getSinkJdbcUniqueKeys());
        firehoseInstrumentation.logDebug(dbConfig);
        JdbcConnectionPool connectionPool = new HikariJdbcConnectionPool(jdbcSinkConfig.getSinkJdbcUrl(), jdbcSinkConfig.getSinkJdbcUsername(),
                jdbcSinkConfig.getSinkJdbcPassword(), jdbcSinkConfig.getSinkJdbcConnectionPoolMaxSize(),
                jdbcSinkConfig.getSinkJdbcConnectionPoolTimeoutMs(), jdbcSinkConfig.getSinkJdbcConnectionPoolIdleTimeoutMs(), jdbcSinkConfig.getSinkJdbcConnectionPoolMinIdle());
        firehoseInstrumentation.logInfo("JDBC Connection established");
        QueryTemplate queryTemplate = createQueryTemplate(jdbcSinkConfig, client);

        return new JdbcSink(new FirehoseInstrumentation(statsDReporter, JdbcSink.class), "db", connectionPool, queryTemplate, client);
    }

    private static QueryTemplate createQueryTemplate(JdbcSinkConfig jdbcSinkConfig, StencilClient stencilClient) {
        Parser protoParser = stencilClient.getParser(jdbcSinkConfig.getInputSchemaProtoClass());
        ProtoToFieldMapper protoToFieldMapper = new ProtoToFieldMapper(protoParser, jdbcSinkConfig.getInputSchemaProtoToColumnMapping());
        return new QueryTemplate(jdbcSinkConfig, protoToFieldMapper);
    }
}
