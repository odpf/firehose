package io.odpf.firehose.sink.prometheus;



import io.odpf.firehose.config.PromSinkConfig;
import io.odpf.firehose.metrics.Instrumentation;
import io.odpf.firehose.metrics.StatsDReporter;
import io.odpf.firehose.sink.AbstractSink;
import io.odpf.firehose.sink.SinkFactory;
import io.odpf.firehose.sink.prometheus.request.PromRequest;
import io.odpf.firehose.sink.prometheus.request.PromRequestCreator;
import io.odpf.stencil.client.StencilClient;
import io.odpf.stencil.parser.ProtoParser;
import org.aeonbits.owner.ConfigFactory;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.BasicHttpClientConnectionManager;

import java.util.Map;

/**
 * Factory class to create the Prometheus Sink.
 * The consumer framework would reflectively instantiate this factory
 * using the configurations supplied and invoke
 * {@see #create(Map<String, String> configuration, StatsDReporter statsDReporter, StencilClient stencilClient)}
 * to obtain the Prometheus sink implementation.
 */

public class PromSinkFactory implements SinkFactory {

    /**
     * Create Prometheus sink.
     *
     * @param configuration  the configuration
     * @param statsDReporter the statsd reporter
     * @param stencilClient  the stencil client
     * @return PromSink
     */
    @Override
    public AbstractSink create(Map<String, String> configuration, StatsDReporter statsDReporter, StencilClient stencilClient) {
        PromSinkConfig promSinkConfig = ConfigFactory.create(PromSinkConfig.class, configuration);
        String promSchemaProtoClass = promSinkConfig.getInputSchemaProtoClass();

        Instrumentation instrumentation = new Instrumentation(statsDReporter, PromSinkFactory.class);

        CloseableHttpClient closeableHttpClient = newHttpClient(promSinkConfig);
        instrumentation.logInfo("HTTP connection established");

        ProtoParser protoParser = new ProtoParser(stencilClient, promSchemaProtoClass);

        PromRequest request = new PromRequestCreator(statsDReporter, promSinkConfig, protoParser).createRequest();

        return new PromSink(new Instrumentation(statsDReporter, PromSink.class),
                request,
                closeableHttpClient,
                stencilClient,
                promSinkConfig.getSinkPromRetryStatusCodeRanges(),
                promSinkConfig.getSinkPromRequestLogStatusCodeRanges()
        );
    }

    /**
     * create a new http client.
     *
     * @param promSinkConfig      the prometheus sink configuration
     * @return CloseableHttpClient
     */
    private CloseableHttpClient newHttpClient(PromSinkConfig promSinkConfig) {
        RequestConfig requestConfig = RequestConfig.custom().setSocketTimeout(promSinkConfig.getSinkPromRequestTimeoutMs())
                .setConnectionRequestTimeout(promSinkConfig.getSinkPromRequestTimeoutMs())
                .setConnectTimeout(promSinkConfig.getSinkPromRequestTimeoutMs()).build();
        BasicHttpClientConnectionManager connectionManager = new BasicHttpClientConnectionManager();
        HttpClientBuilder builder = HttpClients.custom().setConnectionManager(connectionManager).setDefaultRequestConfig(requestConfig);

        return builder.build();
    }
}
