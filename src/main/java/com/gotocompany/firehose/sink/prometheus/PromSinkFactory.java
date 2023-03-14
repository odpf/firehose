package com.gotocompany.firehose.sink.prometheus;

import com.gotocompany.firehose.config.PromSinkConfig;
import com.gotocompany.firehose.metrics.FirehoseInstrumentation;
import com.gotocompany.firehose.sink.prometheus.request.PromRequest;
import com.gotocompany.firehose.sink.prometheus.request.PromRequestCreator;
import com.gotocompany.depot.metrics.StatsDReporter;
import com.gotocompany.firehose.sink.AbstractSink;
import com.gotocompany.stencil.client.StencilClient;
import com.gotocompany.stencil.Parser;
import org.aeonbits.owner.ConfigFactory;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;

import java.util.Map;

/**
 * Factory class to create the Prometheus Sink.
 * The consumer framework would reflectively instantiate this factory
 * using the configurations supplied and invoke
 * {@see #create(Map < String, String > configuration, StatsDReporter statsDReporter, StencilClient stencilClient)}
 * to obtain the Prometheus sink implementation.
 */

public class PromSinkFactory {

    /**
     * Create Prometheus sink.
     *
     * @param configuration  the configuration
     * @param statsDReporter the statsd reporter
     * @param stencilClient  the stencil client
     * @return PromSink
     */
    public static AbstractSink create(Map<String, String> configuration, StatsDReporter statsDReporter, StencilClient stencilClient) {
        PromSinkConfig promSinkConfig = ConfigFactory.create(PromSinkConfig.class, configuration);
        String promSchemaProtoClass = promSinkConfig.getInputSchemaProtoClass();

        FirehoseInstrumentation firehoseInstrumentation = new FirehoseInstrumentation(statsDReporter, PromSinkFactory.class);

        CloseableHttpClient closeableHttpClient = newHttpClient(promSinkConfig);
        firehoseInstrumentation.logInfo("HTTP connection established");

        Parser protoParser = stencilClient.getParser(promSchemaProtoClass);

        PromRequest request = new PromRequestCreator(statsDReporter, promSinkConfig, protoParser).createRequest();

        return new PromSink(new FirehoseInstrumentation(statsDReporter, PromSink.class),
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
     * @param promSinkConfig the prometheus sink configuration
     * @return CloseableHttpClient
     */
    private static CloseableHttpClient newHttpClient(PromSinkConfig promSinkConfig) {
        RequestConfig requestConfig = RequestConfig.custom().setSocketTimeout(promSinkConfig.getSinkPromRequestTimeoutMs())
                .setConnectionRequestTimeout(promSinkConfig.getSinkPromRequestTimeoutMs())
                .setConnectTimeout(promSinkConfig.getSinkPromRequestTimeoutMs()).build();
        PoolingHttpClientConnectionManager connectionManager = new PoolingHttpClientConnectionManager();
        if (promSinkConfig.getSinkPromMaxConnections() != null && promSinkConfig.getSinkPromMaxConnections() > 0) {
            connectionManager.setMaxTotal(promSinkConfig.getSinkPromMaxConnections());
            connectionManager.setDefaultMaxPerRoute(promSinkConfig.getSinkPromMaxConnections());
        }

        HttpClientBuilder builder = HttpClients.custom().setConnectionManager(connectionManager).setDefaultRequestConfig(requestConfig);

        return builder.build();
    }
}
