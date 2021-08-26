package io.odpf.firehose.sink.http;



import io.odpf.firehose.config.HttpSinkConfig;
import io.odpf.firehose.metrics.Instrumentation;
import io.odpf.firehose.metrics.StatsDReporter;
import io.odpf.firehose.sink.AbstractSink;
import io.odpf.firehose.sink.SinkFactory;
import io.odpf.firehose.sink.http.auth.OAuth2Credential;
import io.odpf.firehose.sink.http.request.types.Request;
import io.odpf.firehose.sink.http.request.RequestFactory;
import io.odpf.firehose.sink.http.request.uri.UriParser;
import io.odpf.stencil.client.StencilClient;
import io.odpf.stencil.parser.ProtoParser;
import org.aeonbits.owner.ConfigFactory;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;

import java.util.Map;

/**
 * Factory class to create the HTTP Sink.
 * The consumer framework would reflectively instantiate this factory
 * using the configurations supplied and invoke {@see #create(Map < String, String > configuration, StatsDClient client)}
 * to obtain the HTTPSink sink implementation. {@see ParameterizedHTTPSinkConfig}
 */
public class HttpSinkFactory implements SinkFactory {

    /**
     * Create Http sink.
     *
     * @param configuration  the configuration
     * @param statsDReporter the statsd reporter
     * @param stencilClient  the stencil client
     * @return the http sink
     */
    @Override
    public AbstractSink create(Map<String, String> configuration, StatsDReporter statsDReporter, StencilClient stencilClient) {
        HttpSinkConfig httpSinkConfig = ConfigFactory.create(HttpSinkConfig.class, configuration);

        Instrumentation instrumentation = new Instrumentation(statsDReporter, HttpSinkFactory.class);

        CloseableHttpClient closeableHttpClient = newHttpClient(httpSinkConfig, statsDReporter);
        instrumentation.logInfo("HTTP connection established");

        UriParser uriParser = new UriParser(new ProtoParser(stencilClient, httpSinkConfig.getInputSchemaProtoClass()), httpSinkConfig.getKafkaRecordParserMode());

        Request request = new RequestFactory(statsDReporter, httpSinkConfig, stencilClient, uriParser).createRequest();

        return new HttpSink(new Instrumentation(statsDReporter, HttpSink.class), request, closeableHttpClient, stencilClient, httpSinkConfig.getSinkHttpRetryStatusCodeRanges(), httpSinkConfig.getSinkHttpRequestLogStatusCodeRanges());
    }

    private CloseableHttpClient newHttpClient(HttpSinkConfig httpSinkConfig, StatsDReporter statsDReporter) {
        Integer maxHttpConnections = httpSinkConfig.getSinkHttpMaxConnections();
        RequestConfig requestConfig = RequestConfig.custom().setSocketTimeout(httpSinkConfig.getSinkHttpRequestTimeoutMs())
                .setConnectionRequestTimeout(httpSinkConfig.getSinkHttpRequestTimeoutMs())
                .setConnectTimeout(httpSinkConfig.getSinkHttpRequestTimeoutMs()).build();
        PoolingHttpClientConnectionManager connectionManager = new PoolingHttpClientConnectionManager();
        connectionManager.setMaxTotal(maxHttpConnections);
        connectionManager.setDefaultMaxPerRoute(maxHttpConnections);
        HttpClientBuilder builder = HttpClients.custom().setConnectionManager(connectionManager).setDefaultRequestConfig(requestConfig);
        if (httpSinkConfig.isSinkHttpOAuth2Enable()) {
            OAuth2Credential oauth2 = new OAuth2Credential(
                    new Instrumentation(statsDReporter, OAuth2Credential.class),
                    httpSinkConfig.getSinkHttpOAuth2ClientName(),
                    httpSinkConfig.getSinkHttpOAuth2ClientSecret(),
                    httpSinkConfig.getSinkHttpOAuth2Scope(),
                    httpSinkConfig.getSinkHttpOAuth2AccessTokenUrl());
            builder = oauth2.initialize(builder);
        }
        return builder.build();
    }
}
