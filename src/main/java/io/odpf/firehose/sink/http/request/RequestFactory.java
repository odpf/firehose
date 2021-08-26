package io.odpf.firehose.sink.http.request;



import io.odpf.firehose.config.HttpSinkConfig;
import io.odpf.firehose.config.enums.HttpSinkRequestMethodType;
import io.odpf.firehose.metrics.Instrumentation;
import io.odpf.firehose.metrics.StatsDReporter;
import io.odpf.firehose.proto.ProtoToFieldMapper;
import io.odpf.firehose.serializer.MessageSerializer;
import io.odpf.firehose.sink.http.factory.SerializerFactory;
import io.odpf.firehose.sink.http.request.body.JsonBody;
import io.odpf.firehose.sink.http.request.entity.RequestEntityBuilder;
import io.odpf.firehose.sink.http.request.header.HeaderBuilder;
import io.odpf.firehose.sink.http.request.types.DynamicUrlRequest;
import io.odpf.firehose.sink.http.request.types.ParameterizedHeaderRequest;
import io.odpf.firehose.sink.http.request.types.ParameterizedUriRequest;
import io.odpf.firehose.sink.http.request.types.Request;
import io.odpf.firehose.sink.http.request.types.SimpleRequest;
import io.odpf.firehose.sink.http.request.uri.UriBuilder;
import io.odpf.firehose.sink.http.request.uri.UriParser;
import io.odpf.stencil.client.StencilClient;
import io.odpf.stencil.parser.ProtoParser;

import java.util.Arrays;
import java.util.List;

/**
 * Request factory create requests based on configuration.
 */
public class RequestFactory {

    private HttpSinkConfig httpSinkConfig;
    private UriParser uriParser;
    private StencilClient stencilClient;
    private StatsDReporter statsDReporter;
    private Instrumentation instrumentation;

    /**
     * Instantiates a new Request factory.
     *
     * @param statsDReporter the statsd reporter
     * @param httpSinkConfig the http sink config
     * @param stencilClient  the stencil client
     * @param uriParser      the uri parser
     */
    public RequestFactory(StatsDReporter statsDReporter, HttpSinkConfig httpSinkConfig, StencilClient stencilClient, UriParser uriParser) {
        this.statsDReporter = statsDReporter;
        this.stencilClient = stencilClient;
        this.httpSinkConfig = httpSinkConfig;
        this.uriParser = uriParser;
        instrumentation = new Instrumentation(this.statsDReporter, RequestFactory.class);
    }

    public Request createRequest() {
        JsonBody body = createBody();
        HttpSinkRequestMethodType httpSinkRequestMethodType = httpSinkConfig.getSinkHttpRequestMethod();
        HeaderBuilder headerBuilder = new HeaderBuilder(httpSinkConfig.getSinkHttpHeaders());
        UriBuilder uriBuilder = new UriBuilder(httpSinkConfig.getSinkHttpServiceUrl(), uriParser);
        RequestEntityBuilder requestEntityBuilder = new RequestEntityBuilder();

        List<Request> requests = Arrays.asList(
                new SimpleRequest(statsDReporter, httpSinkConfig, body, httpSinkRequestMethodType),
                new DynamicUrlRequest(statsDReporter, httpSinkConfig, body, httpSinkRequestMethodType),
                new ParameterizedHeaderRequest(statsDReporter, httpSinkConfig, body, httpSinkRequestMethodType, getProtoToFieldMapper()),
                new ParameterizedUriRequest(statsDReporter, httpSinkConfig, body, httpSinkRequestMethodType, getProtoToFieldMapper()));

        Request request = requests.stream()
                .filter(Request::canProcess)
                .findFirst()
                .orElse(new SimpleRequest(statsDReporter, httpSinkConfig, body, httpSinkRequestMethodType));
        instrumentation.logInfo("Request type: {}", request.getClass());

        return request.setRequestStrategy(headerBuilder, uriBuilder, requestEntityBuilder);
    }

    private ProtoToFieldMapper getProtoToFieldMapper() {
        ProtoParser protoParser = new ProtoParser(stencilClient, httpSinkConfig.getSinkHttpParameterSchemaProtoClass());
        return new ProtoToFieldMapper(protoParser, httpSinkConfig.getInputSchemaProtoToColumnMapping());
    }

    private JsonBody createBody() {
        MessageSerializer messageSerializer = new SerializerFactory(
                httpSinkConfig,
                stencilClient,
                statsDReporter)
                .build();
        return new JsonBody(messageSerializer);
    }
}
