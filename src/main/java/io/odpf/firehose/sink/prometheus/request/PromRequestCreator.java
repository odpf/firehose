package io.odpf.firehose.sink.prometheus.request;


import io.odpf.firehose.config.PromSinkConfig;
import io.odpf.firehose.metrics.Instrumentation;
import io.odpf.firehose.metrics.StatsDReporter;
import io.odpf.firehose.sink.prometheus.builder.HeaderBuilder;
import io.odpf.firehose.sink.prometheus.builder.RequestEntityBuilder;
import io.odpf.firehose.sink.prometheus.builder.TimeSeriesBuilder;
import io.odpf.firehose.sink.prometheus.builder.WriteRequestBuilder;
import io.odpf.stencil.parser.ProtoParser;

/**
 * Prometheus Request Creator.
 */
public class PromRequestCreator {
    private PromSinkConfig promSinkConfig;
    private StatsDReporter statsDReporter;
    private ProtoParser protoParser;


    /**
     * Instantiates a new Prometheus request creator.
     *
     * @param statsDReporter        the statsd reporter
     * @param promSinkConfig  the configuration for prometheus sink
     * @param protoParser           the proto parser
     */
    public PromRequestCreator(StatsDReporter statsDReporter, PromSinkConfig promSinkConfig, ProtoParser protoParser) {
        this.statsDReporter = statsDReporter;
        this.promSinkConfig = promSinkConfig;
        this.protoParser = protoParser;
    }

    /**
     * Instantiates a new Prometheus request.
     *
     * @return PromRequest
     */
    public PromRequest createRequest() {
        WriteRequestBuilder body = createBody();
        HeaderBuilder headerBuilder = new HeaderBuilder(promSinkConfig.getSinkPromHeaders());
        String baseUrl = promSinkConfig.getSinkPromServiceUrl();
        RequestEntityBuilder requestEntityBuilder = new RequestEntityBuilder();

        return new PromRequest(new Instrumentation(statsDReporter, PromRequest.class),
                headerBuilder, baseUrl, requestEntityBuilder, body);
    }

    /**
     * create prometheus writeRequest builder.
     *
     * @return WriteRequestBuilder
     */
    private WriteRequestBuilder createBody() {
        TimeSeriesBuilder timeSeriesBuilder = new TimeSeriesBuilder(promSinkConfig);
        return new WriteRequestBuilder(timeSeriesBuilder, protoParser);
    }
}
