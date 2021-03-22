package io.odpf.firehose.sink.http.request.types;

import io.odpf.firehose.config.HttpSinkConfig;
import io.odpf.firehose.config.enums.HttpSinkRequestMethodType;
import io.odpf.firehose.config.enums.HttpSinkParameterSourceType;
import io.odpf.firehose.consumer.Message;
import io.odpf.firehose.exception.DeserializerException;
import io.odpf.firehose.metrics.Instrumentation;
import io.odpf.firehose.metrics.StatsDReporter;
import io.odpf.firehose.sink.http.request.body.JsonBody;
import io.odpf.firehose.sink.http.request.create.BatchRequestCreator;
import io.odpf.firehose.sink.http.request.create.IndividualRequestCreator;
import io.odpf.firehose.sink.http.request.create.RequestCreator;
import io.odpf.firehose.sink.http.request.entity.RequestEntityBuilder;
import io.odpf.firehose.sink.http.request.header.HeaderBuilder;
import io.odpf.firehose.sink.http.request.uri.UriBuilder;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;

import java.net.URISyntaxException;
import java.util.List;

public class SimpleRequest implements Request {

    private HttpSinkConfig httpSinkConfig;
    private JsonBody body;
    private HttpSinkRequestMethodType method;
    private RequestEntityBuilder requestEntityBuilder;
    private RequestCreator requestCreator;
    private StatsDReporter statsDReporter;

    public SimpleRequest(StatsDReporter statsDReporter, HttpSinkConfig config, JsonBody body, HttpSinkRequestMethodType method) {
        this.httpSinkConfig = config;
        this.body = body;
        this.method = method;
        this.statsDReporter = statsDReporter;
    }

    public List<HttpEntityEnclosingRequestBase> build(List<Message> messages) throws DeserializerException, URISyntaxException {
        return requestCreator.create(messages, requestEntityBuilder);
    }

    @Override
    public Request setRequestStrategy(HeaderBuilder headerBuilder, UriBuilder uriBuilder, RequestEntityBuilder requestEntitybuilder) {
        if (isTemplateBody(httpSinkConfig)) {
            this.requestCreator = new IndividualRequestCreator(new Instrumentation(
                    statsDReporter, IndividualRequestCreator.class), uriBuilder, headerBuilder, method, body);
        } else {
            this.requestCreator = new BatchRequestCreator(new Instrumentation(
                    statsDReporter, BatchRequestCreator.class), uriBuilder, headerBuilder, method, body);
        }
        this.requestEntityBuilder = requestEntitybuilder;
        return this;
    }

    @Override
    public boolean canProcess() {
        boolean isDynamicUrl = httpSinkConfig.getSinkHttpServiceUrl().contains(",");
        return httpSinkConfig.getSinkHttpParameterSource() == HttpSinkParameterSourceType.DISABLED && !isDynamicUrl;
    }
}
