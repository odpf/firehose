package com.gotocompany.firehose.sink.http.request.create;

import com.gotocompany.firehose.config.HttpSinkConfig;
import com.gotocompany.firehose.config.enums.HttpSinkRequestMethodType;
import com.gotocompany.firehose.message.Message;
import com.gotocompany.firehose.metrics.FirehoseInstrumentation;
import com.gotocompany.firehose.sink.http.request.entity.RequestEntityBuilder;
import com.gotocompany.firehose.sink.http.request.header.HeaderBuilder;
import com.gotocompany.firehose.sink.http.request.HttpRequestMethodFactory;
import com.gotocompany.firehose.sink.http.request.body.JsonBody;
import com.gotocompany.firehose.sink.http.request.uri.UriBuilder;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class BatchRequestCreator implements RequestCreator {

    private UriBuilder uriBuilder;
    private HeaderBuilder headerBuilder;
    private HttpSinkRequestMethodType method;
    private JsonBody jsonBody;
    private FirehoseInstrumentation firehoseInstrumentation;
    private HttpSinkConfig httpSinkConfig;

    public BatchRequestCreator(FirehoseInstrumentation firehoseInstrumentation, UriBuilder uriBuilder, HeaderBuilder headerBuilder, HttpSinkRequestMethodType method, JsonBody jsonBody, HttpSinkConfig httpSinkConfig) {
        this.uriBuilder = uriBuilder;
        this.headerBuilder = headerBuilder;
        this.method = method;
        this.jsonBody = jsonBody;
        this.httpSinkConfig = httpSinkConfig;
        this.firehoseInstrumentation = firehoseInstrumentation;
    }

    @Override
    public List<HttpEntityEnclosingRequestBase> create(List<Message> messages, RequestEntityBuilder requestEntityBuilder) throws URISyntaxException {
        URI uri = uriBuilder.build();
        HttpEntityEnclosingRequestBase request = HttpRequestMethodFactory
                .create(uri, method);

        Map<String, String> headerMap = headerBuilder.build();
        headerMap.forEach(request::addHeader);
        String messagesString = jsonBody.serialize(messages).toString();

        if (!(method == HttpSinkRequestMethodType.DELETE && !httpSinkConfig.getSinkHttpDeleteBodyEnable())) {
            request.setEntity(requestEntityBuilder.buildHttpEntity(messagesString));
            firehoseInstrumentation.logDebug("\nRequest URL: {}\nRequest headers: {}\nRequest content: {}\nRequest method: {}",
                    uri, headerMap, jsonBody.serialize(messages), method);
        } else {
            firehoseInstrumentation.logDebug("\nRequest URL: {}\nRequest headers: {}\nRequest content: no body\nRequest method: {}",
                    uri, headerMap, method);
        }
        return Collections.singletonList(request);
    }
}
