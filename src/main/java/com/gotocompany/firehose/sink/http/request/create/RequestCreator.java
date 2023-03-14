package com.gotocompany.firehose.sink.http.request.create;

import com.gotocompany.firehose.message.Message;
import com.gotocompany.firehose.sink.http.request.entity.RequestEntityBuilder;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;

import java.net.URISyntaxException;
import java.util.List;

/**
 * Creates http requests.
 */
public interface RequestCreator {

    List<HttpEntityEnclosingRequestBase> create(List<Message> bodyContents, RequestEntityBuilder entity) throws URISyntaxException;
}
