package com.gotocompany.firehose.sink.http.request.types;

import com.gotocompany.firehose.config.HttpSinkConfig;
import com.gotocompany.firehose.config.enums.HttpSinkDataFormatType;
import com.gotocompany.firehose.exception.DeserializerException;
import com.gotocompany.firehose.message.Message;
import com.gotocompany.firehose.sink.http.request.header.HeaderBuilder;
import com.gotocompany.firehose.sink.http.request.entity.RequestEntityBuilder;
import com.gotocompany.firehose.sink.http.request.uri.UriBuilder;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;

import java.net.URISyntaxException;
import java.util.List;

/**
 * Http Request.
 */
public interface Request {
    /**
     * Create list of requests.
     *
     * @param messages the messages
     * @return the list
     * @throws URISyntaxException    the uri syntax exception
     * @throws DeserializerException the deserializer exception
     */
    List<HttpEntityEnclosingRequestBase> build(List<Message> messages) throws URISyntaxException, DeserializerException;

    /**
     * Sets request strategy.
     *
     * @param headerBuilder        the header builder
     * @param uriBuilder           the uri builder
     * @param requestEntityBuilder the request entity builder
     * @return the request strategy
     */
    Request setRequestStrategy(HeaderBuilder headerBuilder, UriBuilder uriBuilder, RequestEntityBuilder requestEntityBuilder);

    /**
     * This method to used to filter if the Request can be processed.
     *
     * @return the boolean
     */
    boolean canProcess();

    /**
     * return true if json template is present for request body.
     *
     * @param httpSinkConfig the http sink config
     * @return the boolean
     */
    default boolean isTemplateBody(HttpSinkConfig httpSinkConfig) {
        return httpSinkConfig.getSinkHttpDataFormat() == HttpSinkDataFormatType.JSON
                && !httpSinkConfig.getSinkHttpJsonBodyTemplate().isEmpty();
    }
}
