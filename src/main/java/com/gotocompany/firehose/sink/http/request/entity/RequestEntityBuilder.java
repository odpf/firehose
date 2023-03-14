package com.gotocompany.firehose.sink.http.request.entity;

import com.gotocompany.firehose.exception.DeserializerException;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;

import java.util.Collections;

/**
 * Request entity builder.
 */
public class RequestEntityBuilder {
    private boolean wrapArray;

    /**
     * Instantiates a new Request entity builder.
     */
    public RequestEntityBuilder() {
        this.wrapArray = false;
    }

    public RequestEntityBuilder setWrapping(boolean isArrayWrap) {
        this.wrapArray = isArrayWrap;
        return this;
    }

    /**
     * Build http entity string entity.
     *
     * @param bodyContent the body content
     * @return the string entity
     * @throws DeserializerException the deserializer exception
     */
    public StringEntity buildHttpEntity(String bodyContent) throws DeserializerException {
        if (!wrapArray) {
            return new StringEntity(bodyContent, ContentType.APPLICATION_JSON);
        } else {
            String arrayWrappedBody = Collections.singletonList(bodyContent).toString();
            return new StringEntity(arrayWrappedBody, ContentType.APPLICATION_JSON);
        }
    }
}
