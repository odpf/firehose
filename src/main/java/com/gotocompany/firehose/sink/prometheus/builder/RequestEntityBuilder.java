package com.gotocompany.firehose.sink.prometheus.builder;

import cortexpb.Cortex;
import org.apache.http.entity.ByteArrayEntity;
import org.xerial.snappy.Snappy;

import java.io.IOException;

/**
 * Builder for prometheus request entity.
 */
public class RequestEntityBuilder {

    /**
     * Build prometheus request entity.
     * the cortex write request object will be compressed using snappy
     *
     * @param writeRequest  the cortex write request object
     * @return              ByteArrayEntity
     * @throws IOException  the io exception
     */
    public ByteArrayEntity buildHttpEntity(Cortex.WriteRequest writeRequest) throws IOException {
        byte[] compressed = Snappy.compress(writeRequest.toByteArray());
        return new ByteArrayEntity(compressed);
    }
}
