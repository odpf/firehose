package io.odpf.firehose.sink;


import io.odpf.firehose.metrics.StatsDReporter;
import io.odpf.stencil.client.StencilClient;

import java.util.Map;

/**
 * Interface to create the sink.
 *
 * Any sink {@see Sink} can be created by a class implementing this interface.
 * The consumer framework would reflectively instantiate this factory
 * using the configurations supplied and invoke {@see #create(Map<String, String> configuration, StatsDClient client)}
 * to obtain the sink implementation.
 *
 */
public interface SinkFactory {

    /**
     * method to create the sink from configuration supplied.
     *
     * @param configuration key/value configuration supplied as a map
     * @param client {@see StatsDClient}
     * @param stencilClient {@see StencilClient}
     * @return instance of sink to which messages consumed from kafka can be forwarded to. {@see Sink}
     */
    Sink create(Map<String, String> configuration, StatsDReporter client, StencilClient stencilClient);
}
