package io.odpf.firehose.sink.log;



import io.odpf.firehose.config.AppConfig;
import io.odpf.firehose.metrics.Instrumentation;
import io.odpf.firehose.sink.SinkFactory;
import io.odpf.firehose.metrics.StatsDReporter;
import io.odpf.firehose.sink.Sink;
import io.odpf.stencil.client.StencilClient;
import io.odpf.stencil.parser.ProtoParser;
import org.aeonbits.owner.ConfigFactory;

import java.util.Map;

/**
 * Factory class to create the LogSink.
 * <p>
 * The consumer framework would reflectively instantiate this factory
 * using the configurations supplied and invoke {@see #create(Map<String, String> configuration, StatsDClient client)}
 * to obtain the LogSink sink implementation.
 */
public class LogSinkFactory implements SinkFactory {

    /**
     * Creates the LogSink.
     *
     * @param configuration  the configuration
     * @param statsDReporter the stats d reporter
     * @param stencilClient  the stencil client
     * @return the sink
     */
    public Sink create(Map<String, String> configuration, StatsDReporter statsDReporter, StencilClient stencilClient) {
        AppConfig appConfig = ConfigFactory.create(AppConfig.class, configuration);
        KeyOrMessageParser parser = new KeyOrMessageParser(new ProtoParser(stencilClient, appConfig.getInputSchemaProtoClass()), appConfig);
        return new LogSink(parser, new Instrumentation(statsDReporter, LogSink.class));
    }
}
