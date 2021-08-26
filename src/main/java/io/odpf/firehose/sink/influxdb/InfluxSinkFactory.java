package io.odpf.firehose.sink.influxdb;



import io.odpf.firehose.config.InfluxSinkConfig;
import io.odpf.firehose.sink.AbstractSink;
import io.odpf.firehose.sink.SinkFactory;
import io.odpf.firehose.metrics.Instrumentation;
import io.odpf.firehose.metrics.StatsDReporter;
import io.odpf.stencil.client.StencilClient;
import io.odpf.stencil.parser.ProtoParser;
import org.aeonbits.owner.ConfigFactory;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;

import java.util.Map;

/**
 * Influx sink factory create influx sink.
 */
public class InfluxSinkFactory implements SinkFactory {
    /**
     * Create Influx sink.
     *
     * @param configProperties the config properties
     * @param statsDReporter   the statsd reporter
     * @param stencilClient    the stencil client
     * @return Influx sink
     */
    @Override
    public AbstractSink create(Map<String, String> configProperties, StatsDReporter statsDReporter, StencilClient stencilClient) {
        InfluxSinkConfig config = ConfigFactory.create(InfluxSinkConfig.class, configProperties);

        Instrumentation instrumentation = new Instrumentation(statsDReporter, InfluxSinkFactory.class);
        instrumentation.logDebug("\nInflux Url: {}\nInflux Username: {}", config.getSinkInfluxUrl(), config.getSinkInfluxUsername());

        InfluxDB client = InfluxDBFactory.connect(config.getSinkInfluxUrl(), config.getSinkInfluxUsername(), config.getSinkInfluxPassword());
        instrumentation.logInfo("InfluxDB connection established");

        return new InfluxSink(new Instrumentation(statsDReporter, InfluxSink.class), "influx.db", config, new ProtoParser(stencilClient, config.getInputSchemaProtoClass()), client, stencilClient);
    }
}
