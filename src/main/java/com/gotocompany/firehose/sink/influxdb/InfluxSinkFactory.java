package com.gotocompany.firehose.sink.influxdb;


import com.gotocompany.firehose.config.InfluxSinkConfig;
import com.gotocompany.firehose.metrics.FirehoseInstrumentation;
import com.gotocompany.depot.metrics.StatsDReporter;
import com.gotocompany.firehose.sink.AbstractSink;
import com.gotocompany.stencil.client.StencilClient;
import org.aeonbits.owner.ConfigFactory;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;

import java.util.Map;

/**
 * Influx sink factory create influx sink.
 */
public class InfluxSinkFactory {
    /**
     * Create Influx sink.
     *
     * @param configProperties the config properties
     * @param statsDReporter   the statsd reporter
     * @param stencilClient    the stencil client
     * @return Influx sink
     */
    public static AbstractSink create(Map<String, String> configProperties, StatsDReporter statsDReporter, StencilClient stencilClient) {
        InfluxSinkConfig config = ConfigFactory.create(InfluxSinkConfig.class, configProperties);

        FirehoseInstrumentation firehoseInstrumentation = new FirehoseInstrumentation(statsDReporter, InfluxSinkFactory.class);
        firehoseInstrumentation.logDebug("\nInflux Url: {}\nInflux Username: {}", config.getSinkInfluxUrl(), config.getSinkInfluxUsername());

        InfluxDB client = InfluxDBFactory.connect(config.getSinkInfluxUrl(), config.getSinkInfluxUsername(), config.getSinkInfluxPassword());
        firehoseInstrumentation.logInfo("InfluxDB connection established");

        return new InfluxSink(new FirehoseInstrumentation(statsDReporter, InfluxSink.class), "influx.db", config, stencilClient.getParser(config.getInputSchemaProtoClass()), client, stencilClient);
    }
}
