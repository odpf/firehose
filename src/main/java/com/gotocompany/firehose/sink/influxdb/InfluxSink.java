package com.gotocompany.firehose.sink.influxdb;



import com.gotocompany.firehose.config.InfluxSinkConfig;
import com.gotocompany.firehose.message.Message;
import com.gotocompany.firehose.metrics.FirehoseInstrumentation;
import com.gotocompany.firehose.sink.influxdb.builder.PointBuilder;
import com.gotocompany.firehose.sink.AbstractSink;
import com.google.protobuf.DynamicMessage;
import com.gotocompany.stencil.client.StencilClient;
import com.gotocompany.stencil.Parser;
import org.influxdb.InfluxDB;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Influx sink for firehose.
 */
public class InfluxSink extends AbstractSink {
    public static final String FIELD_NAME_MAPPING_ERROR_MESSAGE = "field index mapping cannot be empty; at least one field value is required";

    private InfluxSinkConfig config;
    private Parser protoParser;
    private PointBuilder pointBuilder;
    private InfluxDB client;
    private BatchPoints batchPoints;
    private StencilClient stencilClient;

    /**
     * Instantiates a new Influx sink.
     *
     * @param firehoseInstrumentation the instrumentation
     * @param sinkType        the sink type
     * @param config          the config
     * @param protoParser     the proto parser
     * @param client          the client
     * @param stencilClient   the stencil client
     */
    public InfluxSink(FirehoseInstrumentation firehoseInstrumentation, String sinkType, InfluxSinkConfig config, Parser protoParser, InfluxDB client, StencilClient stencilClient) {
        super(firehoseInstrumentation, sinkType);
        this.config = config;
        this.protoParser = protoParser;
        this.pointBuilder = new PointBuilder(config);
        this.client = client;
        this.stencilClient = stencilClient;
    }

    @Override
    protected void prepare(List<Message> messages) throws IOException {
        batchPoints = BatchPoints.database(config.getSinkInfluxDbName()).retentionPolicy(config.getSinkInfluxRetentionPolicy()).build();
        for (Message message : messages) {
            DynamicMessage dynamicMessage = protoParser.parse(message.getLogMessage());
            Point point = pointBuilder.buildPoint(dynamicMessage);
            getFirehoseInstrumentation().logDebug("Data point: {}", point.toString());
            batchPoints.point(point);
        }
    }

    @Override
    protected List<Message> execute() {
        getFirehoseInstrumentation().logDebug("Batch points: {}", batchPoints.toString());
        client.write(batchPoints);
        return new ArrayList<>();
    }

    @Override
    public void close() throws IOException {
        getFirehoseInstrumentation().logInfo("InfluxDB connection closing");
        stencilClient.close();
    }
}
