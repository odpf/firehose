package com.gotocompany.firehose.sink;

import com.gotocompany.firehose.config.KafkaConsumerConfig;
import com.gotocompany.firehose.config.enums.SinkType;
import com.gotocompany.firehose.consumer.kafka.OffsetManager;
import com.gotocompany.firehose.exception.ConfigurationException;
import com.gotocompany.firehose.metrics.FirehoseInstrumentation;
import com.gotocompany.firehose.sink.bigquery.BigquerySinkUtils;
import com.gotocompany.firehose.sink.blob.BlobSinkFactory;
import com.gotocompany.firehose.sink.elasticsearch.EsSinkFactory;
import com.gotocompany.firehose.sink.grpc.GrpcSinkFactory;
import com.gotocompany.firehose.sink.http.HttpSinkFactory;
import com.gotocompany.firehose.sink.influxdb.InfluxSinkFactory;
import com.gotocompany.firehose.sink.jdbc.JdbcSinkFactory;
import com.gotocompany.firehose.sink.mongodb.MongoSinkFactory;
import com.gotocompany.firehose.sink.prometheus.PromSinkFactory;
import com.gotocompany.depot.bigquery.BigQuerySink;
import com.gotocompany.depot.bigquery.BigQuerySinkFactory;
import com.gotocompany.depot.config.BigQuerySinkConfig;
import com.gotocompany.depot.config.RedisSinkConfig;
import com.gotocompany.depot.bigtable.BigTableSinkFactory;
import com.gotocompany.depot.bigtable.BigTableSink;
import com.gotocompany.depot.config.BigTableSinkConfig;
import com.gotocompany.depot.log.LogSink;
import com.gotocompany.depot.log.LogSinkFactory;
import com.gotocompany.depot.metrics.StatsDReporter;
import com.gotocompany.depot.redis.RedisSink;
import com.gotocompany.depot.redis.RedisSinkFactory;
import com.gotocompany.stencil.client.StencilClient;
import org.aeonbits.owner.ConfigFactory;

import java.util.Map;

public class SinkFactory {
    private final KafkaConsumerConfig kafkaConsumerConfig;
    private final StatsDReporter statsDReporter;
    private final FirehoseInstrumentation firehoseInstrumentation;
    private final StencilClient stencilClient;
    private final OffsetManager offsetManager;
    private final Map<String, String> config;
    private BigQuerySinkFactory bigQuerySinkFactory;
    private BigTableSinkFactory bigTableSinkFactory;
    private LogSinkFactory logSinkFactory;
    private RedisSinkFactory redisSinkFactory;

    public SinkFactory(KafkaConsumerConfig kafkaConsumerConfig,
                       StatsDReporter statsDReporter,
                       StencilClient stencilClient,
                       OffsetManager offsetManager) {
        firehoseInstrumentation = new FirehoseInstrumentation(statsDReporter, SinkFactory.class);
        this.kafkaConsumerConfig = kafkaConsumerConfig;
        this.statsDReporter = statsDReporter;
        this.stencilClient = stencilClient;
        this.offsetManager = offsetManager;
        this.config = SinkFactoryUtils.addAdditionalConfigsForSinkConnectors(System.getenv());
    }

    /**
     * Initialization method for all the sinks.
     */
    public void init() {
        switch (this.kafkaConsumerConfig.getSinkType()) {
            case JDBC:
            case HTTP:
            case INFLUXDB:
            case ELASTICSEARCH:
            case GRPC:
            case PROMETHEUS:
            case BLOB:
            case MONGODB:
                return;
            case LOG:
                logSinkFactory = new LogSinkFactory(config, statsDReporter);
                logSinkFactory.init();
                return;
            case REDIS:
                redisSinkFactory = new RedisSinkFactory(
                        ConfigFactory.create(RedisSinkConfig.class, config),
                        statsDReporter);
                redisSinkFactory.init();
                return;
            case BIGQUERY:
                BigquerySinkUtils.addMetadataColumns(config);
                bigQuerySinkFactory = new BigQuerySinkFactory(
                        ConfigFactory.create(BigQuerySinkConfig.class, config),
                        statsDReporter,
                        BigquerySinkUtils.getRowIDCreator());
                bigQuerySinkFactory.init();
                return;
            case BIGTABLE:
                bigTableSinkFactory = new BigTableSinkFactory(
                        ConfigFactory.create(BigTableSinkConfig.class, config),
                        statsDReporter);
                bigTableSinkFactory.init();
                return;
            default:
                throw new ConfigurationException("Invalid Firehose SINK_TYPE");
        }
    }

    public Sink getSink() {
        SinkType sinkType = kafkaConsumerConfig.getSinkType();
        firehoseInstrumentation.logInfo("Sink Type: {}", sinkType);
        switch (sinkType) {
            case JDBC:
                return JdbcSinkFactory.create(config, statsDReporter, stencilClient);
            case HTTP:
                return HttpSinkFactory.create(config, statsDReporter, stencilClient);
            case INFLUXDB:
                return InfluxSinkFactory.create(config, statsDReporter, stencilClient);
            case LOG:
                return new GenericSink(new FirehoseInstrumentation(statsDReporter, LogSink.class), sinkType.name(), logSinkFactory.create());
            case ELASTICSEARCH:
                return EsSinkFactory.create(config, statsDReporter, stencilClient);
            case REDIS:
                return new GenericSink(new FirehoseInstrumentation(statsDReporter, RedisSink.class), sinkType.name(), redisSinkFactory.create());
            case GRPC:
                return GrpcSinkFactory.create(config, statsDReporter, stencilClient);
            case PROMETHEUS:
                return PromSinkFactory.create(config, statsDReporter, stencilClient);
            case BLOB:
                return BlobSinkFactory.create(config, offsetManager, statsDReporter, stencilClient);
            case BIGQUERY:
                return new GenericSink(new FirehoseInstrumentation(statsDReporter, BigQuerySink.class), sinkType.name(), bigQuerySinkFactory.create());
            case BIGTABLE:
                return new GenericSink(new FirehoseInstrumentation(statsDReporter, BigTableSink.class), sinkType.name(), bigTableSinkFactory.create());
            case MONGODB:
                return MongoSinkFactory.create(config, statsDReporter, stencilClient);
            default:
                throw new ConfigurationException("Invalid Firehose SINK_TYPE");
        }
    }
}
