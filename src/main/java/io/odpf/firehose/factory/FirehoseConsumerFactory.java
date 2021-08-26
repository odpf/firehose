package io.odpf.firehose.factory;

import io.jaegertracing.Configuration;
import io.odpf.firehose.config.AppConfig;
import io.odpf.firehose.config.DlqConfig;
import io.odpf.firehose.config.KafkaConsumerConfig;
import io.odpf.firehose.consumer.FirehoseConsumer;
import io.odpf.firehose.consumer.GenericConsumer;
import io.odpf.firehose.exception.EglcConfigurationException;
import io.odpf.firehose.filter.Filter;
import io.odpf.firehose.filter.MessageFilter;
import io.odpf.firehose.metrics.Instrumentation;
import io.odpf.firehose.metrics.StatsDReporter;
import io.odpf.firehose.sink.Sink;
import io.odpf.firehose.sink.elasticsearch.EsSinkFactory;
import io.odpf.firehose.sink.grpc.GrpcSinkFactory;
import io.odpf.firehose.sink.http.HttpSinkFactory;
import io.odpf.firehose.sink.influxdb.InfluxSinkFactory;
import io.odpf.firehose.sink.jdbc.JdbcSinkFactory;
import io.odpf.firehose.sink.log.KeyOrMessageParser;
import io.odpf.firehose.sink.log.LogSinkFactory;
import io.odpf.firehose.sink.mongodb.MongoSinkFactory;
import io.odpf.firehose.sink.prometheus.PromSinkFactory;
import io.odpf.firehose.sink.redis.RedisSinkFactory;
import io.odpf.firehose.sinkdecorator.BackOff;
import io.odpf.firehose.sinkdecorator.BackOffProvider;
import io.odpf.firehose.sinkdecorator.ExponentialBackOffProvider;
import io.odpf.firehose.sinkdecorator.SinkWithRetry;
import io.odpf.firehose.sinkdecorator.SinkWithDlq;
import io.odpf.firehose.tracer.SinkTracer;
import io.odpf.firehose.util.Clock;
import io.odpf.stencil.StencilClientFactory;
import io.odpf.stencil.client.StencilClient;
import io.odpf.stencil.parser.ProtoParser;

import io.opentracing.Tracer;
import io.opentracing.contrib.kafka.TracingKafkaProducer;
import io.opentracing.noop.NoopTracerFactory;
import org.aeonbits.owner.ConfigFactory;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.util.Map;

/**
 * Factory for Firehose consumer.
 */
public class FirehoseConsumerFactory {

    private Map<String, String> config = System.getenv();
    private final KafkaConsumerConfig kafkaConsumerConfig;
    private StatsDReporter statsDReporter;
    private final Clock clockInstance;
    private StencilClient stencilClient;
    private Instrumentation instrumentation;
    private KeyOrMessageParser parser;

    /**
     * Instantiates a new Firehose consumer factory.
     *
     * @param kafkaConsumerConfig the kafka consumer config
     * @param statsDReporter      the stats d reporter
     */
    public FirehoseConsumerFactory(KafkaConsumerConfig kafkaConsumerConfig, StatsDReporter statsDReporter) {
        this.kafkaConsumerConfig = kafkaConsumerConfig;
        this.statsDReporter = statsDReporter;
        instrumentation = new Instrumentation(this.statsDReporter, FirehoseConsumerFactory.class);

        String additionalConsumerConfig = String.format(""
                        + "\n\tEnable Async Commit: %s"
                        + "\n\tCommit Only Current Partition: %s",
                this.kafkaConsumerConfig.isSourceKafkaAsyncCommitEnable(),
                this.kafkaConsumerConfig.isSourceKafkaCommitOnlyCurrentPartitionsEnable());
        instrumentation.logDebug(additionalConsumerConfig);

        clockInstance = new Clock();

        String stencilUrl = this.kafkaConsumerConfig.getSchemaRegistryStencilUrls();
        stencilClient = this.kafkaConsumerConfig.isSchemaRegistryStencilEnable()
                ? StencilClientFactory.getClient(stencilUrl, FactoryUtil.getStencilConfig(kafkaConsumerConfig), this.statsDReporter.getClient())
                : StencilClientFactory.getClient();
        parser = new KeyOrMessageParser(new ProtoParser(stencilClient, kafkaConsumerConfig.getInputSchemaProtoClass()), kafkaConsumerConfig);
    }

    /**
     * Helps to create consumer based on the config.
     *
     * @return FirehoseConsumer firehose consumer
     */
    public FirehoseConsumer buildConsumer() {

        Filter filter = new MessageFilter(kafkaConsumerConfig, new Instrumentation(statsDReporter, MessageFilter.class));
        GenericKafkaFactory genericKafkaFactory = new GenericKafkaFactory();
        Tracer tracer = NoopTracerFactory.create();
        if (kafkaConsumerConfig.isTraceJaegarEnable()) {
            tracer = Configuration.fromEnv("Firehose" + ": " + kafkaConsumerConfig.getSourceKafkaConsumerGroupId()).getTracer();
        }
        GenericConsumer consumer = genericKafkaFactory.createConsumer(kafkaConsumerConfig, config,
                statsDReporter, filter, tracer);
        Sink retrySink = withRetry(getSink(), genericKafkaFactory, tracer);
        SinkTracer firehoseTracer = new SinkTracer(tracer, kafkaConsumerConfig.getSinkType().name() + " SINK",
                kafkaConsumerConfig.isTraceJaegarEnable());
        return new FirehoseConsumer(consumer, retrySink, clockInstance, firehoseTracer, new Instrumentation(statsDReporter, FirehoseConsumer.class));
    }

    /**
     * return the basic Sink implementation based on the config.
     *
     * @return Sink
     */
    private Sink getSink() {
        instrumentation.logInfo("Sink Type: {}", kafkaConsumerConfig.getSinkType().toString());
        switch (kafkaConsumerConfig.getSinkType()) {
            case JDBC:
                return new JdbcSinkFactory().create(config, statsDReporter, stencilClient);
            case HTTP:
                return new HttpSinkFactory().create(config, statsDReporter, stencilClient);
            case INFLUXDB:
                return new InfluxSinkFactory().create(config, statsDReporter, stencilClient);
            case LOG:
                return new LogSinkFactory().create(config, statsDReporter, stencilClient);
            case ELASTICSEARCH:
                return new EsSinkFactory().create(config, statsDReporter, stencilClient);
            case REDIS:
                return new RedisSinkFactory().create(config, statsDReporter, stencilClient);
            case GRPC:
                return new GrpcSinkFactory().create(config, statsDReporter, stencilClient);
            case PROMETHEUS:
                return new PromSinkFactory().create(config, statsDReporter, stencilClient);
            case MONGODB:
                return new MongoSinkFactory().create(config, statsDReporter, stencilClient);

            default:
                throw new EglcConfigurationException("Invalid Firehose SINK_TYPE");

        }
    }

    /**
     * to enable the retry feature for the basic sinks based on the config.
     *
     * @param basicSink
     * @param genericKafkaFactory
     * @return Sink
     */
    private Sink withRetry(Sink basicSink, GenericKafkaFactory genericKafkaFactory, Tracer tracer) {
        AppConfig appConfig = ConfigFactory.create(AppConfig.class,
                config);
        BackOffProvider backOffProvider = new ExponentialBackOffProvider(
                appConfig.getRetryExponentialBackoffInitialMs(),
                appConfig.getRetryExponentialBackoffRate(),
                appConfig.getRetryExponentialBackoffMaxMs(),
                new Instrumentation(statsDReporter, ExponentialBackOffProvider.class),
                new BackOff(new Instrumentation(statsDReporter, BackOff.class)));

        if (appConfig.isDlqEnable()) {
            DlqConfig dlqConfig = ConfigFactory.create(DlqConfig.class, config);

            KafkaProducer<byte[], byte[]> kafkaProducer = genericKafkaFactory.getKafkaProducer(dlqConfig);
            TracingKafkaProducer<byte[], byte[]> tracingProducer = new TracingKafkaProducer<>(kafkaProducer, tracer);

            return SinkWithDlq.withInstrumentationFactory(
                    new SinkWithRetry(basicSink, backOffProvider, new Instrumentation(statsDReporter, SinkWithRetry.class),
                            dlqConfig.getDlqAttemptsToTrigger(), parser),
                    tracingProducer, dlqConfig.getDlqKafkaTopic(), statsDReporter, backOffProvider);
        } else {
            return new SinkWithRetry(basicSink, backOffProvider, new Instrumentation(statsDReporter, SinkWithRetry.class), parser);
        }
    }
}
