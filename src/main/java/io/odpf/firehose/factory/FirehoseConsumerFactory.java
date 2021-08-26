package io.odpf.firehose.factory;

import com.gojek.de.stencil.StencilClientFactory;
import com.gojek.de.stencil.client.StencilClient;
import com.gojek.de.stencil.parser.ProtoParser;
import io.jaegertracing.Configuration;
import io.odpf.firehose.config.AppConfig;
import io.odpf.firehose.config.DlqConfig;
import io.odpf.firehose.config.ErrorConfig;
import io.odpf.firehose.config.KafkaConsumerConfig;
import io.odpf.firehose.config.enums.KafkaConsumerMode;
import io.odpf.firehose.consumer.ConsumerAndOffsetManager;
import io.odpf.firehose.consumer.FirehoseAsyncConsumer;
import io.odpf.firehose.consumer.FirehoseConsumer;
import io.odpf.firehose.consumer.GenericConsumer;
import io.odpf.firehose.consumer.KafkaConsumer;
import io.odpf.firehose.consumer.SinkPool;
import io.odpf.firehose.exception.EglcConfigurationException;
import io.odpf.firehose.filter.Filter;
import io.odpf.firehose.filter.MessageFilter;
import io.odpf.firehose.metrics.Instrumentation;
import io.odpf.firehose.metrics.StatsDReporter;
import io.odpf.firehose.sink.Sink;
import io.odpf.firehose.sink.bigquery.BigQuerySinkFactory;
import io.odpf.firehose.sink.elasticsearch.EsSinkFactory;
import io.odpf.firehose.sink.grpc.GrpcSinkFactory;
import io.odpf.firehose.sink.http.HttpSinkFactory;
import io.odpf.firehose.sink.influxdb.InfluxSinkFactory;
import io.odpf.firehose.sink.jdbc.JdbcSinkFactory;
import io.odpf.firehose.sink.log.KeyOrMessageParser;
import io.odpf.firehose.sink.log.LogSinkFactory;
import io.odpf.firehose.sink.mongodb.MongoSinkFactory;
import io.odpf.firehose.sink.objectstorage.ObjectStorageSinkFactory;
import io.odpf.firehose.sink.prometheus.PromSinkFactory;
import io.odpf.firehose.sink.redis.RedisSinkFactory;
import io.odpf.firehose.sinkdecorator.BackOff;
import io.odpf.firehose.sinkdecorator.BackOffProvider;
import io.odpf.firehose.error.ErrorHandler;
import io.odpf.firehose.sinkdecorator.ExponentialBackOffProvider;
import io.odpf.firehose.sinkdecorator.SinkFinal;
import io.odpf.firehose.sinkdecorator.SinkWithDlq;
import io.odpf.firehose.sinkdecorator.SinkWithFailHandler;
import io.odpf.firehose.sinkdecorator.SinkWithRetry;
import io.odpf.firehose.sinkdecorator.dlq.DlqWriter;
import io.odpf.firehose.sinkdecorator.dlq.DlqWriterFactory;
import io.odpf.firehose.tracer.SinkTracer;
import io.odpf.firehose.util.Clock;
import io.opentracing.Tracer;
import io.opentracing.noop.NoopTracerFactory;
import org.aeonbits.owner.ConfigFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Factory for Firehose consumer.
 */
public class FirehoseConsumerFactory {

    private final KafkaConsumerConfig kafkaConsumerConfig;
    private final Clock clockInstance;
    private final Map<String, String> config = System.getenv();
    private final StatsDReporter statsDReporter;
    private final StencilClient stencilClient;
    private final Instrumentation instrumentation;
    private final KeyOrMessageParser parser;

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
                ? StencilClientFactory.getClient(stencilUrl, config, this.statsDReporter.getClient())
                : StencilClientFactory.getClient();
        parser = new KeyOrMessageParser(new ProtoParser(stencilClient, kafkaConsumerConfig.getInputSchemaProtoClass()), kafkaConsumerConfig);
    }

    /**
     * Helps to create consumer based on the config.
     *
     * @return FirehoseConsumer firehose consumer
     */
    public KafkaConsumer buildConsumer() {

        Filter filter = new MessageFilter(kafkaConsumerConfig, new Instrumentation(statsDReporter, MessageFilter.class));
        GenericKafkaFactory genericKafkaFactory = new GenericKafkaFactory();
        Tracer tracer = NoopTracerFactory.create();
        if (kafkaConsumerConfig.isTraceJaegarEnable()) {
            tracer = Configuration.fromEnv("Firehose" + ": " + kafkaConsumerConfig.getSourceKafkaConsumerGroupId()).getTracer();
        }
        GenericConsumer genericConsumer = genericKafkaFactory.createConsumer(kafkaConsumerConfig, config,
                statsDReporter, filter, tracer);
        SinkTracer firehoseTracer = new SinkTracer(tracer, kafkaConsumerConfig.getSinkType().name() + " SINK",
                kafkaConsumerConfig.isTraceJaegarEnable());
        if (kafkaConsumerConfig.getSourceKafkaConsumerMode().equals(KafkaConsumerMode.SYNC)) {
            Sink sink = createSink(tracer);
            ConsumerAndOffsetManager consumerAndOffsetManager = new ConsumerAndOffsetManager(Collections.singletonList(sink), genericConsumer, kafkaConsumerConfig, new Instrumentation(statsDReporter, ConsumerAndOffsetManager.class));
            return new FirehoseConsumer(
                    sink,
                    clockInstance,
                    firehoseTracer,
                    consumerAndOffsetManager,
                    new Instrumentation(statsDReporter, FirehoseConsumer.class));
        } else {
            int nThreads = kafkaConsumerConfig.getSourceKafkaConsumerThreads();
            List<Sink> sinks = new ArrayList<>(nThreads);
            for (int ii = 0; ii < nThreads; ii++) {
                sinks.add(createSink(tracer));
            }
            ConsumerAndOffsetManager consumerAndOffsetManager = new ConsumerAndOffsetManager(sinks, genericConsumer, kafkaConsumerConfig, new Instrumentation(statsDReporter, ConsumerAndOffsetManager.class));
            SinkPool sinkPool = new SinkPool(
                    new LinkedBlockingQueue<>(sinks),
                    Executors.newCachedThreadPool(),
                    kafkaConsumerConfig.getSourceKafkaConsumerSinkPollTimeoutMillis());
            return new FirehoseAsyncConsumer(
                    sinkPool,
                    clockInstance,
                    firehoseTracer,
                    consumerAndOffsetManager,
                    new Instrumentation(statsDReporter, FirehoseAsyncConsumer.class));
        }
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
            case OBJECTSTORAGE:
                return new ObjectStorageSinkFactory().create(config, statsDReporter, stencilClient);
            case BIGQUERY:
                return new BigQuerySinkFactory().create(config, statsDReporter, stencilClient);
            case MONGODB:
                return new MongoSinkFactory().create(config, statsDReporter, stencilClient);
            default:
                throw new EglcConfigurationException("Invalid Firehose SINK_TYPE");

        }
    }

    private Sink createSink(Tracer tracer) {
        ErrorHandler errorHandler = new ErrorHandler(ConfigFactory.create(ErrorConfig.class, config));
        Sink baseSink = getSink();
        Sink sinkWithFailHandler = new SinkWithFailHandler(baseSink, errorHandler);
        Sink sinkWithRetry = withRetry(sinkWithFailHandler, errorHandler);
        Sink sinWithDLQ = withDlq(sinkWithRetry, tracer, errorHandler);
        return new SinkFinal(sinWithDLQ, new Instrumentation(statsDReporter, SinkFinal.class));
    }

    public Sink withDlq(Sink sink, Tracer tracer, ErrorHandler errorHandler) {
        DlqConfig dlqConfig = ConfigFactory.create(DlqConfig.class, config);
        DlqWriterFactory dlqWriterFactory = new DlqWriterFactory();
        DlqWriter dlqWriter = dlqWriterFactory.create(new HashMap<>(config), statsDReporter, tracer);
        BackOffProvider backOffProvider = getBackOffProvider();
        return SinkWithDlq.withInstrumentationFactory(
                sink,
                dlqWriter,
                backOffProvider,
                dlqConfig,
                errorHandler,
                statsDReporter);
    }

    /**
     * to enable the retry feature for the basic sinks based on the config.
     *
     * @param basicSink
     * @return Sink
     */
    private Sink withRetry(Sink basicSink, ErrorHandler errorHandler) {
        AppConfig appConfig = ConfigFactory.create(AppConfig.class, config);
        BackOffProvider backOffProvider = getBackOffProvider();
        return new SinkWithRetry(basicSink, backOffProvider, new Instrumentation(statsDReporter, SinkWithRetry.class), appConfig, parser, errorHandler);
    }

    private BackOffProvider getBackOffProvider() {
        AppConfig appConfig = ConfigFactory.create(AppConfig.class, config);
        return new ExponentialBackOffProvider(
                appConfig.getRetryExponentialBackoffInitialMs(),
                appConfig.getRetryExponentialBackoffRate(),
                appConfig.getRetryExponentialBackoffMaxMs(),
                new Instrumentation(statsDReporter, ExponentialBackOffProvider.class),
                new BackOff(new Instrumentation(statsDReporter, BackOff.class)));
    }
}
