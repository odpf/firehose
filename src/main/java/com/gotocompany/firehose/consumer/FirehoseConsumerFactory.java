package com.gotocompany.firehose.consumer;

import com.gotocompany.firehose.consumer.kafka.ConsumerAndOffsetManager;
import com.gotocompany.firehose.consumer.kafka.FirehoseKafkaConsumer;
import com.gotocompany.firehose.consumer.kafka.OffsetManager;
import io.jaegertracing.Configuration;
import com.gotocompany.depot.metrics.StatsDReporter;
import com.gotocompany.firehose.metrics.FirehoseInstrumentation;
import com.gotocompany.firehose.sink.SinkFactory;
import com.gotocompany.firehose.utils.KafkaUtils;
import com.gotocompany.firehose.config.AppConfig;
import com.gotocompany.firehose.config.DlqConfig;
import com.gotocompany.firehose.config.FilterConfig;
import com.gotocompany.firehose.config.ErrorConfig;
import com.gotocompany.firehose.config.KafkaConsumerConfig;
import com.gotocompany.firehose.config.SinkPoolConfig;
import com.gotocompany.firehose.config.enums.KafkaConsumerMode;
import com.gotocompany.firehose.sink.SinkPool;
import com.gotocompany.firehose.filter.Filter;
import com.gotocompany.firehose.filter.NoOpFilter;
import com.gotocompany.firehose.filter.jexl.JexlFilter;
import com.gotocompany.firehose.filter.json.JsonFilter;
import com.gotocompany.firehose.filter.json.JsonFilterUtil;
import com.gotocompany.firehose.sink.Sink;
import com.gotocompany.firehose.sink.common.KeyOrMessageParser;
import com.gotocompany.firehose.sinkdecorator.BackOff;
import com.gotocompany.firehose.sinkdecorator.BackOffProvider;
import com.gotocompany.firehose.error.ErrorHandler;
import com.gotocompany.firehose.sinkdecorator.ExponentialBackOffProvider;
import com.gotocompany.firehose.sinkdecorator.SinkFinal;
import com.gotocompany.firehose.sinkdecorator.SinkWithDlq;
import com.gotocompany.firehose.sinkdecorator.SinkWithFailHandler;
import com.gotocompany.firehose.sinkdecorator.SinkWithRetry;
import com.gotocompany.firehose.sink.dlq.DlqWriter;
import com.gotocompany.firehose.sink.dlq.DlqWriterFactory;
import com.gotocompany.firehose.tracer.SinkTracer;
import com.gotocompany.firehose.utils.StencilUtils;
import com.gotocompany.stencil.StencilClientFactory;
import com.gotocompany.stencil.client.StencilClient;
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
    private final Map<String, String> config = System.getenv();
    private final StatsDReporter statsDReporter;
    private final StencilClient stencilClient;
    private final FirehoseInstrumentation firehoseInstrumentation;
    private final KeyOrMessageParser parser;
    private final OffsetManager offsetManager = new OffsetManager();

    /**
     * Instantiates a new Firehose consumer factory.
     *
     * @param kafkaConsumerConfig the kafka consumer config
     * @param statsDReporter      the stats d reporter
     */
    public FirehoseConsumerFactory(KafkaConsumerConfig kafkaConsumerConfig, StatsDReporter statsDReporter) {
        this.kafkaConsumerConfig = kafkaConsumerConfig;
        this.statsDReporter = statsDReporter;
        firehoseInstrumentation = new FirehoseInstrumentation(this.statsDReporter, FirehoseConsumerFactory.class);

        String additionalConsumerConfig = String.format(""
                        + "\n\tEnable Async Commit: %s"
                        + "\n\tCommit Only Current Partition: %s",
                this.kafkaConsumerConfig.isSourceKafkaAsyncCommitEnable(),
                this.kafkaConsumerConfig.isSourceKafkaCommitOnlyCurrentPartitionsEnable());
        firehoseInstrumentation.logDebug(additionalConsumerConfig);

        String stencilUrl = this.kafkaConsumerConfig.getSchemaRegistryStencilUrls();
        stencilClient = this.kafkaConsumerConfig.isSchemaRegistryStencilEnable()
                ? StencilClientFactory.getClient(stencilUrl, StencilUtils.getStencilConfig(kafkaConsumerConfig, statsDReporter.getClient()))
                : StencilClientFactory.getClient();
        parser = new KeyOrMessageParser(stencilClient.getParser(kafkaConsumerConfig.getInputSchemaProtoClass()), kafkaConsumerConfig);
    }

    private FirehoseFilter buildFilter(FilterConfig filterConfig) {
        firehoseInstrumentation.logInfo("Filter Engine: {}", filterConfig.getFilterEngine());
        Filter filter;
        switch (filterConfig.getFilterEngine()) {
            case JSON:
                FirehoseInstrumentation jsonFilterUtilFirehoseInstrumentation = new FirehoseInstrumentation(statsDReporter, JsonFilterUtil.class);
                JsonFilterUtil.logConfigs(filterConfig, jsonFilterUtilFirehoseInstrumentation);
                JsonFilterUtil.validateConfigs(filterConfig, jsonFilterUtilFirehoseInstrumentation);
                filter = new JsonFilter(stencilClient, filterConfig, new FirehoseInstrumentation(statsDReporter, JsonFilter.class));
                break;
            case JEXL:
                filter = new JexlFilter(filterConfig, new FirehoseInstrumentation(statsDReporter, JexlFilter.class));
                break;
            case NO_OP:
                filter = new NoOpFilter(new FirehoseInstrumentation(statsDReporter, NoOpFilter.class));
                break;
            default:
                throw new IllegalArgumentException("Invalid filter engine type");
        }
        return new FirehoseFilter(filter, new FirehoseInstrumentation(statsDReporter, FirehoseFilter.class));
    }

    /**
     * Helps to create consumer based on the config.
     *
     * @return FirehoseConsumer firehose consumer
     */
    public FirehoseConsumer buildConsumer() {
        FilterConfig filterConfig = ConfigFactory.create(FilterConfig.class, config);
        FirehoseFilter firehoseFilter = buildFilter(filterConfig);
        Tracer tracer = NoopTracerFactory.create();
        if (kafkaConsumerConfig.isTraceJaegarEnable()) {
            tracer = Configuration.fromEnv("Firehose" + ": " + kafkaConsumerConfig.getSourceKafkaConsumerGroupId()).getTracer();
        }
        FirehoseKafkaConsumer firehoseKafkaConsumer = KafkaUtils.createConsumer(kafkaConsumerConfig, config, statsDReporter, tracer);
        SinkTracer firehoseTracer = new SinkTracer(tracer, kafkaConsumerConfig.getSinkType().name() + " SINK",
                kafkaConsumerConfig.isTraceJaegarEnable());
        SinkFactory sinkFactory = new SinkFactory(kafkaConsumerConfig, statsDReporter, stencilClient, offsetManager);
        sinkFactory.init();
        if (kafkaConsumerConfig.getSourceKafkaConsumerMode().equals(KafkaConsumerMode.SYNC)) {
            Sink sink = createSink(tracer, sinkFactory);
            ConsumerAndOffsetManager consumerAndOffsetManager = new ConsumerAndOffsetManager(Collections.singletonList(sink), offsetManager, firehoseKafkaConsumer, kafkaConsumerConfig, new FirehoseInstrumentation(statsDReporter, ConsumerAndOffsetManager.class));
            return new FirehoseSyncConsumer(
                    sink,
                    firehoseTracer,
                    consumerAndOffsetManager,
                    firehoseFilter,
                    new FirehoseInstrumentation(statsDReporter, FirehoseSyncConsumer.class));
        } else {
            SinkPoolConfig sinkPoolConfig = ConfigFactory.create(SinkPoolConfig.class, config);
            int nThreads = sinkPoolConfig.getSinkPoolNumThreads();
            List<Sink> sinks = new ArrayList<>(nThreads);
            for (int ii = 0; ii < nThreads; ii++) {
                sinks.add(createSink(tracer, sinkFactory));
            }
            ConsumerAndOffsetManager consumerAndOffsetManager = new ConsumerAndOffsetManager(sinks, offsetManager, firehoseKafkaConsumer, kafkaConsumerConfig, new FirehoseInstrumentation(statsDReporter, ConsumerAndOffsetManager.class));
            SinkPool sinkPool = new SinkPool(
                    new LinkedBlockingQueue<>(sinks),
                    Executors.newCachedThreadPool(),
                    sinkPoolConfig.getSinkPoolQueuePollTimeoutMS());
            return new FirehoseAsyncConsumer(
                    sinkPool,
                    firehoseTracer,
                    consumerAndOffsetManager,
                    firehoseFilter,
                    new FirehoseInstrumentation(statsDReporter, FirehoseAsyncConsumer.class));
        }
    }

    private Sink createSink(Tracer tracer, SinkFactory sinkFactory) {
        ErrorHandler errorHandler = new ErrorHandler(ConfigFactory.create(ErrorConfig.class, config));
        Sink baseSink = sinkFactory.getSink();
        Sink sinkWithFailHandler = new SinkWithFailHandler(baseSink, errorHandler);
        Sink sinkWithRetry = withRetry(sinkWithFailHandler, errorHandler);
        Sink sinkWithDLQ = withDlq(sinkWithRetry, tracer, errorHandler);
        return new SinkFinal(sinkWithDLQ, new FirehoseInstrumentation(statsDReporter, SinkFinal.class));
    }

    public Sink withDlq(Sink sink, Tracer tracer, ErrorHandler errorHandler) {
        DlqConfig dlqConfig = ConfigFactory.create(DlqConfig.class, config);
        if (!dlqConfig.getDlqSinkEnable()) {
            return sink;
        }
        DlqWriter dlqWriter = DlqWriterFactory.create(new HashMap<>(config), statsDReporter, tracer);
        BackOffProvider backOffProvider = getBackOffProvider();
        return new SinkWithDlq(
                sink,
                dlqWriter,
                backOffProvider,
                dlqConfig,
                errorHandler,
                new FirehoseInstrumentation(statsDReporter, SinkWithDlq.class));
    }

    /**
     * to enable the retry feature for the basic sinks based on the config.
     *
     * @param sink         Sink To wrap with retry decorator
     * @param errorHandler error handler
     * @return Sink with retry decorator
     */
    private Sink withRetry(Sink sink, ErrorHandler errorHandler) {
        AppConfig appConfig = ConfigFactory.create(AppConfig.class, config);
        BackOffProvider backOffProvider = getBackOffProvider();
        return new SinkWithRetry(sink, backOffProvider, new FirehoseInstrumentation(statsDReporter, SinkWithRetry.class), appConfig, parser, errorHandler);
    }

    private BackOffProvider getBackOffProvider() {
        AppConfig appConfig = ConfigFactory.create(AppConfig.class, config);
        return new ExponentialBackOffProvider(
                appConfig.getRetryExponentialBackoffInitialMs(),
                appConfig.getRetryExponentialBackoffRate(),
                appConfig.getRetryExponentialBackoffMaxMs(),
                new FirehoseInstrumentation(statsDReporter, ExponentialBackOffProvider.class),
                new BackOff(new FirehoseInstrumentation(statsDReporter, BackOff.class)));
    }
}
