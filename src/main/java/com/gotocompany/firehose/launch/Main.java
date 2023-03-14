package com.gotocompany.firehose.launch;

import com.gotocompany.firehose.consumer.FirehoseConsumer;
import com.gotocompany.firehose.consumer.FirehoseConsumerFactory;
import com.gotocompany.firehose.metrics.FirehoseInstrumentation;
import com.gotocompany.firehose.metrics.Metrics;
import com.gotocompany.depot.config.MetricsConfig;
import com.gotocompany.depot.metrics.StatsDReporter;
import com.gotocompany.depot.metrics.StatsDReporterBuilder;
import com.gotocompany.firehose.config.KafkaConsumerConfig;
import org.aeonbits.owner.ConfigFactory;

import java.io.IOException;

/**
 * Main class to run firehose.
 */
public class Main {

    /**
     * The entry point of application.
     *
     * @param args the input arguments
     * @throws InterruptedException the interrupted exception
     */
    public static void main(String[] args) throws InterruptedException {
        KafkaConsumerConfig kafkaConsumerConfig = ConfigFactory.create(KafkaConsumerConfig.class, System.getenv());
        multiThreadedConsumers(kafkaConsumerConfig);
    }

    private static void multiThreadedConsumers(KafkaConsumerConfig kafkaConsumerConfig) throws InterruptedException {
        MetricsConfig config = ConfigFactory.create(MetricsConfig.class, System.getenv());
        StatsDReporter statsDReporter = StatsDReporterBuilder.builder().withMetricConfig(config)
                .withExtraTags(Metrics.tag(Metrics.CONSUMER_GROUP_ID_TAG, kafkaConsumerConfig.getSourceKafkaConsumerGroupId()))
                .build();
        FirehoseInstrumentation firehoseInstrumentation = new FirehoseInstrumentation(statsDReporter, Main.class);
        firehoseInstrumentation.logInfo("Number of consumer threads: " + kafkaConsumerConfig.getApplicationThreadCount());
        firehoseInstrumentation.logInfo("Delay to clean up consumer threads in ms: " + kafkaConsumerConfig.getApplicationThreadCleanupDelay());

        Task consumerTask = new Task(
                kafkaConsumerConfig.getApplicationThreadCount(),
                kafkaConsumerConfig.getApplicationThreadCleanupDelay(),
                new FirehoseInstrumentation(statsDReporter, Task.class),
                taskFinished -> {

                    FirehoseConsumer firehoseConsumer = null;
                    try {
                        firehoseConsumer = new FirehoseConsumerFactory(kafkaConsumerConfig, statsDReporter).buildConsumer();
                        while (true) {
                            if (Thread.interrupted()) {
                                firehoseInstrumentation.logWarn("Consumer Thread interrupted, leaving the loop!");
                                break;
                            }
                            firehoseConsumer.process();
                        }
                    } catch (Exception | Error e) {
                        firehoseInstrumentation.captureFatalError("firehose_error_event", e, "Caught exception or error, exiting the application");
                        System.exit(1);
                    } finally {
                        ensureThreadInterruptStateIsClearedAndClose(firehoseConsumer, firehoseInstrumentation);
                        taskFinished.run();
                    }
                });
        firehoseInstrumentation.logInfo("Consumer Task Created");

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            firehoseInstrumentation.logInfo("Program is going to exit. Have started execution of shutdownHook before this");
            consumerTask.stop();
        }));

        consumerTask.run().waitForCompletion();
        firehoseInstrumentation.logInfo("Exiting main thread");
    }

    private static void ensureThreadInterruptStateIsClearedAndClose(FirehoseConsumer firehoseConsumer, FirehoseInstrumentation firehoseInstrumentation) {
        Thread.interrupted();
        try {
            firehoseConsumer.close();
        } catch (IOException e) {
            firehoseInstrumentation.captureFatalError("firehose_error_event", e, "Exception on closing firehose consumer");
        }
    }
}
