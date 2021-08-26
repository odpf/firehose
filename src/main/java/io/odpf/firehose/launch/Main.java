package io.odpf.firehose.launch;

import io.odpf.firehose.config.KafkaConsumerConfig;
import io.odpf.firehose.consumer.KafkaConsumer;
import io.odpf.firehose.consumer.Task;
import io.odpf.firehose.factory.FirehoseConsumerFactory;
import io.odpf.firehose.metrics.Instrumentation;
import io.odpf.firehose.metrics.StatsDReporter;
import io.odpf.firehose.metrics.StatsDReporterFactory;
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
        StatsDReporter statsDReporter = StatsDReporterFactory
                .fromKafkaConsumerConfig(kafkaConsumerConfig)
                .buildReporter();
        Instrumentation instrumentation = new Instrumentation(statsDReporter, Main.class);
        instrumentation.logInfo("Number of consumer threads: " + kafkaConsumerConfig.getApplicationThreadCount());
        instrumentation.logInfo("Delay to clean up consumer threads in ms: " + kafkaConsumerConfig.getApplicationThreadCleanupDelay());

        Task consumerTask = new Task(
                kafkaConsumerConfig.getApplicationThreadCount(),
                kafkaConsumerConfig.getApplicationThreadCleanupDelay(),
                new Instrumentation(statsDReporter, Task.class),
                taskFinished -> {

                    KafkaConsumer firehoseConsumer = null;
                    try {
                        firehoseConsumer = new FirehoseConsumerFactory(
                                kafkaConsumerConfig,
                                statsDReporter)
                                .buildConsumer();
                        while (true) {
                            if (Thread.interrupted()) {
                                instrumentation.logWarn("Consumer Thread interrupted, leaving the loop!");
                                break;
                            }
                            firehoseConsumer.process();
                        }
                    } catch (Exception e) {
                        instrumentation.captureFatalError(e, "Exception on creating the consumer, exiting the application");
                        System.exit(1);
                    } finally {
                        ensureThreadInterruptStateIsClearedAndClose(firehoseConsumer, instrumentation);
                        taskFinished.run();
                    }
                });
        instrumentation.logInfo("Consumer Task Created");

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            instrumentation.logInfo("Program is going to exit. Have started execution of shutdownHook before this");
            consumerTask.stop();
        }));

        consumerTask.run().waitForCompletion();
        instrumentation.logInfo("Exiting main thread");
    }

    private static void ensureThreadInterruptStateIsClearedAndClose(KafkaConsumer firehoseConsumer, Instrumentation instrumentation) {
        Thread.interrupted();
        try {
            firehoseConsumer.close();
        } catch (IOException e) {
            instrumentation.captureFatalError(e, "Exception on closing firehose consumer");
        }
    }
}
