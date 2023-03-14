package com.gotocompany.firehose.sink.mongodb;

import com.gotocompany.firehose.config.MongoSinkConfig;
import com.gotocompany.firehose.config.enums.SinkType;
import com.gotocompany.firehose.metrics.FirehoseInstrumentation;
import com.gotocompany.firehose.serializer.MessageToJson;
import com.gotocompany.firehose.sink.mongodb.client.MongoSinkClient;
import com.gotocompany.firehose.sink.mongodb.request.MongoRequestHandler;
import com.gotocompany.firehose.sink.mongodb.request.MongoRequestHandlerFactory;
import com.gotocompany.firehose.sink.mongodb.util.MongoSinkFactoryUtil;
import com.gotocompany.depot.metrics.StatsDReporter;
import com.gotocompany.stencil.client.StencilClient;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.gotocompany.firehose.sink.Sink;
import org.aeonbits.owner.ConfigFactory;

import java.util.List;
import java.util.Map;

/**
 * Sink factory to configure and create MongoDB sink.
 *
 * @since 0.1
 */
public class MongoSinkFactory {

    /**
     * Creates MongoDB sink. Logs a success message to instrumentation
     * upon successful creation of the sink.
     *
     * @param configuration  the configuration map
     * @param statsDReporter the stats d reporter
     * @param stencilClient  the stencil client
     * @return created sink
     * @since 0.1
     */
    public static Sink create(Map<String, String> configuration, StatsDReporter statsDReporter, StencilClient stencilClient) {
        MongoSinkConfig mongoSinkConfig = ConfigFactory.create(MongoSinkConfig.class, configuration);
        FirehoseInstrumentation firehoseInstrumentation = new FirehoseInstrumentation(statsDReporter, MongoSinkFactory.class);

        logMongoConfig(mongoSinkConfig, firehoseInstrumentation);
        MongoRequestHandler mongoRequestHandler = new MongoRequestHandlerFactory(mongoSinkConfig, new FirehoseInstrumentation(statsDReporter, MongoRequestHandlerFactory.class),
                mongoSinkConfig.getSinkMongoPrimaryKey(), mongoSinkConfig.getSinkMongoInputMessageType(),
                new MessageToJson(stencilClient.getParser(mongoSinkConfig.getInputSchemaProtoClass()), mongoSinkConfig.isSinkMongoPreserveProtoFieldNamesEnable(), false)
        ).getRequestHandler();

        MongoClient mongoClient = buildMongoClient(mongoSinkConfig, firehoseInstrumentation);
        MongoSinkClient mongoSinkClient = new MongoSinkClient(mongoSinkConfig, new FirehoseInstrumentation(statsDReporter, MongoSinkClient.class), mongoClient);
        mongoSinkClient.prepare();
        firehoseInstrumentation.logInfo("MONGO connection established");

        return new MongoSink(new FirehoseInstrumentation(statsDReporter, MongoSink.class), SinkType.MONGODB.name().toLowerCase(), mongoRequestHandler,
                mongoSinkClient);
    }

    /**
     * Builds the Mongo client.
     * <p>
     * This method extracts the MongoDB Server URL and port from the MongoSinkConfig.
     * Multiple server seeds are also allowed to connect the MongoClient
     * <p>
     * Then, this method checks whether the parameter SINK_MONGO_AUTH_ENABLE is true or not
     * If Authentication parameter is enabled then it extracts the login credentials, i.e.
     * username, password and the MongoDB authentication database.
     * If Authentication parameter is disabled then the MongoClient session is started
     * in non-authentication mode.
     *
     * @return the mongo client
     * @since 0.1
     */
    private static MongoClient buildMongoClient(MongoSinkConfig mongoSinkConfig, FirehoseInstrumentation firehoseInstrumentation) {
        List<ServerAddress> serverAddresses = MongoSinkFactoryUtil.getServerAddresses(mongoSinkConfig.getSinkMongoConnectionUrls(), firehoseInstrumentation);
        MongoClientOptions options = MongoClientOptions.builder()
                .connectTimeout(mongoSinkConfig.getSinkMongoConnectTimeoutMs())
                .serverSelectionTimeout(mongoSinkConfig.getSinkMongoServerSelectTimeoutMs())
                .build();

        MongoClient mongoClient;
        if (mongoSinkConfig.isSinkMongoAuthEnable()) {

            if (mongoSinkConfig.getSinkMongoAuthUsername() == null) {
                throw new IllegalArgumentException("Username cannot be null in Auth mode");
            }
            if (mongoSinkConfig.getSinkMongoAuthPassword() == null) {
                throw new IllegalArgumentException("Password cannot be null in Auth mode");
            }
            if (mongoSinkConfig.getSinkMongoAuthDB() == null) {
                throw new IllegalArgumentException("Auth DB cannot be null in Auth mode");
            }
            MongoCredential mongoCredential = MongoCredential.createCredential(mongoSinkConfig.getSinkMongoAuthUsername(), mongoSinkConfig.getSinkMongoAuthDB(), mongoSinkConfig.getSinkMongoAuthPassword().toCharArray());
            mongoClient = new MongoClient(serverAddresses, mongoCredential, options);
        } else {
            mongoClient = new MongoClient(serverAddresses, options);
        }
        return mongoClient;
    }

    /**
     * Logs all the configuration parameters of MongoDB Sink to the instrumentation
     * logger, in Debug Mode. If the parameter is null, i.e. not specified, then the
     * logger logs "null" to the log console.
     *
     * @since 0.1
     */
    private static void logMongoConfig(MongoSinkConfig mongoSinkConfig, FirehoseInstrumentation firehoseInstrumentation) {
        String mongoConfig = String.format("\n\tMONGO connection urls: %s"
                        + "\n\tMONGO Database name: %s"
                        + "\n\tMONGO Primary Key: %s"
                        + "\n\tMONGO input message type: %s"
                        + "\n\tMONGO Collection Name: %s"
                        + "\n\tMONGO request timeout in ms: %s"
                        + "\n\tMONGO retry status code blacklist: %s"
                        + "\n\tMONGO update only mode: %s"
                        + "\n\tMONGO Authentication Enable: %s"
                        + "\n\tMONGO Authentication Username: %s"
                        + "\n\tMONGO Authentication Database: %s",

                mongoSinkConfig.getSinkMongoConnectionUrls(),
                mongoSinkConfig.getSinkMongoDBName(),
                mongoSinkConfig.getSinkMongoPrimaryKey(),
                mongoSinkConfig.getSinkMongoInputMessageType(),
                mongoSinkConfig.getSinkMongoCollectionName(),
                mongoSinkConfig.getSinkMongoConnectTimeoutMs(),
                mongoSinkConfig.getSinkMongoRetryStatusCodeBlacklist(),
                mongoSinkConfig.isSinkMongoModeUpdateOnlyEnable(),
                mongoSinkConfig.isSinkMongoAuthEnable(),
                mongoSinkConfig.getSinkMongoAuthUsername(),
                mongoSinkConfig.getSinkMongoAuthDB());

        firehoseInstrumentation.logDebug(mongoConfig);
    }
}
