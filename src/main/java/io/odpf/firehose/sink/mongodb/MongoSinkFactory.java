package io.odpf.firehose.sink.mongodb;

import io.odpf.stencil.client.StencilClient;
import io.odpf.stencil.parser.ProtoParser;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import io.odpf.firehose.config.MongoSinkConfig;
import io.odpf.firehose.config.enums.SinkType;
import io.odpf.firehose.metrics.Instrumentation;
import io.odpf.firehose.metrics.StatsDReporter;
import io.odpf.firehose.serializer.MessageToJson;
import io.odpf.firehose.sink.Sink;
import io.odpf.firehose.sink.SinkFactory;
import io.odpf.firehose.sink.mongodb.client.MongoSinkClient;
import io.odpf.firehose.sink.mongodb.request.MongoRequestHandler;
import io.odpf.firehose.sink.mongodb.request.MongoRequestHandlerFactory;
import io.odpf.firehose.sink.mongodb.util.MongoSinkFactoryUtil;
import org.aeonbits.owner.ConfigFactory;

import java.util.List;
import java.util.Map;

/**
 * Sink factory to configure and create MongoDB sink.
 *
 * @since 0.1
 */
public class MongoSinkFactory implements SinkFactory {

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
    @Override
    public Sink create(Map<String, String> configuration, StatsDReporter statsDReporter, StencilClient stencilClient) {
        MongoSinkConfig mongoSinkConfig = ConfigFactory.create(MongoSinkConfig.class, configuration);
        Instrumentation instrumentation = new Instrumentation(statsDReporter, MongoSinkFactory.class);

        logMongoConfig(mongoSinkConfig, instrumentation);
        MongoRequestHandler mongoRequestHandler = new MongoRequestHandlerFactory(mongoSinkConfig, new Instrumentation(statsDReporter, MongoRequestHandlerFactory.class),
                mongoSinkConfig.getSinkMongoPrimaryKey(), mongoSinkConfig.getSinkMongoInputMessageType(),
                new MessageToJson(new ProtoParser(stencilClient, mongoSinkConfig.getInputSchemaProtoClass()), mongoSinkConfig.isSinkMongoPreserveProtoFieldNamesEnable(), false)
        ).getRequestHandler();

        MongoClient mongoClient = buildMongoClient(mongoSinkConfig, instrumentation);
        MongoSinkClient mongoSinkClient = new MongoSinkClient(mongoSinkConfig, new Instrumentation(statsDReporter, MongoSinkClient.class), mongoClient);
        mongoSinkClient.prepare();
        instrumentation.logInfo("MONGO connection established");

        return new MongoSink(new Instrumentation(statsDReporter, MongoSink.class), SinkType.MONGODB.name().toLowerCase(), mongoRequestHandler,
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
    private MongoClient buildMongoClient(MongoSinkConfig mongoSinkConfig, Instrumentation instrumentation) {
        List<ServerAddress> serverAddresses = MongoSinkFactoryUtil.getServerAddresses(mongoSinkConfig.getSinkMongoConnectionUrls(), instrumentation);
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
    private void logMongoConfig(MongoSinkConfig mongoSinkConfig, Instrumentation instrumentation) {
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

        instrumentation.logDebug(mongoConfig);
    }
}
