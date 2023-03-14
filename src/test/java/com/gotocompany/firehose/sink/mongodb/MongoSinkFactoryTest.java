package com.gotocompany.firehose.sink.mongodb;

import com.gotocompany.firehose.config.MongoSinkConfig;
import com.gotocompany.firehose.metrics.FirehoseInstrumentation;
import com.mongodb.MongoClient;
import org.aeonbits.owner.ConfigFactory;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.MockitoAnnotations.initMocks;

public class MongoSinkFactoryTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private MongoSinkFactory mongoSinkFactory;
    private Map<String, String> configuration;

    @Mock
    private FirehoseInstrumentation firehoseInstrumentation;

    private Method buildMongoClientMethod;

    @Before
    public void setUp() {

        try {
            buildMongoClientMethod = MongoSinkFactory.class.getDeclaredMethod("buildMongoClient", MongoSinkConfig.class, FirehoseInstrumentation.class);
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
        }
        buildMongoClientMethod.setAccessible(true);

        configuration = new HashMap<>();
        initMocks(this);
        mongoSinkFactory = new MongoSinkFactory();

        configuration.put("SINK_MONGO_CONNECTION_URLS", "localhost:8080");
        configuration.put("SINK_MONGO_DB_NAME", "sampleDatabase");
        configuration.put("SINK_MONGO_PRIMARY_KEY", "customer_id");
        configuration.put("SINK_MONGO_INPUT_MESSAGE_TYPE", "JSON");
        configuration.put("SINK_MONGO_COLLECTION_NAME", "customers");
        configuration.put("SINK_MONGO_REQUEST_TIMEOUT_MS", "8277");
        configuration.put("SINK_MONGO_RETRY_STATUS_CODE_BLACKLIST", "11000");
        configuration.put("SINK_MONGO_MODE_UPDATE_ONLY_ENABLE", "true");

    }

    @Test
    public void shouldCreateMongoClientWithValidAuthentication() {
        configuration.put("SINK_MONGO_AUTH_ENABLE", "true");
        configuration.put("SINK_MONGO_AUTH_USERNAME", "john_dale");
        configuration.put("SINK_MONGO_AUTH_PASSWORD", "pass@123");
        configuration.put("SINK_MONGO_AUTH_DB", "agents_cred");

        MongoSinkConfig mongoSinkConfig = ConfigFactory.create(MongoSinkConfig.class, configuration);
        MongoClient mongoClient = null;
        try {
            mongoClient = (MongoClient) buildMongoClientMethod.invoke(mongoSinkFactory, mongoSinkConfig, firehoseInstrumentation);
        } catch (IllegalAccessException | InvocationTargetException e) {
            e.printStackTrace();
        }
        assertEquals(mongoClient.getClass(), MongoClient.class);
    }


    @Test
    public void shouldCreateMongoClientWithNoAuthentication() {
        configuration.put("SINK_MONGO_AUTH_ENABLE", "false");

        MongoSinkConfig mongoSinkConfig = ConfigFactory.create(MongoSinkConfig.class, configuration);
        MongoClient mongoClient = null;
        try {
            mongoClient = (MongoClient) buildMongoClientMethod.invoke(mongoSinkFactory, mongoSinkConfig, firehoseInstrumentation);
        } catch (IllegalAccessException | InvocationTargetException e) {
            e.printStackTrace();
        }
        assertEquals(mongoClient.getClass(), MongoClient.class);
    }


    @Test
    public void shouldThrowExceptionWhenCreateMongoClientWithNullUsername() {
        configuration.put("SINK_MONGO_AUTH_ENABLE", "true");
        configuration.put("SINK_MONGO_AUTH_PASSWORD", "pass@123");
        configuration.put("SINK_MONGO_AUTH_DB", "agents_cred");
        thrown.expectMessage("Username cannot be null in Auth mode");
        thrown.expect(IllegalArgumentException.class);

        MongoSinkConfig mongoSinkConfig = ConfigFactory.create(MongoSinkConfig.class, configuration);
        try {
            buildMongoClientMethod.invoke(mongoSinkFactory, mongoSinkConfig, firehoseInstrumentation);
        } catch (InvocationTargetException e) {
            throw (IllegalArgumentException) e.getTargetException();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void shouldThrowExceptionWhenCreateMongoClientWithNullPassword() {
        configuration.put("SINK_MONGO_AUTH_ENABLE", "true");
        configuration.put("SINK_MONGO_AUTH_USERNAME", "john_dale");
        configuration.put("SINK_MONGO_AUTH_DB", "agents_cred");

        thrown.expectMessage("Password cannot be null in Auth mode");
        thrown.expect(IllegalArgumentException.class);
        MongoSinkConfig mongoSinkConfig = ConfigFactory.create(MongoSinkConfig.class, configuration);
        try {
            buildMongoClientMethod.invoke(mongoSinkFactory, mongoSinkConfig, firehoseInstrumentation);
        } catch (InvocationTargetException e) {
            throw (IllegalArgumentException) e.getTargetException();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
    }


    @Test
    public void shouldThrowExceptionWhenCreateMongoClientWithNullAuthDB() {
        configuration.put("SINK_MONGO_AUTH_ENABLE", "true");
        configuration.put("SINK_MONGO_AUTH_USERNAME", "john_dale");
        configuration.put("SINK_MONGO_AUTH_PASSWORD", "pass@123");
        thrown.expectMessage("Auth DB cannot be null in Auth mode");
        thrown.expect(IllegalArgumentException.class);

        MongoSinkConfig mongoSinkConfig = ConfigFactory.create(MongoSinkConfig.class, configuration);
        try {
            buildMongoClientMethod.invoke(mongoSinkFactory, mongoSinkConfig, firehoseInstrumentation);
        } catch (InvocationTargetException e) {
            throw (IllegalArgumentException) e.getTargetException();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
    }
}
