package com.gotocompany.firehose.sink.mongodb.request;

import com.gotocompany.firehose.config.MongoSinkConfig;
import com.gotocompany.firehose.config.enums.MongoSinkMessageType;
import com.gotocompany.firehose.config.enums.MongoSinkRequestType;
import com.gotocompany.firehose.metrics.FirehoseInstrumentation;
import com.gotocompany.firehose.serializer.MessageToJson;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;

import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;

public class MongoRequestHandlerFactoryTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Mock
    private MongoSinkConfig mongoSinkConfig;

    @Mock
    private FirehoseInstrumentation firehoseInstrumentation;

    private MessageToJson jsonSerializer;

    @Before
    public void setUp() throws Exception {
        initMocks(this);
    }

    @Test
    public void shouldReturnMongoRequestHandler() {
        String primaryKey = "customer_id";

        when(mongoSinkConfig.isSinkMongoModeUpdateOnlyEnable()).thenReturn(new Random().nextBoolean());
        MongoRequestHandlerFactory mongoRequestHandlerFactory = new MongoRequestHandlerFactory(mongoSinkConfig, firehoseInstrumentation, primaryKey,
                MongoSinkMessageType.JSON, jsonSerializer);
        when(mongoSinkConfig.getKafkaRecordParserMode()).thenReturn("message");
        MongoRequestHandler requestHandler = mongoRequestHandlerFactory.getRequestHandler();

        assertEquals(MongoRequestHandler.class, requestHandler.getClass().getSuperclass());
    }

    @Test
    public void shouldReturnUpsertRequestHandler() {
        String primaryKey = "customer_id";

        when(mongoSinkConfig.isSinkMongoModeUpdateOnlyEnable()).thenReturn(false);
        MongoRequestHandlerFactory mongoRequestHandlerFactory = new MongoRequestHandlerFactory(mongoSinkConfig, firehoseInstrumentation, primaryKey,
                MongoSinkMessageType.JSON, jsonSerializer);
        when(mongoSinkConfig.getKafkaRecordParserMode()).thenReturn("message");
        MongoRequestHandler requestHandler = mongoRequestHandlerFactory.getRequestHandler();

        verify(firehoseInstrumentation, times(1)).logInfo("Mongo request mode: {}", MongoSinkRequestType.UPSERT);
        assertEquals(MongoUpsertRequestHandler.class, requestHandler.getClass());
    }

    @Test
    public void shouldReturnUpdateRequestHandler() {
        String primaryKey = "customer_id";

        when(mongoSinkConfig.isSinkMongoModeUpdateOnlyEnable()).thenReturn(true);
        MongoRequestHandlerFactory mongoRequestHandlerFactory = new MongoRequestHandlerFactory(mongoSinkConfig, firehoseInstrumentation, primaryKey,
                MongoSinkMessageType.JSON, jsonSerializer);
        when(mongoSinkConfig.getKafkaRecordParserMode()).thenReturn("message");
        MongoRequestHandler requestHandler = mongoRequestHandlerFactory.getRequestHandler();

        verify(firehoseInstrumentation, times(1)).logInfo("Mongo request mode: {}", MongoSinkRequestType.UPDATE_ONLY);
        assertEquals(MongoUpdateRequestHandler.class, requestHandler.getClass());
    }

    @Test
    public void shouldThrowExceptionWhenInvalidRecordParserMode() {
        String primaryKey = "customer_id";

        MongoRequestHandlerFactory mongoRequestHandlerFactory = new MongoRequestHandlerFactory(mongoSinkConfig, firehoseInstrumentation, primaryKey,
                MongoSinkMessageType.JSON, jsonSerializer);
        when(mongoSinkConfig.getKafkaRecordParserMode()).thenReturn("xyz");

        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("KAFKA_RECORD_PARSER_MODE should be key/message");
        mongoRequestHandlerFactory.getRequestHandler();
    }

    @Test
    public void shouldCreateUpsertRequestHandlerWhenPrimaryKeyNotSpecified() {
        String primaryKey = null;

        when(mongoSinkConfig.isSinkMongoModeUpdateOnlyEnable()).thenReturn(false);
        MongoRequestHandlerFactory mongoRequestHandlerFactory = new MongoRequestHandlerFactory(mongoSinkConfig, firehoseInstrumentation, primaryKey,
                MongoSinkMessageType.JSON, jsonSerializer);
        when(mongoSinkConfig.getKafkaRecordParserMode()).thenReturn("message");
        MongoRequestHandler requestHandler = mongoRequestHandlerFactory.getRequestHandler();

        verify(firehoseInstrumentation, times(1)).logInfo("Mongo request mode: {}", MongoSinkRequestType.UPSERT);
        assertEquals(MongoUpsertRequestHandler.class, requestHandler.getClass());
    }

    @Test
    public void shouldThrowExceptionWhenCreateUpdateRequestHandlerWhenPrimaryKeyNotSpecified() {
        String primaryKey = null;
        when(mongoSinkConfig.isSinkMongoModeUpdateOnlyEnable()).thenReturn(true);
        MongoRequestHandlerFactory mongoRequestHandlerFactory = new MongoRequestHandlerFactory(mongoSinkConfig, firehoseInstrumentation, primaryKey,
                MongoSinkMessageType.JSON, jsonSerializer);

        when(mongoSinkConfig.getKafkaRecordParserMode()).thenReturn("message");
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Primary Key cannot be null in Update-Only mode");
        mongoRequestHandlerFactory.getRequestHandler();
    }
}
