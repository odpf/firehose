package com.gotocompany.firehose.sink.elasticsearch.request;

import com.gotocompany.firehose.config.EsSinkConfig;
import com.gotocompany.firehose.config.enums.EsSinkMessageType;
import com.gotocompany.firehose.config.enums.EsSinkRequestType;
import com.gotocompany.firehose.metrics.FirehoseInstrumentation;
import com.gotocompany.firehose.serializer.MessageToJson;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;

public class EsRequestHandlerFactoryTest {

    @Mock
    private EsSinkConfig esSinkConfig;

    @Mock
    private FirehoseInstrumentation firehoseInstrumentation;

    private MessageToJson jsonSerializer;

    @Before
    public void setUp() throws Exception {
        initMocks(this);
    }

    @Test
    public void shouldReturnInsertRequestHandler() {
        when(esSinkConfig.isSinkEsModeUpdateOnlyEnable()).thenReturn(false);
        EsRequestHandlerFactory esRequestHandlerFactory = new EsRequestHandlerFactory(esSinkConfig, firehoseInstrumentation, "id",
                EsSinkMessageType.JSON, jsonSerializer, "customer_id", "booking", "order_number");
        EsRequestHandler requestHandler = esRequestHandlerFactory.getRequestHandler();

        verify(firehoseInstrumentation, times(1)).logInfo("ES request mode: {}", EsSinkRequestType.INSERT_OR_UPDATE);
        assertEquals(EsUpsertRequestHandler.class, requestHandler.getClass());
    }

    @Test
    public void shouldReturnUpdateRequestHandler() {
        when(esSinkConfig.isSinkEsModeUpdateOnlyEnable()).thenReturn(true);
        EsRequestHandlerFactory esRequestHandlerFactory = new EsRequestHandlerFactory(esSinkConfig, firehoseInstrumentation, "id",
                EsSinkMessageType.JSON, jsonSerializer, "customer_id", "booking", "order_number");
        EsRequestHandler requestHandler = esRequestHandlerFactory.getRequestHandler();

        verify(firehoseInstrumentation, times(1)).logInfo("ES request mode: {}", EsSinkRequestType.UPDATE_ONLY);
        assertEquals(EsUpdateRequestHandler.class, requestHandler.getClass());
    }
}
