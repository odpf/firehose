package com.gotocompany.firehose.sink.http.request.types;

import com.gotocompany.firehose.config.HttpSinkConfig;
import com.gotocompany.firehose.config.enums.HttpSinkDataFormatType;
import com.gotocompany.firehose.config.enums.HttpSinkParameterPlacementType;
import com.gotocompany.firehose.config.enums.HttpSinkParameterSourceType;
import com.gotocompany.firehose.config.enums.HttpSinkRequestMethodType;
import com.gotocompany.firehose.message.Message;
import com.gotocompany.firehose.proto.ProtoToFieldMapper;
import com.gotocompany.firehose.sink.http.request.body.JsonBody;
import com.gotocompany.firehose.sink.http.request.entity.RequestEntityBuilder;
import com.gotocompany.firehose.sink.http.request.header.HeaderBuilder;
import com.gotocompany.firehose.sink.http.request.uri.UriBuilder;
import com.gotocompany.depot.metrics.StatsDReporter;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.gradle.internal.impldep.org.junit.Assert.assertFalse;
import static org.gradle.internal.impldep.org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;

public class ParameterizedHeaderRequestTest {

    @Mock
    private UriBuilder uriBuilder;

    @Mock
    private HeaderBuilder headerBuilder;

    @Mock
    private RequestEntityBuilder requestEntityBuilder;

    @Mock
    private JsonBody jsonBody;

    @Mock
    private HttpSinkConfig httpSinkConfig;

    @Mock
    private Message message;

    @Mock
    private ProtoToFieldMapper protoToFieldMapper;

    @Mock
    private StatsDReporter statsDReporter;

    private ParameterizedHeaderRequest parameterizedHeaderRequest;
    private HttpSinkRequestMethodType httpSinkRequestMethodType;

    @Before
    public void setup() {
        initMocks(this);
        httpSinkRequestMethodType = HttpSinkRequestMethodType.POST;
        when(httpSinkConfig.getSinkHttpServiceUrl()).thenReturn("http://127.0.0.1:1080/api");
    }

    @Test
    public void shouldProcessForParametrizedHeaders() {
        when(httpSinkConfig.getSinkHttpParameterSource()).thenReturn(HttpSinkParameterSourceType.MESSAGE);
        when(httpSinkConfig.getSinkHttpParameterPlacement()).thenReturn(HttpSinkParameterPlacementType.HEADER);

        parameterizedHeaderRequest = new ParameterizedHeaderRequest(statsDReporter, httpSinkConfig, jsonBody, httpSinkRequestMethodType, protoToFieldMapper);
        boolean canProcess = parameterizedHeaderRequest.canProcess();
        assertTrue(canProcess);
    }

    @Test
    public void shouldNotProcessIfParameterPlacementDisabled() {
        when(httpSinkConfig.getSinkHttpParameterSource()).thenReturn(HttpSinkParameterSourceType.DISABLED);

        parameterizedHeaderRequest = new ParameterizedHeaderRequest(statsDReporter, httpSinkConfig, jsonBody, httpSinkRequestMethodType, protoToFieldMapper);
        boolean canProcess = parameterizedHeaderRequest.canProcess();

        assertFalse(canProcess);
    }

    @Test
    public void shouldNotProcessIfParameterPlacedInQuery() {
        when(httpSinkConfig.getSinkHttpParameterSource()).thenReturn(HttpSinkParameterSourceType.MESSAGE);
        when(httpSinkConfig.getSinkHttpParameterPlacement()).thenReturn(HttpSinkParameterPlacementType.QUERY);

        parameterizedHeaderRequest = new ParameterizedHeaderRequest(statsDReporter, httpSinkConfig, jsonBody, httpSinkRequestMethodType, protoToFieldMapper);
        boolean canProcess = parameterizedHeaderRequest.canProcess();

        assertFalse(canProcess);
    }

    @Test
    public void shouldNotProcessTemplatesIfAbsent() {
        parameterizedHeaderRequest = new ParameterizedHeaderRequest(statsDReporter, httpSinkConfig, jsonBody, httpSinkRequestMethodType, protoToFieldMapper);
        boolean isTemplate = parameterizedHeaderRequest.isTemplateBody(httpSinkConfig);

        assertFalse(isTemplate);
    }

    @Test
    public void shouldProcessTemplatesIfPresent() {
        when(httpSinkConfig.getSinkHttpDataFormat()).thenReturn(HttpSinkDataFormatType.JSON);
        when(httpSinkConfig.getSinkHttpJsonBodyTemplate()).thenReturn("{\"test\":\"$.routes[0]\", \"$.order_number\" : \"xxx\"}");

        parameterizedHeaderRequest = new ParameterizedHeaderRequest(statsDReporter, httpSinkConfig, jsonBody, httpSinkRequestMethodType, protoToFieldMapper);
        boolean isTemplate = parameterizedHeaderRequest.isTemplateBody(httpSinkConfig);

        assertTrue(isTemplate);
    }

    @org.junit.Test
    public void shouldCheckForTemplateWhileBuilding() throws URISyntaxException {
        when(httpSinkConfig.getSinkHttpDataFormat()).thenReturn(HttpSinkDataFormatType.JSON);
        when(httpSinkConfig.getSinkHttpParameterSource()).thenReturn(HttpSinkParameterSourceType.MESSAGE);
        when(httpSinkConfig.getSinkHttpDataFormat()).thenReturn(HttpSinkDataFormatType.JSON);
        when(httpSinkConfig.getSinkHttpJsonBodyTemplate()).thenReturn("{\"test\":\"$.routes[0]\", \"$.order_number\" : \"xxx\"}");
        when(jsonBody.serialize(any())).thenReturn(Collections.singletonList("test"));
        when(requestEntityBuilder.setWrapping(false)).thenReturn(requestEntityBuilder);
        when(headerBuilder.withParameterizedHeader(protoToFieldMapper, HttpSinkParameterSourceType.MESSAGE)).thenReturn(headerBuilder);

        parameterizedHeaderRequest = new ParameterizedHeaderRequest(statsDReporter, httpSinkConfig, jsonBody, httpSinkRequestMethodType, protoToFieldMapper);
        Request request = parameterizedHeaderRequest.setRequestStrategy(headerBuilder, uriBuilder, requestEntityBuilder);
        request.build(Collections.singletonList(message));

        verify(httpSinkConfig, times(1)).getSinkHttpDataFormat();
        verify(httpSinkConfig, times(1)).getSinkHttpJsonBodyTemplate();
    }

    @org.junit.Test
    public void shouldProcessMessagesIndividually() throws URISyntaxException {
        List<String> serializedMessages = Arrays.asList("Hello", "World!", "How");
        List<Message> messages = Arrays.asList(message, message, message);
        when(httpSinkConfig.getSinkHttpDataFormat()).thenReturn(HttpSinkDataFormatType.JSON);
        when(httpSinkConfig.getSinkHttpParameterSource()).thenReturn(HttpSinkParameterSourceType.MESSAGE);
        when(httpSinkConfig.getSinkHttpJsonBodyTemplate()).thenReturn("{\"test\":\"$.routes[0]\", \"$.order_number\" : \"xxx\"}");
        when(jsonBody.serialize(any())).thenReturn(serializedMessages);
        when(requestEntityBuilder.setWrapping(false)).thenReturn(requestEntityBuilder);
        when(headerBuilder.withParameterizedHeader(protoToFieldMapper, HttpSinkParameterSourceType.MESSAGE)).thenReturn(headerBuilder);

        parameterizedHeaderRequest = new ParameterizedHeaderRequest(statsDReporter, httpSinkConfig, jsonBody, httpSinkRequestMethodType, protoToFieldMapper);
        Request request = parameterizedHeaderRequest
                .setRequestStrategy(headerBuilder, uriBuilder, requestEntityBuilder);
        request.build(messages);

        verify(uriBuilder, times(3)).build(message);
        verify(headerBuilder, times(3)).build(message);
        verify(requestEntityBuilder, times(3)).buildHttpEntity(any(String.class));
    }
}
