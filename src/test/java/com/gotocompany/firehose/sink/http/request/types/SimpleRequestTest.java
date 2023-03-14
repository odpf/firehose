package com.gotocompany.firehose.sink.http.request.types;

import com.gotocompany.firehose.config.HttpSinkConfig;
import com.gotocompany.firehose.config.enums.HttpSinkDataFormatType;
import com.gotocompany.firehose.config.enums.HttpSinkParameterSourceType;
import com.gotocompany.firehose.config.enums.HttpSinkRequestMethodType;
import com.gotocompany.firehose.message.Message;
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
import java.util.List;

import static org.gradle.internal.impldep.org.junit.Assert.assertFalse;
import static org.gradle.internal.impldep.org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;

public class SimpleRequestTest {

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
    private StatsDReporter statsDReporter;

    @Mock
    private Message message;

    private SimpleRequest simpleRequest;
    private HttpSinkRequestMethodType httpSinkRequestMethodType;

    @Before
    public void setup() {
        initMocks(this);
        httpSinkRequestMethodType = HttpSinkRequestMethodType.POST;
        when(httpSinkConfig.getSinkHttpServiceUrl()).thenReturn("http://127.0.0.1:1080/api");
    }

    @Test
    public void shouldProcessBaseCase() {
        when(httpSinkConfig.getSinkHttpParameterSource()).thenReturn(HttpSinkParameterSourceType.DISABLED);

        simpleRequest = new SimpleRequest(statsDReporter, httpSinkConfig, jsonBody, httpSinkRequestMethodType);
        boolean canProcess = simpleRequest.canProcess();
        assertTrue(canProcess);
    }

    @Test
    public void shouldNotProcessForDyanamicURL() {
        when(httpSinkConfig.getSinkHttpServiceUrl()).thenReturn("http://127.0.0.1:1080/api,%s");

        simpleRequest = new SimpleRequest(statsDReporter, httpSinkConfig, jsonBody, httpSinkRequestMethodType);
        boolean canProcess = simpleRequest.canProcess();

        assertFalse(canProcess);
    }

    @Test
    public void shouldNotProcessIfParameterIsEnabled() {
        when(httpSinkConfig.getSinkHttpParameterSource()).thenReturn(HttpSinkParameterSourceType.MESSAGE);

        simpleRequest = new SimpleRequest(statsDReporter, httpSinkConfig, jsonBody, httpSinkRequestMethodType);
        boolean canProcess = simpleRequest.canProcess();

        assertFalse(canProcess);
    }

    @Test
    public void shouldNotProcessTemplatesIfAbsent() {
        simpleRequest = new SimpleRequest(statsDReporter, httpSinkConfig, jsonBody, httpSinkRequestMethodType);
        boolean isTemplate = simpleRequest.isTemplateBody(httpSinkConfig);

        assertFalse(isTemplate);
    }

    @Test
    public void shouldProcessTemplatesIfPresent() {
        when(httpSinkConfig.getSinkHttpDataFormat()).thenReturn(HttpSinkDataFormatType.JSON);
        when(httpSinkConfig.getSinkHttpJsonBodyTemplate()).thenReturn("{\"test\":\"$.routes[0]\", \"$.order_number\" : \"xxx\"}");

        simpleRequest = new SimpleRequest(statsDReporter, httpSinkConfig, jsonBody, httpSinkRequestMethodType);
        boolean isTemplate = simpleRequest.isTemplateBody(httpSinkConfig);

        assertTrue(isTemplate);
    }

    @Test
    public void shouldCheckTemplateAvailabilityForSettingRequestStrategy() {
        when(httpSinkConfig.getSinkHttpDataFormat()).thenReturn(HttpSinkDataFormatType.JSON);
        when(httpSinkConfig.getSinkHttpJsonBodyTemplate()).thenReturn("{\"test\":\"$.routes[0]\", \"$.order_number\" : \"xxx\"}");

        simpleRequest = new SimpleRequest(statsDReporter, httpSinkConfig, jsonBody, httpSinkRequestMethodType);
        simpleRequest.setRequestStrategy(headerBuilder, uriBuilder, requestEntityBuilder);

        verify(httpSinkConfig, times(1)).getSinkHttpDataFormat();
        verify(httpSinkConfig, times(1)).getSinkHttpJsonBodyTemplate();
    }

    @org.junit.Test
    public void shouldProcessMessagesInBatchIfTemplateDisabled() throws URISyntaxException {
        List<String> serializedMessages = Arrays.asList("Hello", "World!", "How");
        List<Message> messages = Arrays.asList(message, message, message);
        when(httpSinkConfig.getSinkHttpDataFormat()).thenReturn(HttpSinkDataFormatType.PROTO);
        when(jsonBody.serialize(any())).thenReturn(serializedMessages);
        when(requestEntityBuilder.setWrapping(true)).thenReturn(requestEntityBuilder);

        simpleRequest = new SimpleRequest(statsDReporter, httpSinkConfig, jsonBody, httpSinkRequestMethodType);
        Request request = simpleRequest.setRequestStrategy(headerBuilder, uriBuilder, requestEntityBuilder);
        request.build(messages);

        verify(uriBuilder, times(1)).build();
        verify(headerBuilder, times(1)).build();
        verify(requestEntityBuilder, times(1)).buildHttpEntity(any(String.class));
    }

    @Test
    public void shouldProcessMessagesIndividuallyIfTemplateEnabled() throws URISyntaxException {
        List<String> serializedMessages = Arrays.asList("Hello", "World!", "How");
        List<Message> messages = Arrays.asList(message, message, message);
        when(httpSinkConfig.getSinkHttpDataFormat()).thenReturn(HttpSinkDataFormatType.JSON);
        when(httpSinkConfig.getSinkHttpJsonBodyTemplate()).thenReturn("{\"test\":\"$.routes[0]\", \"$.order_number\" : \"xxx\"}");
        when(jsonBody.serialize(any())).thenReturn(serializedMessages);
        when(requestEntityBuilder.setWrapping(false)).thenReturn(requestEntityBuilder);

        simpleRequest = new SimpleRequest(statsDReporter, httpSinkConfig, jsonBody, httpSinkRequestMethodType);
        Request request = simpleRequest.setRequestStrategy(headerBuilder, uriBuilder, requestEntityBuilder);
        request.build(messages);

        verify(uriBuilder, times(3)).build(message);
        verify(headerBuilder, times(3)).build(message);
        verify(requestEntityBuilder, times(3)).buildHttpEntity(any(String.class));
    }
}
