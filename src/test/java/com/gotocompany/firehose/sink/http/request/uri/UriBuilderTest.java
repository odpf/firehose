package com.gotocompany.firehose.sink.http.request.uri;

import com.gotocompany.firehose.config.enums.HttpSinkParameterSourceType;
import com.gotocompany.firehose.message.Message;
import com.gotocompany.firehose.proto.ProtoToFieldMapper;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.gradle.internal.impldep.org.junit.Assert.assertEquals;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class UriBuilderTest {

    @Mock
    private UriParser uriParser;

    @Mock
    private ProtoToFieldMapper protoToFieldMapper;

    private Message message;
    private String serviceUrl;

    @Before
    public void setUp() {
        initMocks(this);
        serviceUrl = "http://dummy.com";
        String logKey = "CgYIyOm+xgUSBgiE6r7GBRgNIICAgIDA9/y0LigC";
        String logMessage = "CgYIyOm+xgUSBgiE6r7GBRgNIICAgIDA9/y0LigCMAM\u003d";
        message = new Message(Base64.getDecoder().decode(logKey.getBytes()),
                Base64.getDecoder().decode(logMessage.getBytes()), "sample-topic", 0, 100);

        when(uriParser.parse(message, "http://dummy.com")).thenReturn("http://dummy.com");
    }

    @Test
    public void shouldReturnURIInstanceBasedOnBaseUrl() {
        UriBuilder uriBuilder = new UriBuilder("http://dummy.com", uriParser);
        try {
            assertEquals(new URI("http://dummy.com"), uriBuilder.build());
        } catch (URISyntaxException e) {
            new RuntimeException(e);
        }
    }

    @Test
    public void shouldReturnParsedURIInstanceBasedOnBaseUrlForDynamicURLs() throws URISyntaxException {
        String serviceURL = "http://dummy.com/%s,6";
        when(uriParser.parse(message, serviceURL)).thenReturn("http://dummy.com/protoField");

        UriBuilder uriBuilder = new UriBuilder(serviceURL, uriParser);

        assertEquals(new URI("http://dummy.com/protoField"), uriBuilder.build(message));
    }

    // Test for parameterizedURI and message values
    @Test
    public void shouldAddParamMapToUri() {
        Map<String, Object> mockProtoField = Collections.singletonMap("order_number", "RB_1234");

        when(protoToFieldMapper.getFields(message.getLogMessage())).thenReturn(mockProtoField);

        UriBuilder uriBuilder = new UriBuilder(serviceUrl, uriParser).withParameterizedURI(protoToFieldMapper, HttpSinkParameterSourceType.MESSAGE);

        try {
            URI actualUri = uriBuilder.build(message);
            Assert.assertEquals(new URI("http://dummy.com?order_number=RB_1234"), actualUri);
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void shouldHandleMultipleParam() {
        Map<String, Object> mockProtoField = new HashMap<String, Object>();
        mockProtoField.put("order_number", "RB_1234");
        mockProtoField.put("service_type", "GO_RIDE");

        when(protoToFieldMapper.getFields(message.getLogMessage())).thenReturn(mockProtoField);

        UriBuilder uriBuilder = new UriBuilder(serviceUrl, uriParser).withParameterizedURI(protoToFieldMapper, HttpSinkParameterSourceType.MESSAGE);

        try {
            URI actualUri = uriBuilder.build(message);
            assertEquals(new URI("http://dummy.com?service_type=GO_RIDE&order_number=RB_1234"), actualUri);
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void shouldUseLogKeyWhenSourceParamTypeIsKey() {
        UriBuilder uriBuilder = new UriBuilder(serviceUrl, uriParser)
                .withParameterizedURI(protoToFieldMapper, HttpSinkParameterSourceType.KEY);
        try {
            uriBuilder.build(message);
            verify(protoToFieldMapper, times(1)).getFields(message.getLogKey());
        } catch (URISyntaxException e) {
            new RuntimeException(e);
        }
    }

    @Test
    public void shouldUseLogMessageWhenSourceParamTypeIsMessage() {
        UriBuilder uriBuilder = new UriBuilder(serviceUrl, uriParser)
                .withParameterizedURI(protoToFieldMapper, HttpSinkParameterSourceType.MESSAGE);
        try {
            uriBuilder.build(message);
            verify(protoToFieldMapper, times(1)).getFields(message.getLogMessage());
        } catch (URISyntaxException e) {
            new RuntimeException(e);
        }
    }
}
