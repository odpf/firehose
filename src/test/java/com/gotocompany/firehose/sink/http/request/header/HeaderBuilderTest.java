package com.gotocompany.firehose.sink.http.request.header;

import com.gotocompany.firehose.config.enums.HttpSinkParameterSourceType;
import com.gotocompany.firehose.message.Message;
import com.gotocompany.firehose.proto.ProtoToFieldMapper;
import com.gotocompany.firehose.sink.http.request.uri.UriParser;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class HeaderBuilderTest {

    @Mock
    private UriParser uriParser;

    @Mock
    private ProtoToFieldMapper protoToFieldMapper;

    private Message message;

    @Before
    public void setUp() {
        initMocks(this);
        String logKey = "CgYIyOm+xgUSBgiE6r7GBRgNIICAgIDA9/y0LigC";
        String logMessage = "CgYIyOm+xgUSBgiE6r7GBRgNIICAgIDA9/y0LigCMAM\u003d";
        message = new Message(Base64.getDecoder().decode(logKey.getBytes()),
                Base64.getDecoder().decode(logMessage.getBytes()), "sample-topic", 0, 100);

        when(uriParser.parse(message, "http://dummy.com")).thenReturn("http://dummy.com");
    }

    @Test
    public void shouldGenerateBaseHeader() {
        String headerConfig = "content-type:json";
        HeaderBuilder headerBuilder = new HeaderBuilder(headerConfig);

        assertEquals("json", headerBuilder.build().get("content-type"));
    }

    @Test
    public void shouldHandleMultipleHeader() {
        String headerConfig = "Authorization:auth_token,Accept:text/plain";
        HeaderBuilder headerBuilder = new HeaderBuilder(headerConfig);

        Map<String, String> header = headerBuilder.build();
        assertEquals("auth_token", header.get("Authorization"));
        assertEquals("text/plain", header.get("Accept"));
    }

    @Test
    public void shouldParseWithNilHeadersInBetween() {
        String headerConfig = "foo:bar,,accept:text/plain";
        HeaderBuilder headerBuilder = new HeaderBuilder(headerConfig);
        Map<String, String> expected = new HashMap<String, String>() {
            {
                put("foo", "bar");
                put("accept", "text/plain");
            }
        };
        assertEquals(expected, headerBuilder.build());
    }

    @Test
    public void shouldNotThrowNullPointerExceptionWhenHeaderConfigEmpty() {
        String headerConfig = "";
        HeaderBuilder headerBuilder = new HeaderBuilder(headerConfig);

        headerBuilder.build();
    }

    @Test
    public void shouldAddBaseHeaderPerMessageIfNotParameterized() {
        String headerConfig = "content-type:json";
        HeaderBuilder headerBuilder = new HeaderBuilder(headerConfig);

        Map<String, String> header = headerBuilder.build(message);
        assertEquals("json", header.get("content-type"));
    }

    @Test
    public void shouldHaveExtraParameterizedHeaderIfParameterizedHeaderEnabled() {
        String headerConfig = "content-type:json";
        Map<String, Object> mockParamMap = Collections.singletonMap("orderNumber", "RB_1234");
        when(protoToFieldMapper.getFields(message.getLogMessage())).thenReturn(mockParamMap);

        HeaderBuilder headerBuilder = new HeaderBuilder(headerConfig)
                .withParameterizedHeader(protoToFieldMapper, HttpSinkParameterSourceType.MESSAGE);

        Map<String, String> header = headerBuilder.build(message);

        assertEquals("RB_1234", header.get("orderNumber"));
    }

    @Test
    public void shouldKeepBaseHeadersAndAddExtraHeaderAsItIsProvideInTheConfig() {
        String headerConfig = "content-type:json";
        Map<String, Object> mockParamMap = Collections.singletonMap("X-OrderNumber", "RB_1234");
        when(protoToFieldMapper.getFields(message.getLogMessage())).thenReturn(mockParamMap);

        HeaderBuilder headerBuilder = new HeaderBuilder(headerConfig)
                .withParameterizedHeader(protoToFieldMapper, HttpSinkParameterSourceType.MESSAGE);

        Map<String, String> header = headerBuilder.build(message);

        assertEquals("RB_1234", header.get("X-OrderNumber"));
        assertEquals("json", header.get("content-type"));
    }

    @Test
    public void shouldUseLogKeyWhenSourceTypeIsKey() {
        String headerConfig = "content-type:json";
        HeaderBuilder headerBuilder = new HeaderBuilder(headerConfig)
                .withParameterizedHeader(protoToFieldMapper, HttpSinkParameterSourceType.KEY);

        headerBuilder.build(message);
        verify(protoToFieldMapper, times(1)).getFields(message.getLogKey());
    }

    @Test
    public void shouldUseLogMessageWhenSourceTypeIsMessage() {
        String headerConfig = "content-type:json";
        HeaderBuilder headerBuilder = new HeaderBuilder(headerConfig)
                .withParameterizedHeader(protoToFieldMapper, HttpSinkParameterSourceType.MESSAGE);

        headerBuilder.build(message);
        verify(protoToFieldMapper, times(1)).getFields(message.getLogMessage());
    }
}
