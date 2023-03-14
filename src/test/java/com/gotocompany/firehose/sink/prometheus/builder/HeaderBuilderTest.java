package com.gotocompany.firehose.sink.prometheus.builder;

import com.gotocompany.firehose.sink.prometheus.PromSinkConstants;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class HeaderBuilderTest {

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
    public void shouldParseWithRequiredPromHeadersInBetween() {
        String headerConfig = "foo:bar,,accept:text/plain";
        HeaderBuilder headerBuilder = new HeaderBuilder(headerConfig);
        Map<String, String> expected = new HashMap<String, String>() {
            {
                put("foo", "bar");
                put("accept", "text/plain");
                put(PromSinkConstants.PROMETHEUS_REMOTE_WRITE_VERSION, PromSinkConstants.PROMETHEUS_REMOTE_WRITE_VERSION_DEFAULT);
                put(PromSinkConstants.CONTENT_ENCODING, PromSinkConstants.CONTENT_ENCODING_DEFAULT);
            }
        };
        assertEquals(expected, headerBuilder.build());
    }

    @Test
    public void shouldNotThrowNullPointerExceptionWhenHeaderConfigEmpty() {
        String headerConfig = "";
        HeaderBuilder headerBuilder = new HeaderBuilder(headerConfig);

        Map<String, String> header = headerBuilder.build();
        Assert.assertEquals(PromSinkConstants.PROMETHEUS_REMOTE_WRITE_VERSION_DEFAULT, header.get(PromSinkConstants.PROMETHEUS_REMOTE_WRITE_VERSION));
        Assert.assertEquals(PromSinkConstants.CONTENT_ENCODING_DEFAULT, header.get(PromSinkConstants.CONTENT_ENCODING));
    }

    @Test
    public void shouldAddBaseHeaderPerMessageIfNotParameterized() {
        String headerConfig = "content-type:json";
        HeaderBuilder headerBuilder = new HeaderBuilder(headerConfig);

        Map<String, String> header = headerBuilder.build();
        assertEquals("json", header.get("content-type"));
    }
}

