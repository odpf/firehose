package com.gojek.esb.config;

import com.gojek.esb.config.enums.HttpSinkDataFormatType;
import com.gojek.esb.config.converter.HttpSinkDataFormatTypeConverter;
import org.gradle.internal.impldep.org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class HttpSinkDataFormatTypeConverterTest {

    private HttpSinkDataFormatTypeConverter httpSinkDataFormatTypeConverter;

    @Before
    public void setUp() {
        httpSinkDataFormatTypeConverter = new HttpSinkDataFormatTypeConverter();
    }

    @Test
    public void shouldReturnJsonSinkTypeFromLowerCaseInput() {
        HttpSinkDataFormatType httpSinkDataFormatType = httpSinkDataFormatTypeConverter.convert(null, "json");
        Assert.assertTrue(httpSinkDataFormatType.equals(HttpSinkDataFormatType.JSON));
    }

    @Test
    public void shouldReturnJsonSinkTypeFromUpperCaseInput() {
        HttpSinkDataFormatType httpSinkDataFormatType = httpSinkDataFormatTypeConverter.convert(null, "JSON");
        Assert.assertTrue(httpSinkDataFormatType.equals(HttpSinkDataFormatType.JSON));
    }

    @Test
    public void shouldReturnJsonSinkTypeFromMixedCaseInput() {
        HttpSinkDataFormatType httpSinkDataFormatType = httpSinkDataFormatTypeConverter.convert(null, "JsOn");
        Assert.assertTrue(httpSinkDataFormatType.equals(HttpSinkDataFormatType.JSON));
    }

    @Test
    public void shouldReturnProtoSinkTypeFromInput() {
        HttpSinkDataFormatType httpSinkDataFormatType = httpSinkDataFormatTypeConverter.convert(null, "proto");
        Assert.assertTrue(httpSinkDataFormatType.equals(HttpSinkDataFormatType.PROTO));
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowOnIllegalArgument() {
        httpSinkDataFormatTypeConverter.convert(null, "");
    }
}
