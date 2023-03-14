package com.gotocompany.firehose.config.converter;

import com.gotocompany.firehose.config.enums.HttpSinkDataFormatType;
import org.aeonbits.owner.Converter;

import java.lang.reflect.Method;


public class HttpSinkParameterDataFormatConverter implements Converter<HttpSinkDataFormatType> {
    @Override
    public HttpSinkDataFormatType convert(Method method, String input) {
        return HttpSinkDataFormatType.valueOf(input.toUpperCase());
    }
}
