package com.gotocompany.firehose.config.converter;

import com.gotocompany.firehose.config.enums.HttpSinkParameterSourceType;
import org.aeonbits.owner.Converter;

import java.lang.reflect.Method;

public class HttpSinkParameterSourceTypeConverter implements Converter<HttpSinkParameterSourceType> {
    @Override
    public HttpSinkParameterSourceType convert(Method method, String input) {
        return HttpSinkParameterSourceType.valueOf(input.toUpperCase());
    }
}
