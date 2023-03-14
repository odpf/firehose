package com.gotocompany.firehose.config.converter;

import com.gotocompany.firehose.config.enums.HttpSinkRequestMethodType;
import org.aeonbits.owner.Converter;

import java.lang.reflect.Method;


public class HttpSinkRequestMethodConverter implements Converter<HttpSinkRequestMethodType> {
    @Override
    public HttpSinkRequestMethodType convert(Method method, String input) {
        return HttpSinkRequestMethodType.valueOf(input.toUpperCase());
    }
}
