package com.gotocompany.firehose.config.converter;

import com.gotocompany.firehose.config.enums.HttpSinkParameterPlacementType;
import org.aeonbits.owner.Converter;

import java.lang.reflect.Method;

public class HttpSinkParameterPlacementTypeConverter implements Converter<HttpSinkParameterPlacementType> {
    @Override
    public HttpSinkParameterPlacementType convert(Method method, String input) {
        return HttpSinkParameterPlacementType.valueOf(input.toUpperCase());
    }
}
