package org.raystack.firehose.config.converter;

import org.raystack.firehose.config.enums.FilterEngineType;
import org.aeonbits.owner.Converter;

import java.lang.reflect.Method;

public class FilterEngineTypeConverter implements Converter<FilterEngineType> {
    @Override
    public FilterEngineType convert(Method method, String input) {
        try {
            return FilterEngineType.valueOf(input.toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("FILTER_ENGINE must be JSON or JEXL or NOOP", e);
        }
    }
}
