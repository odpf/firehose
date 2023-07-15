package org.raystack.firehose.config.converter;

import org.raystack.firehose.config.enums.InputSchemaType;
import org.aeonbits.owner.Converter;

import java.lang.reflect.Method;

public class InputSchemaTypeConverter implements Converter<InputSchemaType> {
    @Override
    public InputSchemaType convert(Method method, String input) {
        return InputSchemaType.valueOf(input.trim().toUpperCase());
    }
}
