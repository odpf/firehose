package io.odpf.firehose.config.converter;

import io.odpf.firehose.objectstorage.ObjectStorageType;
import org.aeonbits.owner.Converter;

import java.lang.reflect.Method;

public class ObjectStorageTypeConverter implements Converter<ObjectStorageType> {
    @Override
    public ObjectStorageType convert(Method method, String input) {
        return ObjectStorageType.valueOf(input.toUpperCase());
    }
}
