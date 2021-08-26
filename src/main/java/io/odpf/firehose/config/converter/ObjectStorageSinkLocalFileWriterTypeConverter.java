package io.odpf.firehose.config.converter;

import io.odpf.firehose.sink.objectstorage.Constants;
import org.aeonbits.owner.Converter;

import java.lang.reflect.Method;

public class ObjectStorageSinkLocalFileWriterTypeConverter implements Converter<Constants.WriterType> {
    @Override
    public Constants.WriterType convert(Method method, String input) {
        return Constants.WriterType.valueOf(input.toUpperCase());
    }
}
