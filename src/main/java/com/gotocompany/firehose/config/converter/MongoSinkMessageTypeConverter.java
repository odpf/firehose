package com.gotocompany.firehose.config.converter;

import com.gotocompany.firehose.config.enums.MongoSinkMessageType;
import org.aeonbits.owner.Converter;

import java.lang.reflect.Method;

public class MongoSinkMessageTypeConverter implements Converter<MongoSinkMessageType> {
    @Override
    public MongoSinkMessageType convert(Method method, String input) {
        try {
            return MongoSinkMessageType.valueOf(input.toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("SINK_MONGO_INPUT_MESSAGE_TYPE must be JSON or PROTOBUF");
        }
    }
}
