package io.odpf.firehose.config.converter;

import io.odpf.firehose.config.enums.KafkaConsumerMode;
import org.aeonbits.owner.Converter;

import java.lang.reflect.Method;

public class ConsumerModeConverter implements Converter<KafkaConsumerMode> {
    @Override
    public KafkaConsumerMode convert(Method method, String input) {
        return KafkaConsumerMode.valueOf(input.toUpperCase());
    }
}
