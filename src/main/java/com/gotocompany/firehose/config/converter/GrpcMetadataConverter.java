package com.gotocompany.firehose.config.converter;

import org.aeonbits.owner.Converter;

import java.lang.reflect.Method;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.lang.StringUtils;

public class GrpcMetadataConverter implements Converter<Map<String, String>> {

    private static final String PAIR_DELIMITER = ",";
    private static final String KEY_VALUE_DELIMITER = ":";
    private static final int KEY_INDEX = 0;
    private static final int VALUE_INDEX = 1;

    @Override
    public Map<String, String> convert(Method method, String input) {
        if (StringUtils.isBlank(input)) {
            return new HashMap<>();
        }
        return Arrays.stream(input.split(PAIR_DELIMITER))
                .filter(StringUtils::isNotBlank)
                .map(pair -> {
                    String[] split = pair.split(KEY_VALUE_DELIMITER);
                    if (split.length < 2 || StringUtils.isBlank(split[KEY_INDEX])) {
                        throw new IllegalArgumentException("Invalid metadata entry: " + pair);
                    }
                    return new AbstractMap.SimpleEntry<>(split[KEY_INDEX].trim(), split[VALUE_INDEX].trim());
                })
                .collect(Collectors.toMap(AbstractMap.SimpleEntry::getKey, AbstractMap.SimpleEntry::getValue));
    }

}
