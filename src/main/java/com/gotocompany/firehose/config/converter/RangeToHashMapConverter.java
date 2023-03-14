package com.gotocompany.firehose.config.converter;

import org.aeonbits.owner.Converter;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class RangeToHashMapConverter implements Converter<Map<Integer, Boolean>> {

    @Override
    public Map<Integer, Boolean> convert(Method method, String input) {
        String[] ranges = input.split(",");
        Map<Integer, Boolean> statusMap = new HashMap<Integer, Boolean>();

        Arrays.stream(ranges).forEach(range -> {
            List<Integer> rangeList = Arrays.stream(range.split("-")).map(Integer::parseInt).collect(Collectors.toList());
            IntStream.rangeClosed(rangeList.get(0), rangeList.get(1)).forEach(statusCode -> statusMap.put(statusCode, true));
        });
        return statusMap;
    }
}
