package com.gotocompany.firehose.config;

import com.gotocompany.firehose.config.converter.RangeToHashMapConverter;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertArrayEquals;

public class RangeToHashMapConverterTest {

    @Test
    public void shouldConvertRangeToHashMap() {
        Map<Integer, Boolean> actualHashedRanges = new RangeToHashMapConverter().convert(null, "100-103");
        assertArrayEquals(new Integer[]{100, 101, 102, 103}, actualHashedRanges.keySet().toArray());
    }

    @Test
    public void shouldConvertRangesToHashMap() {
        Map<Integer, Boolean> actualHashedRanges = new RangeToHashMapConverter().convert(null, "100-103,200-203");
        assertArrayEquals(new Integer[]{100, 101, 102, 103, 200, 201, 202, 203}, actualHashedRanges.keySet().toArray());
    }
}
