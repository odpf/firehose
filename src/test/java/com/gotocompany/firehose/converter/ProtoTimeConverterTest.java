package com.gotocompany.firehose.converter;

import com.google.protobuf.Timestamp;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ProtoTimeConverterTest {
    @Test
    public void shouldReturnProperTimestamp() throws Exception {
        Timestamp timestamp = Timestamp.newBuilder().setSeconds(1479135490).setNanos(333).build();
        assertEquals(1479135490000000333L, ProtoTimeConverter.getEpochTimeInNanoSeconds(timestamp));
    }


    @Test
    public void shouldReturnProperTimestampForOnlySeconds() throws Exception {
        Timestamp timestamp = Timestamp.newBuilder().setSeconds(1479135490).build();
        assertEquals(1479135490000000000L, ProtoTimeConverter.getEpochTimeInNanoSeconds(timestamp));
    }
}
