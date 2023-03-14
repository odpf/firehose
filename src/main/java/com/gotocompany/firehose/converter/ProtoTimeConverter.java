package com.gotocompany.firehose.converter;

import com.google.protobuf.Timestamp;

import java.util.concurrent.TimeUnit;

public class ProtoTimeConverter {

    public static long getEpochTimeInNanoSeconds(long timeInSeconds, int nanosOffset) {
        return TimeUnit.NANOSECONDS.convert(timeInSeconds, TimeUnit.SECONDS) + nanosOffset;
    }

    public static long getEpochTimeInNanoSeconds(Timestamp time) {
        return getEpochTimeInNanoSeconds(time.getSeconds(), time.getNanos());
    }
}
