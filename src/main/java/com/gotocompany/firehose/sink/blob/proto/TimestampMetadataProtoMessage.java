package com.gotocompany.firehose.sink.blob.proto;

import com.github.os72.protobuf.dynamic.MessageDefinition;
import com.google.protobuf.Timestamp;

/**
 * TimestampMetadataProtoMessage contains proto schema proto message of Timestamp. The schema equals to {@link com.google.protobuf.Timestamp Timestamp} message.
 * This class provides {@link com.github.os72.protobuf.dynamic.MessageDefinition} to generate protobuf descriptor and builder of {@link com.google.protobuf.Timestamp Timestamp} proto message.
 *
 * message Timestamp {
 *     int64 seconds = 1
 *     int42 nanos = 2;
 * }
 *
 */
public class TimestampMetadataProtoMessage {
    private static final String TYPE_NAME = "Timestamp";
    public static final String SECONDS_FIELD_NAME = "seconds";
    public static final String NANOS_FIELD_NAME = "nanos";
    public static final int SECONDS_FIELD_NUMBER = 1;
    public static final int NANOS_FIELD_NUMBER = 2;

    public static MessageDefinition createMessageDefinition() {
        return MessageDefinition.newBuilder(TYPE_NAME)
                .addField("optional", "int64", SECONDS_FIELD_NAME, SECONDS_FIELD_NUMBER)
                .addField("optional", "int32", NANOS_FIELD_NAME, NANOS_FIELD_NUMBER)
                .build();
    }

    public static String getTypeName() {
        return TYPE_NAME;
    }

    public static Timestamp.Builder newBuilder() {
        return Timestamp.newBuilder();
    }
}
