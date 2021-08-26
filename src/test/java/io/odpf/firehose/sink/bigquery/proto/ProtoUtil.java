package io.odpf.firehose.sink.bigquery.proto;

import com.google.protobuf.Duration;
import com.google.protobuf.Timestamp;
import io.odpf.firehose.StatusBQ;
import io.odpf.firehose.TestMessageBQ;
import io.odpf.firehose.TestNestedMessageBQ;

import java.time.Instant;

public class ProtoUtil {
    private static final int TRIP_DURATION_NANOS = 1000000000;
    private static int call = 0;

    public static TestMessageBQ generateTestMessage(Instant now) {
        call++;
        Timestamp createdAt = Timestamp.newBuilder().setSeconds(now.getEpochSecond()).setNanos(now.getNano()).build();
        return TestMessageBQ.newBuilder()
                .setOrderNumber("order-" + call)
                .setOrderUrl("order-url-" + call)
                .setOrderDetails("order-details-" + call)
                .setCreatedAt(createdAt)
                .setStatus(StatusBQ.COMPLETED)
                .setTripDuration(Duration.newBuilder().setSeconds(1).setNanos(TRIP_DURATION_NANOS).build())
                .addUpdatedAt(createdAt)
                .addUpdatedAt(createdAt)
                .build();

    }

    public static TestNestedMessageBQ generateTestNestedMessage(String nestedId, TestMessageBQ message) {
        return TestNestedMessageBQ.newBuilder()
                .setSingleMessage(message)
                .setNestedId(nestedId)
                .build();
    }
}
