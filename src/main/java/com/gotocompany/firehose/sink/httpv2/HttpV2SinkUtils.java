package com.gotocompany.firehose.sink.httpv2;

import com.gotocompany.firehose.config.enums.KafkaConsumerMode;

import java.util.Map;

public class HttpV2SinkUtils {

    public static void addAdditionalConfigsForHttpV2Sink(Map<String, String> env) {

        System.out.println(env.getOrDefault("SOURCE_KAFKA_CONSUMER_MODE", "SYNC").toUpperCase());
        switch (KafkaConsumerMode.valueOf(env.getOrDefault("SOURCE_KAFKA_CONSUMER_MODE", "SYNC").toUpperCase())) {
            case SYNC:
                env.put("SINK_HTTPV2_MAX_CONNECTIONS", "1");
                break;

            case ASYNC:
                env.put("SINK_HTTPV2_MAX_CONNECTIONS", env.getOrDefault("SINK_POOL_NUM_THREADS", "1"));
                break;
            default:
                throw new IllegalArgumentException("Consumer mode should be async or sync");

        }
    }
}
