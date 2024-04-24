package com.gotocompany.firehose.sink.httpv2;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class HttpV2SinkUtilsTest {
    @Test
    public void shouldAddAdditionalConfigsForSyncConsumer() {
        Map<String, String> config = new HashMap<String, String>() {{
            put("SOURCE_KAFKA_CONSUMER_MODE", "sync");
            put("SINK_HTTPV2_MAX_CONNECTIONS", "5");
        }};
        HttpV2SinkUtils.addAdditionalConfigsForHttpV2Sink(config);
        Assert.assertEquals(config.get("SINK_HTTPV2_MAX_CONNECTIONS"), "1");
    }

    @Test
    public void shouldAddAdditionalConfigsForASyncConsumer() {
        Map<String, String> config = new HashMap<String, String>() {{
            put("SOURCE_KAFKA_CONSUMER_MODE", "async");
            put("SINK_POOL_NUM_THREADS", "10");
            put("SINK_HTTPV2_MAX_CONNECTIONS", "5");
        }};
        HttpV2SinkUtils.addAdditionalConfigsForHttpV2Sink(config);
        Assert.assertEquals(config.get("SINK_HTTPV2_MAX_CONNECTIONS"), "10");
    }
}
