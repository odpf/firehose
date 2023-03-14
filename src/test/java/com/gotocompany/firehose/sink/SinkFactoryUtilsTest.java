package com.gotocompany.firehose.sink;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class SinkFactoryUtilsTest {

    @Test
    public void shouldAddSinkConnectorConfigs() {
        Map<String, String> env = new HashMap<String, String>() {{
            put("INPUT_SCHEMA_PROTO_CLASS", "com.test.SomeProtoClass");
            put("INPUT_SCHEMA_PROTO_ALLOW_UNKNOWN_FIELDS_ENABLE", "true");
        }};
        Map<String, String> configs = SinkFactoryUtils.addAdditionalConfigsForSinkConnectors(env);
        Assert.assertEquals("com.test.SomeProtoClass", configs.get("SINK_CONNECTOR_SCHEMA_PROTO_MESSAGE_CLASS"));
        Assert.assertEquals("com.test.SomeProtoClass", configs.get("SINK_CONNECTOR_SCHEMA_PROTO_KEY_CLASS"));
        Assert.assertEquals("firehose_", configs.get("SINK_METRICS_APPLICATION_PREFIX"));
        Assert.assertEquals("true", configs.get("SINK_CONNECTOR_SCHEMA_PROTO_ALLOW_UNKNOWN_FIELDS_ENABLE"));
        Assert.assertEquals("LOG_MESSAGE", configs.get("SINK_CONNECTOR_SCHEMA_MESSAGE_MODE"));
    }
}
