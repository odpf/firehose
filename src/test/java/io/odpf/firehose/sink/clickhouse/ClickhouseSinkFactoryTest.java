package io.odpf.firehose.sink.clickhouse;

import io.odpf.firehose.proto.ProtoToFieldMapper;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.HashMap;
import java.util.Map;

@RunWith(MockitoJUnitRunner.class)
public class ClickhouseSinkFactoryTest {

    private Map<String, String> configuration = new HashMap<>();

    @Mock
    private ProtoToFieldMapper protoToFieldMapper;

    @Before
    public void setUp() {
        configuration.put("SINK_CLICKHOUSE_HOST", "localhost");
        configuration.put("SINK_CLICKHOUSE_PORT", "8090");
        configuration.put("SINK_CLICKHOUSE_DATABASE", "localhost");
        configuration.put("SINK_CLICKHOUSE_USERNAME", "localhost");
        configuration.put("SINK_CLICKHOUSE_PASSWORD", "localhost");
        configuration.put("SINK_CLICKHOUSE_TABLE_NAME", "localhost");
        configuration.put("SINK_CLICKHOUSE_ASYNC_MODE_ENABLE", "true");
        configuration.put("INPUT_SCHEMA_PROTO_TO_COLUMN_MAPPING", "{\"1\":\"order_number\",\"2\":\"order_url\",\"3\":\"order_details\",\"4\":\"order_name\",\"5\":\"order_quantity\"}");
        configuration.put("INPUT_SCHEMA_PROTO_CLASS", "protoclass");
    }

    @Test
    public void testCreateSink() {

        Assert.assertNotNull(ClickhouseSinkFactory.create(configuration, null, protoToFieldMapper));
    }
}
