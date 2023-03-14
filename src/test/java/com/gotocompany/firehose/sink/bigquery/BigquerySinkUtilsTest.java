package com.gotocompany.firehose.sink.bigquery;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

public class BigquerySinkUtilsTest {
    @Test
    public void shouldGetRowIdCreator() {
        Function<Map<String, Object>, String> rowIDCreator = BigquerySinkUtils.getRowIDCreator();
        String rowId = rowIDCreator.apply(new HashMap<String, Object>() {{
            put("message_topic", "test");
            put("message_partition", 10);
            put("message_offset", 2);
            put("something_else", false);
        }});
        Assert.assertEquals("test_10_2", rowId);
    }

    @Test
    public void shouldAddMetadataColumns() {
        Map<String, String> config = new HashMap<String, String>() {{
            put("test", "test");
        }};
        BigquerySinkUtils.addMetadataColumns(config);
        Assert.assertEquals(config.get("SINK_BIGQUERY_METADATA_COLUMNS_TYPES"), "message_offset=integer,message_topic=string,load_time=timestamp,message_timestamp=timestamp,message_partition=integer");
    }
}
