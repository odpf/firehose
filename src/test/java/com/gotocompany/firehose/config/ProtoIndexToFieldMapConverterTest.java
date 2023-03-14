package com.gotocompany.firehose.config;

import com.gotocompany.firehose.config.converter.ProtoIndexToFieldMapConverter;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Properties;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

public class ProtoIndexToFieldMapConverterTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void shouldConvertJSONConfigToProperties() {
        String json = "{\"1\":\"order_number\",\"2\":\"event_timestamp\",\"3\":\"driver_id\"}";

        Properties actualProperties = new ProtoIndexToFieldMapConverter().convert(null, json);

        assertThat(actualProperties.size(), equalTo(3));
        assertThat(actualProperties.getProperty("1"), equalTo("order_number"));
        assertThat(actualProperties.getProperty("2"), equalTo("event_timestamp"));
        assertThat(actualProperties.getProperty("3"), equalTo("driver_id"));
    }

    @Test
    public void shouldValidateJsonConfigForDuplicates() {
        String json = "{\"1\":\"order_number\",\"2\":\"event_timestamp\",\"3\":\"event_timestamp\"}";
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("duplicates found in INPUT_SCHEMA_PROTO_TO_COLUMN_MAPPING for : [event_timestamp]");
        new ProtoIndexToFieldMapConverter().convert(null, json);
    }

    @Test
    public void shouldValidateJsonConfigForDuplicatesInNestedJsons() {
        String json = "{\"1\":\"order_number\",\"2\":\"event_timestamp\",\"3\":{\"1\":\"event_timestamp\",\"2\":\"order_number\"}}";
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("duplicates found in INPUT_SCHEMA_PROTO_TO_COLUMN_MAPPING for : [event_timestamp, order_number]");
        new ProtoIndexToFieldMapConverter().convert(null, json);
    }

    @Test
    public void shouldConvertNestedJSONToNestedProperties() throws Exception {
        String json = "{\"1\":{\"1\":\"order_number\",\"2\":\"order_url\",\"3\":\"order_details\"},\"3\":\"number_field\"}";

        Properties actualProperties = new ProtoIndexToFieldMapConverter().convert(null, json);

        Properties expectedNestedProperties = new Properties();
        expectedNestedProperties.put("1", "order_number");
        expectedNestedProperties.put("2", "order_url");
        expectedNestedProperties.put("3", "order_details");

        Properties expectedProperties = new Properties();
        expectedProperties.put("1", expectedNestedProperties);
        expectedProperties.put("3", "number_field");

        assertThat(actualProperties, equalTo(expectedProperties));
    }

    @Test
    public void shouldConvertDoublyNestedJSONToNestedProperties() throws Exception {
        String json = "{\"1\":{\"1\":\"order_number\",\"2\":\"order_url\",\"3\":{\"1\":\"longitude\",\"2\":\"latitude\"}},\"3\":\"number_field\"}";

        Properties actualProperties = new ProtoIndexToFieldMapConverter().convert(null, json);

        Properties expectedFurtherNestedProperties = new Properties();
        expectedFurtherNestedProperties.put("1", "longitude");
        expectedFurtherNestedProperties.put("2", "latitude");

        Properties expectedNestedProperties = new Properties();
        expectedNestedProperties.put("1", "order_number");
        expectedNestedProperties.put("2", "order_url");
        expectedNestedProperties.put("3", expectedFurtherNestedProperties);

        Properties expectedProperties = new Properties();
        expectedProperties.put("1", expectedNestedProperties);
        expectedProperties.put("3", "number_field");

        assertThat(actualProperties, equalTo(expectedProperties));
    }

    @Test
    public void shouldNotProcessEmptyStringAsProperties() {
        String json = "";

        Properties actualProperties = new ProtoIndexToFieldMapConverter().convert(null, json);

        assertNull(actualProperties);
    }

    @Test
    public void shouldNotProcessNullStringAsProperties() {
        Properties actualProperties = new ProtoIndexToFieldMapConverter().convert(null, null);

        assertNull(actualProperties);
    }
}
