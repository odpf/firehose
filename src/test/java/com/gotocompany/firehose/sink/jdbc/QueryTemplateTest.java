package com.gotocompany.firehose.sink.jdbc;

import com.gotocompany.firehose.config.JdbcSinkConfig;
import com.gotocompany.firehose.message.Message;
import com.gotocompany.firehose.proto.ProtoToFieldMapper;
import org.gradle.internal.impldep.org.testng.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class QueryTemplateTest {

    @Mock
    private JdbcSinkConfig jdbcSinkConfig;

    @Mock
    private ProtoToFieldMapper protoToFieldMapper;

    @Mock
    private Message mockMessage;

    @Before
    public void setup() {

        when(jdbcSinkConfig.getSinkJdbcTableName()).thenReturn("table");

        Properties properties = new Properties();
        properties.put("1", "order_number");
        properties.put("2", "event_timestamp");
        properties.put("3", "feedback_rating");
        when(jdbcSinkConfig.getInputSchemaProtoToColumnMapping()).thenReturn(properties);

        Map<String, Object> columnToValues = new HashMap<>();
        columnToValues.put("order_number", "order_1");
        columnToValues.put("event_timestamp", "ts1");
        columnToValues.put("feedback_rating", 5);
        when(protoToFieldMapper.getFields(any(byte[].class))).thenReturn(columnToValues);
        when(jdbcSinkConfig.getSinkJdbcUniqueKeys()).thenReturn(String.join(",", ""));
    }

    private void addUniqueKeys(String uniqueKeys) {

        when(jdbcSinkConfig.getSinkJdbcUniqueKeys()).thenReturn(String.join(",", uniqueKeys));
    }

    @Test
    public void shouldUseSingleUniqueKeyColumnForOnConflictResolution() throws IOException, SQLException {
        addUniqueKeys("order_number");
        Message message = new Message("key".getBytes(), "msg".getBytes(), "topic1", 0, 100);
        QueryTemplate queryTemplate = new QueryTemplate(jdbcSinkConfig, protoToFieldMapper);
        String actualSql = queryTemplate.toQueryString(message);

        String expectedSql = "INSERT INTO table ( feedback_rating,event_timestamp,order_number ) values ( '5', 'ts1', 'order_1' ) ON CONFLICT ( order_number ) DO UPDATE SET ( feedback_rating,event_timestamp ) = ('5', 'ts1')";

        Assert.assertEquals(actualSql, expectedSql);
    }

    @Test
    public void shouldUseMultipleUniqueKeyColumnForOnConflictResolution() throws IOException, SQLException {
        addUniqueKeys("order_number, event_timestamp");
        QueryTemplate conflictingQueryTemplate = new QueryTemplate(jdbcSinkConfig, protoToFieldMapper);
        Message message = new Message("key".getBytes(), "msg".getBytes(), "topic1", 0, 100);
        String actualSql = conflictingQueryTemplate.toQueryString(message);

        String expectedSql = "INSERT INTO table ( feedback_rating,event_timestamp,order_number ) values ( '5', 'ts1', 'order_1' ) ON CONFLICT ( order_number, event_timestamp ) DO UPDATE SET ( feedback_rating ) = ('5')";

        Assert.assertEquals(actualSql, expectedSql);
    }

    @Test
    public void shouldNotUpdateWhenAllColumnsAreInUniqueSet() throws IOException, SQLException {
        addUniqueKeys("order_number, event_timestamp, feedback_rating");
        QueryTemplate queryTemplate = new QueryTemplate(jdbcSinkConfig, protoToFieldMapper);
        Message message = new Message("key".getBytes(), "msg".getBytes(), "topic1", 0, 100);
        String actualSql = queryTemplate.toQueryString(message);

        String expectedSql = "INSERT INTO table ( feedback_rating,event_timestamp,order_number ) values ( '5', 'ts1', 'order_1' ) ON CONFLICT ( order_number, event_timestamp, feedback_rating ) DO NOTHING";

        Assert.assertEquals(actualSql, expectedSql);
    }

    @Test
    public void shouldDoSimpleInsertWhenNoUniqueKeysArePresent() {
        QueryTemplate queryTemplate = new QueryTemplate(jdbcSinkConfig, protoToFieldMapper);
        Message message = new Message("key".getBytes(), "msg".getBytes(), "topic1", 0, 100);
        String actualSql = queryTemplate.toQueryString(message);

        String expectedSql = "INSERT INTO table ( feedback_rating,event_timestamp,order_number ) values ( '5', 'ts1', 'order_1' ) ";

        Assert.assertEquals(actualSql, expectedSql);
    }

    @Test
    public void shouldAddNestedColumnsToQuery() throws IOException, SQLException {
        Properties properties2 = new Properties();
        properties2.put("1", "order_number");
        properties2.put("2", "event_timestamp");


        Properties nestedProperties = new Properties();
        nestedProperties.put("1", "latitude");
        nestedProperties.put("2", "longitude");

        properties2.put("4", nestedProperties);
        properties2.put("3", "feedback_rating");
        when(jdbcSinkConfig.getInputSchemaProtoToColumnMapping()).thenReturn(properties2);

        Map<String, Object> columnToValues = new HashMap<>();
        columnToValues.put("order_number", "order_1");
        columnToValues.put("event_timestamp", "ts1");
        columnToValues.put("feedback_rating", 5);
        columnToValues.put("latitude", 3.05);
        columnToValues.put("longitude", 70.02);
        when(protoToFieldMapper.getFields(any(byte[].class))).thenReturn(columnToValues);

        addUniqueKeys("order_number, event_timestamp");
        QueryTemplate queryTemplate = new QueryTemplate(jdbcSinkConfig, protoToFieldMapper);
        Message message = new Message("key".getBytes(), "msg".getBytes(), "topic1", 0, 100);
        String actualSql = queryTemplate.toQueryString(message);

        String expectedSql = "INSERT INTO table ( longitude,latitude,feedback_rating,event_timestamp,order_number ) "
                + "values ( '70.02', '3.05', '5', 'ts1', 'order_1' ) "
                + "ON CONFLICT ( order_number, event_timestamp ) "
                + "DO UPDATE SET ( longitude,latitude,feedback_rating ) = ('70.02', '3.05', '5')";

        Assert.assertEquals(actualSql, expectedSql);
    }

    @Test
    public void shouldUseKafkaRecordKey() throws Exception {
        when(jdbcSinkConfig.getKafkaRecordParserMode()).thenReturn("key");
        when(mockMessage.getLogKey()).thenReturn("key".getBytes());
        QueryTemplate queryTemplate = new QueryTemplate(jdbcSinkConfig, protoToFieldMapper);
        queryTemplate.toQueryString(mockMessage);
        verify(mockMessage, times(1)).getLogKey();
    }

    @Test
    public void shouldUseKafkaRecordMessage() throws Exception {
        when(jdbcSinkConfig.getKafkaRecordParserMode()).thenReturn("message");
        when(mockMessage.getLogMessage()).thenReturn("message".getBytes());
        QueryTemplate queryTemplate = new QueryTemplate(jdbcSinkConfig, protoToFieldMapper);
        queryTemplate.toQueryString(mockMessage);
        verify(mockMessage, times(1)).getLogMessage();
    }
}
