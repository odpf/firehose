package com.gotocompany.firehose.sink.jdbc.field.message;




import com.gotocompany.firehose.consumer.TestAuditEntityLogMessage;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Timestamp;
import com.gotocompany.stencil.StencilClientFactory;
import com.gotocompany.stencil.client.StencilClient;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.time.Instant;

public class DBTimestampFieldTest {

    private StencilClient stencilClient;

    @Before
    public void setUp() throws Exception {
        stencilClient = StencilClientFactory.getClient();
    }

    @Test
    public void shouldParseTheTimestampFieldAsString() throws Exception {

        Instant now = Instant.now();
        Timestamp timestamp = Timestamp.newBuilder().setSeconds(now.getEpochSecond()).setNanos(now.getNano()).build();
        TestAuditEntityLogMessage auditEntityLogMessage = TestAuditEntityLogMessage.newBuilder().setEventTimestamp(timestamp).build();

        Descriptors.FieldDescriptor timestampFieldDescriptor = TestAuditEntityLogMessage.getDescriptor().getFields().get(3);
        DynamicMessage auditEntityParsed = stencilClient.getParser("com.gotocompany.firehose.consumer.TestAuditEntityLogMessage").parse(auditEntityLogMessage.toByteArray());
        Object columnValue = auditEntityParsed.getField(timestampFieldDescriptor);

        JdbcTimestampField dbTime = new JdbcTimestampField(columnValue);

        Assert.assertEquals(now, dbTime.getColumn());
    }

    @Test
    public void shouldBeAbleToParseTimestampFields() throws Exception {

        Instant now = Instant.now();
        Timestamp timestamp = Timestamp.newBuilder().setSeconds(now.getEpochSecond()).setNanos(now.getNano()).build();
        TestAuditEntityLogMessage auditEntityLogMessage = TestAuditEntityLogMessage.newBuilder().setEventTimestamp(timestamp).build();

        Descriptors.FieldDescriptor timestampFieldDescriptor = TestAuditEntityLogMessage.getDescriptor().getFields().get(3);
        DynamicMessage auditEntityParsed = stencilClient.getParser("com.gotocompany.firehose.consumer.TestAuditEntityLogMessage").parse(auditEntityLogMessage.toByteArray());
        Object columnValue = auditEntityParsed.getField(timestampFieldDescriptor);

        JdbcTimestampField dbTime = new JdbcTimestampField(columnValue);

        Assert.assertTrue("Should be able to process timestamp Fields", dbTime.canProcess());
    }

    @Test
    public void shouldNotBeAbleToParseStringFields() throws Exception {

        Instant now = Instant.now();
        Timestamp timestamp = Timestamp.newBuilder().setSeconds(now.getEpochSecond()).setNanos(now.getNano()).build();
        TestAuditEntityLogMessage auditEntityLogMessage = TestAuditEntityLogMessage.newBuilder().setEventTimestamp(timestamp).setAuditId("audit_id").build();

        Descriptors.FieldDescriptor auditIdFieldDescriptor = TestAuditEntityLogMessage.getDescriptor().getFields().get(0);
        DynamicMessage auditEntityParsed = stencilClient.getParser("com.gotocompany.firehose.consumer.TestAuditEntityLogMessage").parse(auditEntityLogMessage.toByteArray());
        Object columnValue = auditEntityParsed.getField(auditIdFieldDescriptor);

        JdbcTimestampField dbTime = new JdbcTimestampField(columnValue);

        Assert.assertFalse("Should not be able to process string Fields", dbTime.canProcess());
    }

}
