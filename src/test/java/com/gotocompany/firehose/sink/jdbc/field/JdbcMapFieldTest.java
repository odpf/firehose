package com.gotocompany.firehose.sink.jdbc.field;




import com.gotocompany.firehose.consumer.TestAuditEntityLogMessage;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.gotocompany.stencil.StencilClientFactory;
import com.gotocompany.stencil.client.StencilClient;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;

public class JdbcMapFieldTest {


    private StencilClient stencilClient;

    @Before
    public void setUp() throws Exception {
        stencilClient = StencilClientFactory.getClient();
    }

    @Test
    public void shouldParseTheMapFieldAsString() throws Exception {


        HashMap<String, String> currentStates = new HashMap<>();
        currentStates.put("key", "value");
        currentStates.put("key2", "value2");
        TestAuditEntityLogMessage auditEntityLogMessage = TestAuditEntityLogMessage.newBuilder().putAllCurrentState(currentStates).build();

        Descriptors.FieldDescriptor currentEntityFieldDescriptor = TestAuditEntityLogMessage.getDescriptor().getFields().get(6);
        DynamicMessage auditEntityParsed = stencilClient.getParser("com.gotocompany.firehose.consumer.TestAuditEntityLogMessage").parse(auditEntityLogMessage.toByteArray());
        Object columnValue = auditEntityParsed.getField(currentEntityFieldDescriptor);

        JdbcMapField jdbcMapField = new JdbcMapField(columnValue, currentEntityFieldDescriptor);

        Object data = jdbcMapField.getColumn();

        Assert.assertEquals("{\"key2\":\"value2\",\"key\":\"value\"}", data);
    }

    @Test
    public void shouldParseTheMapFieldWithEmptyValueAsString() throws Exception {
        HashMap<String, String> currentStates = new HashMap<>();
        currentStates.put("key", "value");
        currentStates.put("key2", "value2");
        currentStates.put("key3", "");
        TestAuditEntityLogMessage auditEntityLogMessage = TestAuditEntityLogMessage.newBuilder().putAllCurrentState(currentStates).build();

        Descriptors.FieldDescriptor currentEntityFieldDescriptor = TestAuditEntityLogMessage.getDescriptor().getFields().get(6);
        DynamicMessage auditEntityParsed = stencilClient.getParser("com.gotocompany.firehose.consumer.TestAuditEntityLogMessage").parse(auditEntityLogMessage.toByteArray());
        Object columnValue = auditEntityParsed.getField(currentEntityFieldDescriptor);

        JdbcMapField jdbcMapField = new JdbcMapField(columnValue, currentEntityFieldDescriptor);

        Object data = jdbcMapField.getColumn();

        Assert.assertEquals("{\"key2\":\"value2\",\"key3\":\"\",\"key\":\"value\"}", data);
    }

    @Test
    public void shouldBeAbleToParseMapFields() throws Exception {

        HashMap<String, String> currentStates = new HashMap<>();
        currentStates.put("key", "value");
        currentStates.put("key2", "value2");
        currentStates.put("key3", "value3");
        TestAuditEntityLogMessage auditEntityLogMessage = TestAuditEntityLogMessage.newBuilder().putAllCurrentState(currentStates).build();

        Descriptors.FieldDescriptor currentEntityFieldDescriptor = TestAuditEntityLogMessage.getDescriptor().getFields().get(6);
        DynamicMessage auditEntityParsed = stencilClient.getParser("com.gotocompany.firehose.consumer.TestAuditEntityLogMessage").parse(auditEntityLogMessage.toByteArray());
        Object columnValue = auditEntityParsed.getField(currentEntityFieldDescriptor);

        JdbcMapField jdbcMapField = new JdbcMapField(columnValue, currentEntityFieldDescriptor);

        Assert.assertTrue("Should be able to process map Fields", jdbcMapField.canProcess());
    }

    @Test
    public void shouldBeAbleToParseStringFields() throws Exception {

        HashMap<String, String> currentStates = new HashMap<>();
        currentStates.put("key", "value");
        currentStates.put("key2", "value2");
        TestAuditEntityLogMessage auditEntityLogMessage = TestAuditEntityLogMessage.newBuilder().setAuditId("audit_id").putAllCurrentState(currentStates).build();

        Descriptors.FieldDescriptor auditIdFieldDescriptor = TestAuditEntityLogMessage.getDescriptor().getFields().get(0);
        DynamicMessage auditEntityParsed = stencilClient.getParser("com.gotocompany.firehose.consumer.TestAuditEntityLogMessage").parse(auditEntityLogMessage.toByteArray());
        Object columnValue = auditEntityParsed.getField(auditIdFieldDescriptor);

        JdbcMapField jdbcMapField = new JdbcMapField(columnValue, auditIdFieldDescriptor);

        Assert.assertFalse("Should not be able to process repeated Fields", jdbcMapField.canProcess());
    }


}
