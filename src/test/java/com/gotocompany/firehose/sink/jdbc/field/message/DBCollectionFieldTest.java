package com.gotocompany.firehose.sink.jdbc.field.message;


import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.gotocompany.firehose.consumer.TestFeedbackLogMessage;
import com.gotocompany.firehose.consumer.TestReason;
import com.gotocompany.stencil.StencilClientFactory;
import com.gotocompany.stencil.client.StencilClient;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;

public class DBCollectionFieldTest {

    private StencilClient stencilClient;

    @Before
    public void setUp() {
        stencilClient = StencilClientFactory.getClient();
    }


    @Test
    public void shouldParseTheCollectionFieldAsString() throws Exception {

        TestReason reason = TestReason.newBuilder().setReasonId("1").setGroupId("1").build();
        TestReason reason2 = TestReason.newBuilder().setReasonId("2").setGroupId("2").build();
        ArrayList<TestReason> reasons = new ArrayList<>();
        reasons.add(reason);
        reasons.add(reason2);
        TestFeedbackLogMessage feedback = TestFeedbackLogMessage
                .newBuilder()
                .addAllReason(reasons)
                .build();

        Descriptors.FieldDescriptor reasonFieldDescriptor = TestFeedbackLogMessage.getDescriptor().getFields().get(10);

        DynamicMessage feedbackParsed = stencilClient.getParser("com.gotocompany.firehose.consumer.TestFeedbackLogMessage").parse(feedback.toByteArray());
        Object columnValue = feedbackParsed.getField(reasonFieldDescriptor);

        JdbcCollectionField dbCollectionField = new JdbcCollectionField(columnValue, reasonFieldDescriptor);
        Object data = dbCollectionField.getColumn();

        Assert.assertEquals("[{\"reason_id\":\"1\",\"group_id\":\"1\"},{\"reason_id\":\"2\",\"group_id\":\"2\"}]", data);
    }

    @Test
    public void shouldBeAbleToParseCollectionFields() throws Exception {

        TestReason reason = TestReason.newBuilder().setReasonId("1").setGroupId("1").build();
        TestReason reason2 = TestReason.newBuilder().setReasonId("2").setGroupId("2").build();
        ArrayList<TestReason> reasons = new ArrayList<>();
        reasons.add(reason);
        reasons.add(reason2);
        TestFeedbackLogMessage feedback = TestFeedbackLogMessage
                .newBuilder()
                .addAllReason(reasons)
                .build();

        Descriptors.FieldDescriptor reasonFieldDescriptor = TestFeedbackLogMessage.getDescriptor().getFields().get(10);

        DynamicMessage feedbackParsed = stencilClient.getParser("com.gotocompany.firehose.consumer.TestFeedbackLogMessage").parse(feedback.toByteArray());
        Object columnValue = feedbackParsed.getField(reasonFieldDescriptor);

        JdbcCollectionField dbCollectionField = new JdbcCollectionField(columnValue, reasonFieldDescriptor);

        Assert.assertTrue("Should be able to process collection Fields", dbCollectionField.canProcess());
    }

    @Test
    public void shouldNotBeAbleToParseStringFields() throws Exception {

        TestReason reason = TestReason.newBuilder().setReasonId("1").setGroupId("1").build();
        TestReason reason2 = TestReason.newBuilder().setReasonId("2").setGroupId("2").build();
        ArrayList<TestReason> reasons = new ArrayList<>();
        reasons.add(reason);
        reasons.add(reason2);
        TestFeedbackLogMessage feedback = TestFeedbackLogMessage
                .newBuilder()
                .setOrderNumber("order_number")
                .addAllReason(reasons)
                .build();

        Descriptors.FieldDescriptor orderNumberFieldDescriptor = TestFeedbackLogMessage.getDescriptor().getFields().get(0);

        DynamicMessage feedbackParsed = stencilClient.getParser("com.gotocompany.firehose.consumer.TestFeedbackLogMessage").parse(feedback.toByteArray());
        Object columnValue = feedbackParsed.getField(orderNumberFieldDescriptor);

        JdbcCollectionField dbCollectionField = new JdbcCollectionField(columnValue, orderNumberFieldDescriptor);

        Assert.assertFalse("Should not be able to process repeated Fields", dbCollectionField.canProcess());
    }


}
