package com.gotocompany.firehose.sink.jdbc.field.message;




import com.gotocompany.firehose.consumer.TestBookingLogMessage;
import com.gotocompany.firehose.consumer.TestLocation;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.gotocompany.stencil.StencilClientFactory;
import com.gotocompany.stencil.client.StencilClient;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class DBDefaultMessageFieldTest {
    private StencilClient stencilClient;

    @Before
    public void setUp() throws Exception {
        stencilClient = StencilClientFactory.getClient();
    }

    @Test
    public void shouldParseTheMessageFieldAsString() throws Exception {

        TestBookingLogMessage booking = TestBookingLogMessage.newBuilder().setDriverDropoffLocation(TestLocation.newBuilder().setName("location_name").build()).build();

        Descriptors.FieldDescriptor locationFieldDescriptor = TestBookingLogMessage.getDescriptor().getFields().get(12);
        DynamicMessage bookingParsed = stencilClient.getParser("com.gotocompany.firehose.consumer.TestBookingLogMessage").parse(booking.toByteArray());
        Object columnValue = bookingParsed.getField(locationFieldDescriptor);

        JdbcDefaultMessageField dbDefaultMessageField = new JdbcDefaultMessageField(columnValue);
        Assert.assertEquals("{\"name\":\"\",\"address\":\"\",\"latitude\":0.0,\"longitude\":0.0,\"type\":\"\",\"note\":\"\",\"place_id\":\"\",\"accuracy_meter\":0.0,\"gate_id\":\"\"}", dbDefaultMessageField.getColumn());
    }

    @Test
    public void shouldBeAbleToParseMessageFields() throws Exception {

        TestBookingLogMessage booking = TestBookingLogMessage.newBuilder().setDriverDropoffLocation(TestLocation.newBuilder().setName("location_name").build()).build();

        Descriptors.FieldDescriptor locationFieldDescriptor = TestBookingLogMessage.getDescriptor().getFields().get(12);
        DynamicMessage bookingParsed = stencilClient.getParser("com.gotocompany.firehose.consumer.TestBookingLogMessage").parse(booking.toByteArray());
        Object columnValue = bookingParsed.getField(locationFieldDescriptor);

        JdbcDefaultMessageField dbDefaultMessageField = new JdbcDefaultMessageField(columnValue);

        Assert.assertTrue("Should be able to process default message Fields", dbDefaultMessageField.canProcess());
    }

    @Test
    public void shouldNotBeAbleToParseNormalFields() throws Exception {

        TestBookingLogMessage booking = TestBookingLogMessage.newBuilder().setDriverDropoffLocation(TestLocation.newBuilder().setName("location_name").build()).setCustomerId("customer_id").build();

        Descriptors.FieldDescriptor customerIdFieldDescriptor = TestBookingLogMessage.getDescriptor().getFields().get(5);
        DynamicMessage bookingParsed = stencilClient.getParser("com.gotocompany.firehose.consumer.TestBookingLogMessage").parse(booking.toByteArray());
        Object columnValue = bookingParsed.getField(customerIdFieldDescriptor);

        JdbcDefaultMessageField dbDefaultMessageField = new JdbcDefaultMessageField(columnValue);

        Assert.assertFalse("Should not be able to process repeated Fields", dbDefaultMessageField.canProcess());
    }


}
