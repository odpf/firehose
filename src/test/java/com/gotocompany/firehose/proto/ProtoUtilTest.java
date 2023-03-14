package com.gotocompany.firehose.proto;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.UnknownFieldSet;
import com.gotocompany.firehose.consumer.TestBookingLogMessage;
import com.gotocompany.firehose.consumer.TestLocation;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ProtoUtilTest {
    @Test
    public void shouldReturnTrueWhenUnknownFieldsExistOnRootLevelFields() {
        Descriptors.Descriptor bookingLogMessage = TestBookingLogMessage.getDescriptor();
        Descriptors.Descriptor location = TestLocation.getDescriptor();

        Descriptors.FieldDescriptor fieldDescriptor = bookingLogMessage.findFieldByName("driver_pickup_location");
        DynamicMessage dynamicMessage = DynamicMessage.newBuilder(bookingLogMessage)
                .setField(fieldDescriptor, DynamicMessage.newBuilder(location)
                        .build())
                .setUnknownFields(UnknownFieldSet.newBuilder()
                        .addField(1, UnknownFieldSet.Field.getDefaultInstance())
                        .addField(2, UnknownFieldSet.Field.getDefaultInstance())
                        .build())
                .build();

        boolean unknownFieldExist = ProtoUtils.hasUnknownField(dynamicMessage);
        assertTrue(unknownFieldExist);
    }

    @Test
    public void shouldReturnTrueWhenUnknownFieldsExistOnNestedChildFields() {
        Descriptors.Descriptor bookingLogMessage = TestBookingLogMessage.getDescriptor();
        Descriptors.Descriptor location = TestLocation.getDescriptor();
        Descriptors.FieldDescriptor fieldDescriptor = bookingLogMessage.findFieldByName("driver_pickup_location");

        DynamicMessage dynamicMessage = DynamicMessage.newBuilder(bookingLogMessage)
                .setField(fieldDescriptor, DynamicMessage.newBuilder(location)
                        .setUnknownFields(UnknownFieldSet.newBuilder()
                                .addField(1, UnknownFieldSet.Field.getDefaultInstance())
                                .addField(2, UnknownFieldSet.Field.getDefaultInstance())
                                .build())
                        .build())
                .build();

        boolean unknownFieldExist = ProtoUtils.hasUnknownField(dynamicMessage);
        assertTrue(unknownFieldExist);
    }

    @Test
    public void shouldReturnFalseWhenNoUnknownFieldsExist() {
        Descriptors.Descriptor bookingLogMessage = TestBookingLogMessage.getDescriptor();
        Descriptors.Descriptor location = TestLocation.getDescriptor();

        Descriptors.FieldDescriptor fieldDescriptor = bookingLogMessage.findFieldByName("driver_pickup_location");
        DynamicMessage dynamicMessage = DynamicMessage.newBuilder(bookingLogMessage)
                .setField(fieldDescriptor, DynamicMessage.newBuilder(location).build())
                .build();

        boolean unknownFieldExist = ProtoUtils.hasUnknownField(dynamicMessage);
        assertFalse(unknownFieldExist);
    }

    @Test
    public void shouldReturnFalseWhenRootIsNull() {
        boolean unknownFieldExist = ProtoUtils.hasUnknownField(null);
        assertFalse(unknownFieldExist);
    }
}

