package io.odpf.firehose.sink.bigquery.converter.fields;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import lombok.AllArgsConstructor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

@AllArgsConstructor
public class StructField implements ProtoField {
    private final Descriptors.FieldDescriptor descriptor;
    private final Object fieldValue;

    @Override
    public Object getValue() {
        try {
            if (fieldValue instanceof Collection<?>) {
                List<String> structStrValues = new ArrayList<>();
                for (Object field : (Collection<?>) fieldValue) {
                    structStrValues.add(getString(field));
                }
                return structStrValues;
            }
            return getString(fieldValue);
        } catch (InvalidProtocolBufferException e) {
            return "";
        }
    }

    private String getString(Object field) throws InvalidProtocolBufferException {
        return JsonFormat.printer()
                .omittingInsignificantWhitespace()
                .print((DynamicMessage) field);
    }

    @Override
    public boolean matches() {
        return descriptor.getType() == Descriptors.FieldDescriptor.Type.MESSAGE
               && descriptor.getMessageType().getFullName().equals(com.google.protobuf.Struct.getDescriptor().getFullName());
    }
}
