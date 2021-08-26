package io.odpf.firehose.sink.bigquery.converter.fields;

import com.google.protobuf.Descriptors;
import lombok.AllArgsConstructor;

import java.util.ArrayList;
import java.util.List;

@AllArgsConstructor
public class EnumField implements ProtoField {
    private final Descriptors.FieldDescriptor descriptor;
    private final Object fieldValue;

    @Override
    public Object getValue() {
        if (descriptor.isRepeated()) {
            List<Descriptors.EnumValueDescriptor> enumValues = ((List<Descriptors.EnumValueDescriptor>) (fieldValue));
            List<String> enumStrValues = new ArrayList<>();
            for (Descriptors.EnumValueDescriptor enumVal : enumValues) {
                enumStrValues.add(enumVal.toString());
            }
            return enumStrValues;
        }
        return fieldValue.toString();
    }

    @Override
    public boolean matches() {
        return descriptor.getType() == Descriptors.FieldDescriptor.Type.ENUM;
    }
}
