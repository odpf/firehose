package io.odpf.firehose.sink.bigquery.converter.fields;

import com.google.api.client.util.DateTime;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import lombok.AllArgsConstructor;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

@AllArgsConstructor
public class TimestampField implements ProtoField {
    private final Descriptors.FieldDescriptor descriptor;
    private final Object fieldValue;

    @Override
    public Object getValue() {
        if (fieldValue instanceof Collection<?>) {
            List<DateTime> tsValues = new ArrayList<>();
            for (Object field : (Collection<?>) fieldValue) {
                tsValues.add(getTime(field));
            }
            return tsValues;
        }

        return getTime(fieldValue);
    }

    private DateTime getTime(Object field) {
        DynamicMessage dynamicField = (DynamicMessage) field;
        List<Descriptors.FieldDescriptor> descriptors = dynamicField.getDescriptorForType().getFields();
        List<Object> timeFields = new ArrayList<>();
        descriptors.forEach(desc -> timeFields.add(dynamicField.getField(desc)));
        Instant time = Instant.ofEpochSecond((long) timeFields.get(0), ((Integer) timeFields.get(1)).longValue());
        return new DateTime(time.toEpochMilli());
    }

    @Override
    public boolean matches() {
        return descriptor.getType() == Descriptors.FieldDescriptor.Type.MESSAGE
               && descriptor.getMessageType().getFullName().equals(com.google.protobuf.Timestamp.getDescriptor().getFullName());
    }
}
