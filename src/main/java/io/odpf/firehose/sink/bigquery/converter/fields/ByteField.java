package io.odpf.firehose.sink.bigquery.converter.fields;


import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import lombok.AllArgsConstructor;

import java.util.Base64;

@AllArgsConstructor
public class ByteField implements ProtoField {

    private final Descriptors.FieldDescriptor descriptor;
    private final Object fieldValue;

    @Override
    public Object getValue() {
        ByteString byteString = (ByteString) fieldValue;
        return new String(Base64.getEncoder().encode(byteString.toStringUtf8().getBytes()));
    }

    @Override
    public boolean matches() {
        return descriptor.getType() == Descriptors.FieldDescriptor.Type.BYTES;
    }
}
