package io.odpf.firehose.sink.bigquery.converter.fields;


import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import lombok.AllArgsConstructor;

import java.util.Base64;

@AllArgsConstructor
public class ByteProtoField implements ProtoField {

    private final Descriptors.FieldDescriptor descriptor;
    private final Object fieldValue;

    @Override
    public Object getValue() {
        ByteString byteString = (ByteString) fieldValue;
        byte[] bytes = byteString.toStringUtf8().getBytes();
        return base64Encode(bytes);
    }

    private String base64Encode(byte[] bytes) {
        return new String(Base64.getEncoder().encode(bytes));
    }

    @Override
    public boolean matches() {
        return descriptor.getType() == Descriptors.FieldDescriptor.Type.BYTES;
    }
}
