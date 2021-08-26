package io.odpf.firehose.sink.objectstorage.proto;

import com.github.os72.protobuf.dynamic.DynamicSchema;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;

public class ProtoUtils {
    public static Descriptors.FileDescriptor createFileDescriptor(DynamicSchema schema) {
        DescriptorProtos.FileDescriptorSet fileDescriptorSet = schema.getFileDescriptorSet();
        DescriptorProtos.FileDescriptorProto fileDescriptorProto = fileDescriptorSet.getFile(0);
        Descriptors.FileDescriptor[] dependencies = {};

        try {
            return Descriptors.FileDescriptor.buildFrom(fileDescriptorProto, dependencies);
        } catch (Descriptors.DescriptorValidationException e) {
            throw new RuntimeException(e);
        }
    }
}
