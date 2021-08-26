package io.odpf.firehose.sink.bigquery.proto;

import com.google.protobuf.Descriptors;

import java.util.Map;

public class DescriptorCache {
    public Descriptors.Descriptor fetch(Map<String, Descriptors.Descriptor> allDescriptors, Map<String, String> typeNameToPackageNameMap, String protoName) {
        if (allDescriptors.get(protoName) != null) {
            return allDescriptors.get(protoName);
        }
        String packageName = typeNameToPackageNameMap.get(protoName);
        if (packageName == null) {
            return null;
        }
        return allDescriptors.get(packageName);
    }
}
