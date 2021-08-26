package io.odpf.firehose.sink.bigquery.proto;

import com.google.protobuf.Descriptors;
import io.odpf.firehose.sink.bigquery.models.ProtoField;

import java.util.Map;

public class ProtoFieldParser {
    /**
     * Bigquery supports a maximum of 15 level nested structures.
     * Thus for nested data type of more than 15 level or for recursive data type,
     * we limit the fields to only contain schema upto 15 levels deep
     */
    private static final int MAX_BIGQUERY_NESTED_SCHEMA_LEVEL = 15;
    private final DescriptorCache descriptorCache = new DescriptorCache();

    public ProtoField parseFields(ProtoField protoField, String protoSchema, Map<String, Descriptors.Descriptor> allDescriptors,
                                  Map<String, String> typeNameToPackageNameMap) {
        return parseFields(protoField, protoSchema, allDescriptors, typeNameToPackageNameMap, 1);
    }

    private ProtoField parseFields(ProtoField protoField, String protoSchema, Map<String, Descriptors.Descriptor> allDescriptors,
                                   Map<String, String> typeNameToPackageNameMap, int level) {

        Descriptors.Descriptor currentProto = descriptorCache.fetch(allDescriptors, typeNameToPackageNameMap, protoSchema);
        if (currentProto == null) {
            // statsClient.increment(String.format("proto.notfound.errors,proto=%s", protoSchema));
            throw new RuntimeException("No Proto found for class " + protoSchema);
        }
        for (Descriptors.FieldDescriptor field : currentProto.getFields()) {
            ProtoField fieldModel = new ProtoField(field.toProto());
            if (fieldModel.isNested()) {
                if (protoSchema.substring(1).equals(currentProto.getFullName())) {
                    if (level >= MAX_BIGQUERY_NESTED_SCHEMA_LEVEL) {
                        continue;
                    }
                }
                fieldModel = parseFields(fieldModel, field.toProto().getTypeName(), allDescriptors, typeNameToPackageNameMap, level + 1);
            }
            protoField.addField(fieldModel);
        }
        return protoField;
    }
}
