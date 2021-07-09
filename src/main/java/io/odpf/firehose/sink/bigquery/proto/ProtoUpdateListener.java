package io.odpf.firehose.sink.bigquery.proto;

import com.gojek.de.stencil.models.DescriptorAndTypeName;
import com.gojek.de.stencil.parser.Parser;
import com.gojek.de.stencil.utils.StencilUtils;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.Field;
import io.odpf.firehose.config.BigQuerySinkConfig;
import io.odpf.firehose.sink.bigquery.converter.MessageRecordConverter;
import io.odpf.firehose.sink.bigquery.converter.MessageRecordConverterWrapper;
import io.odpf.firehose.sink.bigquery.converter.RowMapper;
import io.odpf.firehose.sink.bigquery.handler.BigQueryClient;
import io.odpf.firehose.sink.bigquery.models.BQField;
import io.odpf.firehose.sink.bigquery.models.ProtoField;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

@Slf4j
public class ProtoUpdateListener extends com.gojek.de.stencil.cache.ProtoUpdateListener {
    private final BigQuerySinkConfig config;
    private final Converter protoMappingConverter = new Converter();
    private final ProtoFieldParser protoMappingParser = new ProtoFieldParser();
    private final BigQueryClient bqClient;
    private final MessageRecordConverterWrapper messageRecordConverterWrapper;
    @Setter
    private Parser stencilParser;

    public ProtoUpdateListener(BigQuerySinkConfig config, BigQueryClient bqClient, MessageRecordConverterWrapper messageRecordConverterWrapper) {
        super(config.getInputSchemaProtoClass());
        this.config = config;
        this.bqClient = bqClient;
        this.messageRecordConverterWrapper = messageRecordConverterWrapper;
    }

    public void update(Map<String, DescriptorAndTypeName> newDescriptors) {
        onProtoUpdate("", newDescriptors);
    }

    @Override
    public void onProtoUpdate(String url, Map<String, DescriptorAndTypeName> newDescriptors) {
        log.info("stencil cache was refreshed, validating if bigquery schema changed");
        try {
            ProtoField protoField = new ProtoField();
            protoField = protoMappingParser.parseFields(protoField, config.getInputSchemaProtoClass(), StencilUtils.getAllProtobufDescriptors(newDescriptors), StencilUtils.getTypeNameToPackageNameMap(newDescriptors));
            updateProtoParser(protoField);
        } catch (BigQueryException | IOException e) {
            String errMsg = "Error while updating bigquery table on callback:" + e.getMessage();
            log.error(errMsg);
            throw new RuntimeException(errMsg, e);
        }
    }

    // First get latest protomapping, update bq schema, and if all goes fine
    // then only update beast's proto mapping config
    private void updateProtoParser(final ProtoField protoField) throws IOException {
        String protoMappingString = protoMappingConverter.generateColumnMappings(protoField.getFields());
        List<Field> bqSchemaFields = protoMappingConverter.generateBigquerySchema(protoField);
        addMetadataFields(bqSchemaFields);
        bqClient.upsertTable(bqSchemaFields);
        config.setProperty("SINK_BIGQUERY_PROTO_COLUMN_MAPPING", protoMappingString);
        setProtoParser(config.getProtoColumnMapping());
    }

    private void addMetadataFields(List<Field> bqSchemaFields) {
        List<Field> bqMetadataFields = new ArrayList<>();
        String namespaceName = config.getBqMetadataNamespace();
        if (namespaceName.isEmpty()) {
            bqMetadataFields.addAll(BQField.getMetadataFields());
        } else {
            bqMetadataFields.add(BQField.getNamespacedMetadataField(namespaceName));
        }

        List<String> duplicateFields = getDuplicateFields(bqSchemaFields, bqMetadataFields).stream().map(Field::getName).collect(Collectors.toList());
        if (duplicateFields.size() > 0) {
            throw new RuntimeException(String.format("Metadata field(s) is already present in the schema. "
                                                     + "fields: %s", duplicateFields));
        }
        bqSchemaFields.addAll(bqMetadataFields);
    }

    private void setProtoParser(Properties columnMapping) {
        messageRecordConverterWrapper.setMessageRecordConverter(
                new MessageRecordConverter(new RowMapper(columnMapping, config.getFailOnUnknownFields()),
                        stencilParser, config));
    }

    public void close() throws IOException {
    }

    private List<Field> getDuplicateFields(List<Field> fields1, List<Field> fields2) {
        return fields1.stream().filter(field -> containsField(fields2, field.getName())).collect(Collectors.toList());
    }

    private boolean containsField(List<Field> fields, String fieldName) {
        return fields.stream().anyMatch(field -> field.getName().equals(fieldName));
    }
}
